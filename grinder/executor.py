"""Task worker executor implementation."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import multiprocessing
import os
import random
import socket
import threading
import typing
from multiprocessing.queues import JoinableQueue
from queue import Empty
from traceback import format_exception

from django.tasks import TaskResult
from django.tasks.base import TaskContext, TaskError, TaskResultStatus
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json

if typing.TYPE_CHECKING:
    from .backends import AcknowledgeableTaskBackend


@dataclasses.dataclass(kw_only=True, slots=True)
class TaskExecutor:
    """Consume tasks from a priority queue with process and thread pools."""

    backend: AcknowledgeableTaskBackend
    workers: int | None = None
    threads: int = 1
    max_tasks: int = 0
    max_tasks_jitter: int = 0
    task_timeout: datetime.timedelta = datetime.timedelta(hours=1)
    acquire_timeout: datetime.timedelta = datetime.timedelta(seconds=1)
    is_acquiring: bool = dataclasses.field(default=True, init=False)
    is_publishing: bool = dataclasses.field(default=True, init=False)
    _worker_processes: list[WorkerProcess] = dataclasses.field(
        default_factory=list, init=False
    )
    process_count: int = dataclasses.field(init=False)
    thread_count: int = dataclasses.field(init=False)
    shared_task_queue: multiprocessing.JoinableQueue[TaskResult] = dataclasses.field(
        init=False
    )
    processed_task_queue: multiprocessing.JoinableQueue[TaskResult] = dataclasses.field(
        init=False
    )

    def __post_init__(self) -> None:
        """Initialize derived orchestration fields and queues."""
        self.process_count = self.workers or max(multiprocessing.cpu_count() - 1, 1)
        self.thread_count = max(self.threads, 1)
        self.shared_task_queue = multiprocessing.JoinableQueue(
            maxsize=self.process_count * self.thread_count,
        )
        self.processed_task_queue = multiprocessing.JoinableQueue()

    def _get_maximum_tasks_per_child(self) -> int | None:
        """Return worker recycling limit based on config and thread count."""
        if self.max_tasks:
            return (
                self.max_tasks + random.randint(0, self.max_tasks_jitter)  # noqa: S311
            ) // self.thread_count
        return None

    def _create_worker_process(self) -> WorkerProcess:
        """Create and start a new worker process."""
        worker = WorkerProcess(
            self.shared_task_queue,
            self.processed_task_queue,
            self.thread_count,
            self.task_timeout,
            self._get_maximum_tasks_per_child(),
        )
        worker.start()
        return worker

    def run(self) -> None:
        """Start consuming tasks until shutdown is requested."""
        self._worker_processes = [
            self._create_worker_process() for _ in range(self.process_count)
        ]
        asyncio.gather(
            asyncio.create_task(self.acquire_tasks()),
            asyncio.create_task(self.acknowledge_tasks()),
            asyncio.create_task(self.maintain_worker_pool()),
        )

    async def acquire_tasks(self) -> None:
        """Buffer tasks in shared task queue."""
        while self.is_acquiring:
            self.shared_task_queue.put(self.backend.acquire())

    async def acknowledge_tasks(self) -> None:
        """Acknowledge processed tasks and publish updated results in main process."""
        while self.is_publishing:
            self.backend.acknowledge(self.processed_task_queue.get(block=True))
            self.processed_task_queue.task_done()

    def shutdown(self) -> None:
        """Stop queue consumption and terminate all worker processes."""
        self.is_acquiring = False
        self.shared_task_queue.join()
        for worker in self._worker_processes:
            worker.shutdown()
        self.processed_task_queue.join()
        self.is_publishing = False

    async def maintain_worker_pool(self) -> None:
        """Restart worker processes that have exited."""
        while self.is_publishing:
            for index, worker in enumerate(self._worker_processes):
                if worker.is_alive():
                    continue
                worker.join(timeout=0)
                self._worker_processes[index] = self._create_worker_process()


class WorkerProcess(multiprocessing.Process):
    """Single worker process running thread_count consumer threads."""

    def __init__(
        self,
        task_queue: JoinableQueue[TaskResult],
        processed_task_queue: JoinableQueue[TaskResult],
        thread_count: int,
        task_timeout: datetime.timedelta,
        max_tasks: int | None = None,
    ) -> None:
        """Create process with dedicated thread pool for task execution."""
        self.shutdown_requested = multiprocessing.Event()
        super().__init__(
            target=_run_worker_process,
            args=(
                task_queue,
                processed_task_queue,
                thread_count,
                task_timeout,
                max_tasks,
                self.shutdown_requested,
            ),
            daemon=True,
        )
        self.thread_count = thread_count

    def shutdown(self) -> None:
        """Request graceful worker stop and wait for process exit."""
        self.shutdown_requested.set()
        self.join()


class _WorkerState:
    """State shared by process-local consumer threads."""

    def __init__(
        self,
        max_tasks: int | None,
        task_timeout: datetime.timedelta,
    ) -> None:
        """Create process-local execution state."""
        self.max_tasks = max_tasks
        self.task_timeout = task_timeout
        self.task_count = 0
        self.lock = threading.Lock()
        self.expired = threading.Event()

    def record_task(self) -> None:
        """Record one processed task and stop when max_tasks is reached."""
        if self.max_tasks is None:
            return
        with self.lock:
            self.task_count += 1
            if self.task_count >= self.max_tasks:
                self.expired.set()


def _run_worker_process(
    task_queue: multiprocessing.JoinableQueue[TaskResult],
    processed_task_queue: multiprocessing.JoinableQueue[TaskResult],
    thread_count: int,
    task_timeout: datetime.timedelta,
    max_tasks: int | None,
) -> None:
    """Run consumer threads in a process that read from shared task queue."""
    state = _WorkerState(
        max_tasks=max_tasks,
        task_timeout=task_timeout,
    )
    if thread_count == 1:
        _consume_tasks(task_queue, processed_task_queue, state)
        return
    consumer_threads = [
        threading.Thread(
            target=_consume_tasks,
            args=(task_queue, processed_task_queue, state),
            name=f"task-consumer-thread-{index}",
        )
        for index in range(thread_count)
    ]
    for consumer_thread in consumer_threads:
        consumer_thread.start()
    for consumer_thread in consumer_threads:
        # Wait for consumer thread to finish or timeout.
        consumer_thread.join(task_timeout.total_seconds())


def _consume_tasks(
    task_queue: multiprocessing.JoinableQueue[TaskResult],
    processed_task_queue: multiprocessing.JoinableQueue[TaskResult],
    state: _WorkerState,
) -> None:
    """Consume and execute tasks from shared task queue."""
    while not state.expired.is_set():
        try:
            task_result = task_queue.get(timeout=1.0)
        except Empty:
            continue
        try:
            processed_task_queue.put(
                _execute_task_result(
                    task_result,
                )
            )
        finally:
            task_queue.task_done()
            state.record_task()


def _execute_task_result(
    task_result: TaskResult,
) -> TaskResult:
    """Execute task from task result and update result lifecycle state."""
    started_at = timezone.now()
    task_result = dataclasses.replace(
        task_result,
        status=TaskResultStatus.RUNNING,
        started_at=started_at,
        last_attempted_at=started_at,
        worker_ids=[*task_result.worker_ids, _create_worker_id()],
    )
    task_enqueued.send(TaskExecutor, task_result=task_result)
    task_started.send(TaskExecutor, task_result=task_result)

    try:
        return_value = _call_task(task_result)
    except Exception as exception:
        task_result = dataclasses.replace(
            task_result,
            status=TaskResultStatus.FAILED,
            errors=[*task_result.errors, _create_task_error(exception)],
        )
    else:
        task_result = dataclasses.replace(
            task_result,
            status=TaskResultStatus.SUCCESSFUL,
            _return_value=normalize_json(return_value),
        )
    finally:
        task_result = dataclasses.replace(
            task_result,
            finished_at=timezone.now(),
        )
        task_finished.send(TaskExecutor, task_result=task_result)

    return task_result


def _create_worker_id() -> str:
    """Create worker id in host-process-thread format."""
    return f"{socket.gethostname()}:{os.getpid()}:{threading.get_ident()}"


def _call_task(task_result: TaskResult) -> typing.Any:
    """Call a task with context when required."""
    task = task_result.task
    if task.takes_context:
        return task.call(
            TaskContext(task_result=task_result),
            *task_result.args,
            **task_result.kwargs,
        )
    else:
        return task.call(*task_result.args, **task_result.kwargs)


def _create_task_error(exception: BaseException) -> TaskError:
    """Build a task error payload for failed execution."""
    exception_type = type(exception)
    return TaskError(
        exception_class_path=f"{exception_type.__module__}.{exception_type.__qualname__}",
        traceback="".join(format_exception(exception)),
    )
