"""Task worker executor implementation."""

from __future__ import annotations

import multiprocessing
import random
import threading
from multiprocessing import JoinableQueue, Process
from queue import Empty, Queue, ShutDown
from traceback import format_exception
from typing import Any

from django.tasks import TaskResult
from django.tasks.base import TaskContext, TaskError, TaskResultStatus, normalize_json
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone


class WorkerProcess(Process):
    """Single worker process running thread_count consumer threads."""

    def __init__(
        self,
        task_queue: JoinableQueue[TaskResult],
        thread_count: int,
        max_tasks: int | None = None,
    ) -> None:
        """Create process with dedicated thread pool for task execution."""
        super().__init__(
            target=_run_worker_process,
            args=(task_queue, thread_count, max_tasks),
            daemon=True,
        )
        self.thread_count = thread_count

    def shutdown(self) -> None:
        """Terminate worker process and release resources."""
        self.terminate()
        self.join()


class TaskExecutor:
    """Consume tasks from a priority queue with process and thread pools."""

    def __init__(
        self,
        *,
        backend: list[str] | str | None = None,
        queue: Queue[TaskResult],
        workers: int | None = None,
        threads: int = 1,
        max_tasks: int = 0,
        max_tasks_jitter: int = 0,
        get_timeout_secs: float = 1.0,
    ) -> None:
        """Create pool-backed executor with queue consumption settings."""
        del backend
        self.task_queue = queue
        self.process_count = workers or max(multiprocessing.cpu_count() - 1, 1)
        self.thread_count = threads
        self.max_tasks = max_tasks
        self.max_tasks_jitter = max_tasks_jitter
        self.get_timeout_secs = get_timeout_secs
        self.is_running = True
        self._worker_processes: list[WorkerProcess] = []
        self.processing_slot_count = self.process_count * self.thread_count
        self.shared_task_queue: JoinableQueue[TaskResult] = JoinableQueue(
            maxsize=self.processing_slot_count,
        )

    def _get_maximum_tasks_per_child(self) -> int | None:
        """Return worker recycling limit based on config and thread count."""
        if self.max_tasks:
            return (self.max_tasks + random.randint(0, self.max_tasks_jitter)) // self.thread_count
        return None

    def _create_worker_process(self) -> WorkerProcess:
        """Create and start a new worker process."""
        worker = WorkerProcess(
            self.shared_task_queue,
            self.thread_count,
            self._get_maximum_tasks_per_child(),
        )
        worker.start()
        return worker

    def run(self) -> None:
        """Start consuming tasks until shutdown is requested."""
        self._worker_processes = [
            self._create_worker_process() for _ in range(self.process_count)
        ]
        while self.is_running:
            self._replace_dead_worker_processes()
            try:
                task_result = self.task_queue.get(timeout=self.get_timeout_secs)
            except (Empty, ShutDown):
                continue
            try:
                # Block when all worker threads are saturated.
                self.shared_task_queue.put(task_result)
            finally:
                self.task_queue.task_done()

    def shutdown(self) -> None:
        """Stop queue consumption and terminate all worker processes."""
        self.is_running = False
        for worker in self._worker_processes:
            worker.shutdown()

    def _replace_dead_worker_processes(self) -> None:
        """Restart worker processes that have exited."""
        for index, worker in enumerate(self._worker_processes):
            if worker.is_alive():
                continue
            self._worker_processes[index] = self._create_worker_process()


def _run_worker_process(
    task_queue: JoinableQueue[TaskResult],
    thread_count: int,
    max_tasks: int | None,
) -> None:
    """Run consumer threads in a process that read from shared task queue."""
    state = _WorkerState(max_tasks=max_tasks)
    if thread_count == 1:
        _consume_tasks(task_queue, state)
        return
    consumer_threads = [
        threading.Thread(
            target=_consume_tasks,
            args=(task_queue, state),
            name=f"task-consumer-thread-{index}",
        )
        for index in range(thread_count)
    ]
    for consumer_thread in consumer_threads:
        consumer_thread.start()
    for consumer_thread in consumer_threads:
        consumer_thread.join()


class _WorkerState:
    """State shared by process-local consumer threads."""

    def __init__(self, max_tasks: int | None) -> None:
        """Create process-local execution state."""
        self.max_tasks = max_tasks
        self.task_count = 0
        self.lock = threading.Lock()
        self.should_stop = threading.Event()

    def record_task(self) -> None:
        """Record one processed task and stop when max_tasks is reached."""
        if self.max_tasks is None:
            return
        with self.lock:
            self.task_count += 1
            if self.task_count >= self.max_tasks:
                self.should_stop.set()


def _consume_tasks(
    task_queue: JoinableQueue[TaskResult],
    state: _WorkerState,
) -> None:
    """Consume and execute tasks from shared task queue."""
    while not state.should_stop.is_set():
        try:
            task_result = task_queue.get(timeout=1.0)
        except Empty:
            continue
        try:
            _execute_task_result(task_result)
        finally:
            task_queue.task_done()
            state.record_task()


def _execute_task_result(task_result: TaskResult) -> TaskResult:
    """Execute task from task result and update result lifecycle state."""
    enqueued_at = timezone.now()
    object.__setattr__(task_result, "enqueued_at", enqueued_at)
    task_enqueued.send(TaskExecutor, task_result=task_result)

    started_at = timezone.now()
    object.__setattr__(task_result, "status", TaskResultStatus.RUNNING)
    object.__setattr__(task_result, "started_at", started_at)
    object.__setattr__(task_result, "last_attempted_at", started_at)
    task_started.send(TaskExecutor, task_result=task_result)

    try:
        raw_return_value = _call_task(task_result)
    except KeyboardInterrupt:
        raise
    except BaseException as exception:  # noqa: BLE001
        object.__setattr__(task_result, "finished_at", timezone.now())
        task_result.errors.append(_create_task_error(exception))
        object.__setattr__(task_result, "status", TaskResultStatus.FAILED)
    else:
        object.__setattr__(task_result, "finished_at", timezone.now())
        object.__setattr__(task_result, "_return_value", normalize_json(raw_return_value))
        object.__setattr__(task_result, "status", TaskResultStatus.SUCCESSFUL)

    task_finished.send(TaskExecutor, task_result=task_result)
    return task_result



def _call_task(task_result: TaskResult) -> Any:
    """Call task with context when required."""
    task = task_result.task
    match task.takes_context:
        case True:
            return task.call(
                TaskContext(task_result=task_result),
                *task_result.args,
                **task_result.kwargs,
            )
        case False:
            return task.call(*task_result.args, **task_result.kwargs)


def _create_task_error(exception: BaseException) -> TaskError:
    """Build task error payload for failed execution."""
    exception_type = type(exception)
    return TaskError(
        exception_class_path=f"{exception_type.__module__}.{exception_type.__qualname__}",
        traceback="".join(format_exception(exception)),
    )
