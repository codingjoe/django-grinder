"""Task worker executor implementation."""

from __future__ import annotations

import dataclasses
import multiprocessing
import random
import threading
import typing
from multiprocessing import JoinableQueue, Process, Queue
from queue import Empty, ShutDown
from traceback import format_exception

from django.tasks import TaskResult
from django.tasks.base import TaskContext, TaskError, TaskResultStatus
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone

if typing.TYPE_CHECKING:
    from .backends import AcknowledgeableTaskBackend


class WorkerProcess(Process):
    """Single worker process running thread_count consumer threads."""

    def __init__(
        self,
        task_queue: JoinableQueue[TaskResult],
        processed_task_queue: JoinableQueue[TaskResult],
        thread_count: int,
        task_timeout: float,
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


class TaskExecutor:
    """Consume tasks from a priority queue with process and thread pools."""

    def __init__(
        self,
        *,
        backend: AcknowledgeableTaskBackend,
        workers: int | None = None,
        threads: int = 1,
        max_tasks: int = 0,
        max_tasks_jitter: int = 0,
        get_timeout_secs: float = 1.0,
        task_timeout: float = 3600.0,
    ) -> None:
        """Create pool-backed executor with queue consumption settings."""
        self.backend = backend
        self.process_count = workers or max(multiprocessing.cpu_count() - 1, 1)
        self.thread_count = threads
        self.max_tasks = max_tasks
        self.max_tasks_jitter = max_tasks_jitter
        self.get_timeout_secs = get_timeout_secs
        self.task_timeout = task_timeout
        self.is_running = True
        self._worker_processes: list[WorkerProcess] = []
        self.processing_slot_count = self.process_count * self.thread_count
        self.shared_task_queue: JoinableQueue[TaskResult] = JoinableQueue(
            maxsize=self.processing_slot_count,
        )
        self.processed_task_queue: JoinableQueue[TaskResult] = JoinableQueue(
            maxsize=self.processing_slot_count,
        )

    def _get_maximum_tasks_per_child(self) -> int | None:
        """Return worker recycling limit based on config and thread count."""
        if self.max_tasks:
            return (
                self.max_tasks + random.randint(0, self.max_tasks_jitter)
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
        while self.is_running:
            self._replace_dead_worker_processes()
            self._drain_processed_tasks()
            try:
                task_result = self.backend.acquire(timeout=self.get_timeout_secs)
            except (Empty, ShutDown):
                continue
            # Block when all worker threads are saturated.
            self.shared_task_queue.put(task_result)

    def _drain_processed_tasks(self) -> None:
        """Acknowledge processed tasks and publish updated results in main process."""
        while True:
            try:
                task_result = self.processed_task_queue.get_nowait()
            except Empty:
                return
            try:
                self.backend.acknowledge(task_result)
            finally:
                self.processed_task_queue.task_done()

    def shutdown(self) -> None:
        """Stop queue consumption and terminate all worker processes."""
        self.is_running = False
        self.shared_task_queue.join()
        self._drain_processed_tasks()
        for worker in self._worker_processes:
            worker.shutdown()
        self._drain_processed_tasks()

    def _replace_dead_worker_processes(self) -> None:
        """Restart worker processes that have exited."""
        for index, worker in enumerate(self._worker_processes):
            if worker.is_alive():
                continue
            worker.join(timeout=0)
            self._worker_processes[index] = self._create_worker_process()


def _run_worker_process(
    task_queue: JoinableQueue[TaskResult],
    processed_task_queue: JoinableQueue[TaskResult],
    thread_count: int,
    task_timeout: float,
    max_tasks: int | None,
    shutdown_requested: typing.Any,
) -> None:
    """Run consumer threads in a process that read from shared task queue."""
    state = _WorkerState(
        max_tasks=max_tasks,
        task_timeout=task_timeout,
        shutdown_requested=shutdown_requested,
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
        consumer_thread.join()


class _WorkerState:
    """State shared by process-local consumer threads."""

    def __init__(
        self,
        max_tasks: int | None,
        task_timeout: float,
        shutdown_requested: typing.Any,
    ) -> None:
        """Create process-local execution state."""
        self.max_tasks = max_tasks
        self.task_timeout = task_timeout
        self.shutdown_requested = shutdown_requested
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
    processed_task_queue: JoinableQueue[TaskResult],
    state: _WorkerState,
) -> None:
    """Consume and execute tasks from shared task queue."""
    while not state.should_stop.is_set():
        if state.shutdown_requested.is_set() and task_queue.empty():
            return
        try:
            task_result = task_queue.get(timeout=1.0)
        except Empty:
            if state.shutdown_requested.is_set():
                return
            continue
        try:
            processed_task_result = _execute_task_result(
                task_result,
                task_timeout=state.task_timeout,
            )
            processed_task_queue.put(processed_task_result)
        finally:
            task_queue.task_done()
            state.record_task()


def _execute_task_result(task_result: TaskResult, task_timeout: float) -> TaskResult:
    """Execute task from task result and update result lifecycle state."""
    task_result = dataclasses.replace(task_result, enqueued_at=timezone.now())
    task_enqueued.send(TaskExecutor, task_result=task_result)

    started_at = timezone.now()
    task_result = dataclasses.replace(
        task_result,
        status=TaskResultStatus.RUNNING,
        started_at=started_at,
        last_attempted_at=started_at,
    )
    task_started.send(TaskExecutor, task_result=task_result)

    if task_error := _call_task_with_timeout(task_result, task_timeout):
        task_result = dataclasses.replace(
            task_result,
            finished_at=timezone.now(),
            status=TaskResultStatus.FAILED,
            errors=[*task_result.errors, task_error],
        )
    else:
        task_result = dataclasses.replace(
            task_result,
            finished_at=timezone.now(),
            status=TaskResultStatus.SUCCESSFUL,
        )

    task_finished.send(TaskExecutor, task_result=task_result)
    return task_result


def _call_task_with_timeout(
    task_result: TaskResult, task_timeout: float
) -> TaskError | None:
    """Execute task in a killable subprocess and return TaskError on failure."""
    result_queue: Queue[TaskError | None] = multiprocessing.Queue(maxsize=1)
    task_process = Process(
        target=_run_task_call_in_subprocess,
        args=(task_result, result_queue),
        daemon=True,
    )
    task_process.start()
    task_process.join(timeout=task_timeout)

    if task_process.is_alive():
        task_process.kill()
        task_process.join()
        return _create_task_timeout_error(task_timeout)

    if task_process.exitcode not in (0, None):
        exit_code = task_process.exitcode
        if exit_code is not None:
            return _create_task_crash_error(exit_code)

    try:
        return result_queue.get_nowait()
    except Empty:
        return None


def _run_task_call_in_subprocess(
    task_result: TaskResult,
    result_queue: Queue[TaskError | None],
) -> None:
    """Run task call and report failures through queue."""
    try:
        _call_task(task_result)
    except BaseException as exception:  # noqa: BLE001
        result_queue.put(_create_task_error(exception))
        return
    result_queue.put(None)


def _call_task(task_result: TaskResult) -> typing.Any:
    """Call a task with context when required."""
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
    """Build a task error payload for failed execution."""
    exception_type = type(exception)
    return TaskError(
        exception_class_path=f"{exception_type.__module__}.{exception_type.__qualname__}",
        traceback="".join(format_exception(exception)),
    )


def _create_task_timeout_error(task_timeout: float) -> TaskError:
    """Build task error payload for task timeout."""
    return TaskError(
        exception_class_path="builtins.TimeoutError",
        traceback=f"Task exceeded timeout of {task_timeout} seconds.",
    )


def _create_task_crash_error(exit_code: int) -> TaskError:
    """Build task error payload for subprocess crash."""
    return TaskError(
        exception_class_path="builtins.RuntimeError",
        traceback=f"Task subprocess crashed with exit code {exit_code}.",
    )
