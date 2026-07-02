"""Tests for threadmill.retry — RetryableTask, backoff factories, and task()."""

from __future__ import annotations

import dataclasses
import datetime
import uuid
from unittest.mock import patch

import pytest
from django.tasks import TaskResult, TaskResultStatus
from django.tasks.base import TaskContext
from django.utils import timezone

from tests.test_executor import _make_worker
from threadmill.executor import WorkerThread
from threadmill.retry import (
    RetryableTask,
    constant_backoff,
    exponential_backoff,
    linear_backoff,
    task,
)


# Override the Redis-flushing autouse fixture: none of these tests use real Redis.
@pytest.fixture(autouse=True)
def flush_default_backend():  # noqa: PT004 — intentional no-op override
    yield


# Module-level task definitions (Django requires tasks to be defined at module level).


@task(queue_name="default")
def _plain_task():
    pass


_constant_retry_fn = constant_backoff(delay=datetime.timedelta(seconds=5))


@task(queue_name="default", retry_fn=_constant_retry_fn)
def _retryable_task():
    pass


@task(queue_name="default", retry_fn=lambda ctx: None)
def _no_retry_task():
    pass


@task(queue_name="default", retry_fn=lambda ctx: datetime.timedelta(seconds=30))
def _always_retry_task():
    pass


@task
def _bare_task():
    pass


def _failing_task_result(retryable_task, attempts: int = 1) -> TaskResult:
    """Build a FAILED TaskResult with the given attempt count."""
    return TaskResult(
        task=retryable_task,
        id=str(uuid.uuid4()),
        status=TaskResultStatus.FAILED,
        enqueued_at=timezone.now(),
        started_at=timezone.now(),
        finished_at=timezone.now(),
        last_attempted_at=timezone.now(),
        args=[],
        kwargs={},
        backend="default",
        errors=[],
        worker_ids=["worker"] * attempts,
    )


def _context(attempts: int = 1) -> TaskContext:
    """Build a TaskContext with a fake FAILED result for the given attempt count."""
    return TaskContext(task_result=_failing_task_result(_plain_task, attempts))


class TestRetryableTask:
    """Tests for the RetryableTask dataclass."""

    def test_retryable_task__is_task_subclass(self):
        """RetryableTask is a subclass of django.tasks.Task."""
        from django.tasks import Task

        assert issubclass(RetryableTask, Task)

    def test_retryable_task__retry_fn_defaults_to_none(self):
        """RetryableTask.retry_fn defaults to None when not provided."""
        assert isinstance(_plain_task, RetryableTask)
        assert _plain_task.retry_fn is None

    def test_retryable_task__stores_retry_fn(self):
        """RetryableTask.retry_fn holds the configured retry function."""
        assert _retryable_task.retry_fn is _constant_retry_fn

    def test_retryable_task__is_frozen(self):
        """RetryableTask instances are immutable (frozen dataclass)."""
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            _retryable_task.retry_fn = None  # type: ignore[misc]


class TestTaskDecorator:
    """Tests for the threadmill.retry.task() decorator."""

    def test_task__returns_retryable_task(self):
        """task() returns a RetryableTask instance."""
        assert isinstance(_plain_task, RetryableTask)

    def test_task__without_retry_fn(self):
        """task() with no retry_fn creates RetryableTask with retry_fn=None."""
        assert _plain_task.retry_fn is None

    def test_task__with_retry_fn(self):
        """task() with retry_fn stores it on the RetryableTask."""
        assert _retryable_task.retry_fn is _constant_retry_fn

    def test_task__bare_decorator(self):
        """@task without parentheses returns a RetryableTask."""
        assert isinstance(_bare_task, RetryableTask)


class TestExponentialBackoff:
    """Tests for exponential_backoff()."""

    def test_exponential_backoff__first_attempt(self):
        """First retry uses the base delay (delay * base^0 = delay)."""
        fn = exponential_backoff(delay=datetime.timedelta(seconds=2))
        assert fn(_context(attempts=1)) == datetime.timedelta(seconds=2)

    def test_exponential_backoff__second_attempt(self):
        """Second retry doubles the delay (delay * base^1)."""
        fn = exponential_backoff(base=2, delay=datetime.timedelta(seconds=1))
        assert fn(_context(attempts=2)) == datetime.timedelta(seconds=2)

    def test_exponential_backoff__respects_max_retries(self):
        """Returns None once max_retries is exceeded."""
        fn = exponential_backoff(max_retries=2)
        assert fn(_context(attempts=3)) is None

    def test_exponential_backoff__within_max_retries(self):
        """Returns a timedelta while within max_retries."""
        fn = exponential_backoff(max_retries=2, delay=datetime.timedelta(seconds=1))
        assert fn(_context(attempts=2)) is not None

    def test_exponential_backoff__no_max_retries_retries_forever(self):
        """Returns a timedelta indefinitely when max_retries is None."""
        fn = exponential_backoff(max_retries=None, delay=datetime.timedelta(seconds=1))
        assert fn(_context(attempts=10)) is not None

    def test_exponential_backoff__cap_limits_delay(self):
        """Cap prevents the delay from exceeding the ceiling."""
        fn = exponential_backoff(
            delay=datetime.timedelta(seconds=1),
            cap=datetime.timedelta(seconds=5),
        )
        assert fn(_context(attempts=10)) == datetime.timedelta(seconds=5)


class TestLinearBackoff:
    """Tests for linear_backoff()."""

    def test_linear_backoff__first_attempt(self):
        """First retry uses delay * 1."""
        fn = linear_backoff(delay=datetime.timedelta(seconds=3))
        assert fn(_context(attempts=1)) == datetime.timedelta(seconds=3)

    def test_linear_backoff__second_attempt(self):
        """Second retry uses delay * 2."""
        fn = linear_backoff(delay=datetime.timedelta(seconds=3))
        assert fn(_context(attempts=2)) == datetime.timedelta(seconds=6)

    def test_linear_backoff__respects_max_retries(self):
        """Returns None once max_retries is exceeded."""
        fn = linear_backoff(delay=datetime.timedelta(seconds=1), max_retries=2)
        assert fn(_context(attempts=3)) is None


class TestConstantBackoff:
    """Tests for constant_backoff()."""

    def test_constant_backoff__always_same_delay(self):
        """Returns the same delay regardless of attempt count."""
        delay = datetime.timedelta(seconds=10)
        fn = constant_backoff(delay=delay)
        assert fn(_context(attempts=1)) == delay
        assert fn(_context(attempts=5)) == delay

    def test_constant_backoff__respects_max_retries(self):
        """Returns None once max_retries is exceeded."""
        fn = constant_backoff(delay=datetime.timedelta(seconds=1), max_retries=1)
        assert fn(_context(attempts=2)) is None


class TestWorkerThreadRetry:
    """Tests for WorkerThread.retry() using RetryableTask."""

    def _make_thread(self, backend=None):
        from django.tasks import default_task_backend

        return WorkerThread(
            worker=_make_worker(), index=0, backend=backend or default_task_backend
        )

    def test_retry__no_retry_fn(self):
        """retry() does nothing when the task has no retry_fn."""
        thread = self._make_thread()
        task_result = _failing_task_result(_plain_task)
        with patch.object(thread.backend, "enqueue") as mock_enqueue:
            thread.retry(task_result)
        mock_enqueue.assert_not_called()

    def test_retry__retry_fn_returns_none(self):
        """retry() does nothing when retry_fn returns None."""
        thread = self._make_thread()
        task_result = _failing_task_result(_no_retry_task)
        with patch.object(thread.backend, "enqueue") as mock_enqueue:
            thread.retry(task_result)
        mock_enqueue.assert_not_called()

    def test_retry__enqueues_with_delay(self):
        """retry() re-enqueues the task when retry_fn returns a timedelta."""
        thread = self._make_thread()
        task_result = _failing_task_result(_always_retry_task)
        with patch.object(thread.backend, "enqueue") as mock_enqueue:
            thread.retry(task_result)
        mock_enqueue.assert_called_once()
        enqueued_task = mock_enqueue.call_args[0][0]
        assert enqueued_task.run_after is not None
