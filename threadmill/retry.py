"""Task retry and backoff utilities."""

from __future__ import annotations

import dataclasses
import datetime
from collections.abc import Callable

from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, DEFAULT_TASK_QUEUE_NAME, Task
from django.tasks.base import DEFAULT_TASK_PRIORITY, TaskContext

RetryFunction = Callable[[TaskContext], datetime.timedelta | None]


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class RetryableTask(Task):
    """A task that retries automatically on failure via a configurable retry function."""

    retry_fn: RetryFunction | None = None


def exponential_backoff(
    *,
    base: float = 2,
    delay: datetime.timedelta = datetime.timedelta(seconds=1),
    max_retries: int | None = None,
    cap: datetime.timedelta | None = None,
) -> RetryFunction:
    """Return a retry function using exponential back-off.

    The delay grows as ``delay * base ** (attempt - 1)``.

    Args:
        base: The multiplicative factor applied each attempt.
        delay: The base delay for the first retry.
        max_retries: Maximum number of retries after the first failure.
            ``None`` retries indefinitely.  With ``max_retries=3`` the task
            will be attempted at most 4 times total (original + 3 retries).
        cap: Upper bound on the computed delay.
    """

    def retry_fn(context: TaskContext) -> datetime.timedelta | None:
        if max_retries is not None and context.task_result.attempts > max_retries:
            return None
        next_delay = delay * (base ** (context.task_result.attempts - 1))
        if cap is not None:
            next_delay = min(next_delay, cap)
        return next_delay

    return retry_fn


def linear_backoff(
    *,
    delay: datetime.timedelta,
    max_retries: int | None = None,
) -> RetryFunction:
    """Return a retry function using linear back-off.

    The delay grows as ``delay * attempt``.

    Args:
        delay: The base delay multiplied by the current attempt number.
        max_retries: Maximum number of retries after the first failure.
            ``None`` retries indefinitely.  With ``max_retries=3`` the task
            will be attempted at most 4 times total (original + 3 retries).
    """

    def retry_fn(context: TaskContext) -> datetime.timedelta | None:
        if max_retries is not None and context.task_result.attempts > max_retries:
            return None
        return delay * context.task_result.attempts

    return retry_fn


def constant_backoff(
    *,
    delay: datetime.timedelta,
    max_retries: int | None = None,
) -> RetryFunction:
    """Return a retry function using a constant (fixed) delay.

    Args:
        delay: The fixed delay between every retry.
        max_retries: Maximum number of retries after the first failure.
            ``None`` retries indefinitely.  With ``max_retries=3`` the task
            will be attempted at most 4 times total (original + 3 retries).
    """

    def retry_fn(context: TaskContext) -> datetime.timedelta | None:
        if max_retries is not None and context.task_result.attempts > max_retries:
            return None
        return delay

    return retry_fn


def task(
    function=None,
    *,
    priority: int = DEFAULT_TASK_PRIORITY,
    queue_name: str = DEFAULT_TASK_QUEUE_NAME,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
    takes_context: bool = False,
    retry_fn: RetryFunction | None = None,
):
    """Decorate a function as a Django task with optional retry support.

    Returns a :class:`RetryableTask` instance, which stores ``retry_fn``
    as a proper field so the executor can look it up after a failure.

    Args:
        function: The function to decorate (when used without arguments).
        priority: Task priority.
        queue_name: Queue name.
        backend: Backend alias.
        takes_context: Whether the task receives a :class:`~django.tasks.TaskContext`.
        retry_fn: Called on failure; returns the delay until the next attempt
            or ``None`` to stop retrying.
    """

    def wrapper(f):
        return RetryableTask(
            priority=priority,
            func=f,
            queue_name=queue_name,
            backend=backend,
            takes_context=takes_context,
            run_after=None,
            retry_fn=retry_fn,
        )

    if function is not None:
        return wrapper(function)
    return wrapper
