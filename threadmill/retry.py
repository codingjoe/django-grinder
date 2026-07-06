"""Reusable callables for retrying tasks."""

import dataclasses
import datetime
import typing

if typing.TYPE_CHECKING:
    from django.tasks import TaskContext


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class ExponentialBackoff:
    """
    Exponential backoff retry strategy.

    Usage:

        @task(retry=ExponentialBackoff(base_delay=datetime.timedelta(seconds=1), max_delay=datetime.timedelta(minutes=5), factor=2.0, max_retries=5))
        def my_task():
            ...
    """

    base_delay: datetime.timedelta = dataclasses.field(
        default=datetime.timedelta(seconds=1), doc="Base delay in seconds"
    )
    max_delay: datetime.timedelta = dataclasses.field(
        default=datetime.timedelta(minutes=60), doc="Maximum delay in seconds"
    )
    factor: float = dataclasses.field(default=2.0, doc="Exponential factor for backoff")
    max_retries: int = dataclasses.field(default=5, doc="Maximum number of retries")
    expected_exceptions: tuple[type[Exception], ...] = dataclasses.field(
        default=(Exception,), doc="Tuple of exception classes that trigger a retry"
    )

    def __call__(self, context: TaskContext) -> datetime.timedelta | None:
        if context.attempt < self.max_retries and issubclass(
            context.task_result.errors[-1].exception_class, self.expected_exceptions
        ):
            return min(self.base_delay * (self.factor**context.attempt), self.max_delay)

    def deconstruct(self):
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            (),
            dataclasses.asdict(self),
        )
