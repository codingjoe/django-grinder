import datetime
import time
import uuid

import pytest
from django.tasks import TaskResult, TaskResultStatus
from django.tasks.base import TaskError
from django.utils import timezone

from threadmill.backends.base import Broker, ThreadmillTaskBackend
from threadmill.exceptions import AcknowledgementTimeout


class BackendDouble(ThreadmillTaskBackend):
    def enqueue(self, task, args, kwargs):
        return TaskResult(
            task=task,
            id=str(uuid.uuid4()),
            status=TaskResultStatus.READY,
            enqueued_at=timezone.now(),
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            backend=self.alias,
            errors=[],
            worker_ids=[],
            args=args,
            kwargs=kwargs,
        )


class TestAcknowledgeableTaskBackend:
    def test_acquire__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend acquire API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).acquire(
                timeout=datetime.timedelta(seconds=1)
            )

    def test_acknowledge__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend acknowledge API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).acknowledge(task_result=None)

    def test_requeue__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend requeue API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).requeue(
                task_result=None,
                run_after=timezone.now(),
            )

    def test_peek__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend peek_results API."""
        with pytest.raises(NotImplementedError):
            list(
                BackendDouble(alias="default", params={}).peek(
                    "default", status=TaskResultStatus.READY
                )
            )

    def test_telemetry__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend telemetry API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).telemetry()


class TestAcknowledgementTimeout:
    """Tests for the AcknowledgementTimeout exception."""

    def test_exception_can_be_instantiated(self) -> None:
        """AcknowledgementTimeout can be instantiated."""
        exc = AcknowledgementTimeout()
        assert isinstance(exc, Exception)

    def test_exception_can_be_used_in_task_error(self) -> None:
        """AcknowledgementTimeout can be used as a TaskError's exception_class_path."""
        error = TaskError(
            exception_class_path="threadmill.exceptions.AcknowledgementTimeout",
            traceback="Task processing lease expired.",
        )
        assert (
            error.exception_class_path == "threadmill.exceptions.AcknowledgementTimeout"
        )


class FakeBroker(Broker):
    """Broker that records main() calls for testing."""

    def __init__(
        self, *, interval: datetime.timedelta = datetime.timedelta(seconds=0.01)
    ) -> None:
        super().__init__(interval=interval)
        self.maintain_calls: list[float] = []

    def main(self) -> None:
        self.maintain_calls.append(time.monotonic())


class TestBroker:
    def test_main__is_noop(self) -> None:
        """Base Broker.main() is a no-op."""
        Broker(interval=datetime.timedelta(seconds=1)).main()

    def test_run__calls_maintain_then_exits_on_shutdown(self) -> None:
        """run() loops calling main() and exits after shutdown()."""
        broker = FakeBroker(interval=datetime.timedelta(seconds=0.01))
        broker.start()
        time.sleep(0.05)
        broker.shutdown()
        broker.join(timeout=1)
        assert not broker.is_alive()
        assert len(broker.maintain_calls) >= 1

    def test_interval_is_honored(self) -> None:
        """Broker waits at least interval between main() calls."""
        broker = FakeBroker(interval=datetime.timedelta(seconds=0.1))
        broker.start()
        time.sleep(0.25)
        broker.shutdown()
        broker.join(timeout=1)
        assert len(broker.maintain_calls) >= 2
        for i in range(1, len(broker.maintain_calls)):
            assert broker.maintain_calls[i] - broker.maintain_calls[i - 1] >= 0.09

    def test_shutdown__sets_event(self) -> None:
        """shutdown() sets the shutdown_requested event."""
        broker = Broker(interval=datetime.timedelta(seconds=1))
        assert not broker.shutdown_requested.is_set()
        broker.shutdown()
        assert broker.shutdown_requested.is_set()


from django.tasks import TaskResultStatus as TRS  # noqa: E402
from django.tasks.exceptions import InvalidTask  # noqa: E402
from django.utils import timezone as tz  # noqa: E402

from tests.testapp.tasks import (  # noqa: E402
    boom_with_retry,
    echo,
    retry_always,
)
from threadmill.backends.base import RetryTask  # noqa: E402


def _task_result_with_retry(task, *, errors=None, worker_ids=None) -> TaskResult:
    """Build a FAILED TaskResult with errors for retry testing."""
    now = tz.now()
    return TaskResult(
        task=task,
        id=str(uuid.uuid4()),
        status=TRS.FAILED,
        enqueued_at=now,
        started_at=now,
        finished_at=now,
        last_attempted_at=now,
        backend="default",
        errors=errors or [],
        worker_ids=worker_ids or [],
        args=[],
        kwargs={},
    )


class TestRetryTask:
    """Tests for the RetryTask class."""

    def test_retry_path__none_when_no_retry(self) -> None:
        """Return None when retry callback is not set."""
        task = RetryTask(func=echo.func)
        assert task.retry_path is None

    def test_retry_path__returns_dotted_path(self) -> None:
        """Return the importable dotted path of the retry callback."""
        task = RetryTask(func=echo.func, retry=retry_always)
        assert task.retry_path == "tests.testapp.tasks.retry_always"

    def test_validate_task__accepts_module_level_function(self) -> None:
        """validate_task accepts a module-level retry callback."""
        backend = BackendDouble(alias="default", params={})
        backend.validate_task(boom_with_retry)

    def test_validate_task__rejects_lambda(self) -> None:
        """Reject a non-module-level retry callback during task creation."""
        with pytest.raises(InvalidTask):
            RetryTask(
                func=echo.func,
                retry=lambda ctx: None,
            )


class TestRetryTaskSerialization:
    """Tests for RetryTask JSON and pickle serialization."""

    def test_serialize__encodes_retry_as_path(self) -> None:
        """TaskResultEncoder serializes retry as a dotted path."""
        result = _task_result_with_retry(boom_with_retry)
        data = ThreadmillTaskBackend.serialize_task_result(result)
        assert "tests.testapp.tasks.retry_always" in data

    def test_serialize__omits_retry_when_none(self) -> None:
        """TaskResultEncoder omits retry when it is None."""
        task = RetryTask(func=echo.func)
        result = TaskResult(
            task=task,
            id=str(uuid.uuid4()),
            status=TRS.READY,
            enqueued_at=tz.now(),
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            backend="default",
            errors=[],
            worker_ids=[],
            args=[],
            kwargs={},
        )
        data = ThreadmillTaskBackend.serialize_task_result(result)
        import json

        decoded = json.loads(data)
        assert decoded["task"]["retry"] is None

    def test_deserialize__roundtrips_retry_callback(self) -> None:
        """deserialize_task_result restores the retry callable from path."""
        result = _task_result_with_retry(boom_with_retry)
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert restored.task.retry is retry_always
        assert restored.task.retry_path == "tests.testapp.tasks.retry_always"

    def test_deserialize__roundtrips_without_retry(self) -> None:
        """deserialize_task_result works when retry is None."""
        result = _task_result_with_retry(echo)
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert restored.task.retry is None

    def test_pickle__roundtrips_retry_callback(self) -> None:
        """Pickle preserves the retry callable by importable path."""
        import pickle

        result = _task_result_with_retry(boom_with_retry)
        restored = pickle.loads(pickle.dumps(result))  # noqa: S301
        assert restored.task.retry is retry_always

    def test_pickle__roundtrips_without_retry(self) -> None:
        """Pickle works when retry is None."""
        import pickle

        result = _task_result_with_retry(echo)
        restored = pickle.loads(pickle.dumps(result))  # noqa: S301
        assert restored.task.retry is None

    def test_deserialize__preserves_errors(self) -> None:
        """deserialize_task_result preserves accumulated errors."""
        from django.tasks.base import TaskError

        error = TaskError(
            exception_class_path="ValueError",
            traceback="traceback",
        )
        result = _task_result_with_retry(boom_with_retry, errors=[error])
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert len(restored.errors) == 1
        assert restored.errors[0].exception_class_path == "ValueError"
