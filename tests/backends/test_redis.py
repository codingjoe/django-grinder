import dataclasses
import datetime
import logging
import time
from dataclasses import replace
from unittest.mock import patch

from django.tasks import default_task_backend
from django.tasks.base import TaskResultStatus
from django.utils import timezone

from tests.testapp.tasks import boom, boom_with_retry, compute_workload, echo
from threadmill.backends.base import (
    BackendTelemetry,
    QueueCounts,
    QueueRates,
    QueueStats,
)
from threadmill.backends.redis import RedisBroker, RedisTaskBackend  # noqa: E402

TELEMETRY_INTERVAL = datetime.timedelta(seconds=60)


def _stats(**overrides: int | datetime.timedelta) -> QueueStats:
    interval = overrides.pop("interval", TELEMETRY_INTERVAL)
    counts = QueueCounts(
        ready=overrides.get("ready", 0),
        running=overrides.get("running", 0),
        deferred=overrides.get("deferred", 0),
        successful=overrides.get("successful", 0),
        failed=overrides.get("failed", 0),
    )
    rates = QueueRates(
        interval=interval,
        ingress=overrides.get("ingress", 0),
        egress=overrides.get("egress", 0),
    )
    return QueueStats(counts=counts, rates=rates)


class TestRedisBroker:
    def test_mover__moves_deferred_task_to_ready(self):
        """Mover promotes due deferred tasks to the ready queue."""
        deferred_task = replace(
            compute_workload,
            run_after=timezone.now() - datetime.timedelta(seconds=10),
        )
        task_result = default_task_backend.enqueue(deferred_task, args=[])
        broker = RedisBroker(default_task_backend)
        broker.main()
        acquired = default_task_backend.acquire(timeout=datetime.timedelta(seconds=1))
        assert acquired is not None
        assert acquired.id == task_result.id

    def test_error_path__maintain_continues_after_exception(self, caplog):
        """main() logs and continues when any per-queue step raises."""
        broker = RedisBroker(default_task_backend)
        with caplog.at_level(logging.ERROR):
            with (
                patch.object(broker, "_move_queue", side_effect=RuntimeError("mover")),
                patch.object(
                    broker, "_reap_running_queue", side_effect=RuntimeError("reaper")
                ),
            ):
                broker.main()
        assert "Mover error for queue" in caplog.text
        assert "Running reaper error for queue" in caplog.text


class TestRedisTaskBackend:
    """Tests for the RedisTaskBackend update and lease functionality."""

    def test_acquire__moves_to_running_set(self):
        """acquire() moves task directly to running set with worker info."""
        backend = RedisTaskBackend(
            "acquire_running_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="worker-1"
            )
            assert acquired is not None
            assert acquired.id == task_result.id

            # Verify task is in running set, not in any processing set
            running_key = backend._segment_key(TaskResultStatus.RUNNING, "default")
            assert backend.client.zscore(running_key, task_result.id) is not None

            # Verify task data was updated with worker info
            task_key = backend.TASK_KEY.format(
                prefix=backend.key_prefix, task_id=task_result.id
            )
            stored_data = backend.client.hget(task_key, "data")
            deserialized = backend.deserialize_task_result(stored_data)
            assert deserialized.status == TaskResultStatus.RUNNING
            assert deserialized.worker_ids == ["worker-1"]
            assert deserialized.last_attempted_at is not None
        finally:
            backend.close()

    def test_acquire__sets_last_attempted_at(self):
        """acquire() sets last_attempted_at and worker_ids in the stored task data."""
        backend = RedisTaskBackend(
            "last_attempted_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="test-worker"
            )
            assert acquired is not None
            assert acquired.last_attempted_at is not None
            assert acquired.worker_ids == ["test-worker"]

            # Verify it's persisted in Redis
            task_key = backend.TASK_KEY.format(
                prefix=backend.key_prefix, task_id=task_result.id
            )
            stored_data = backend.client.hget(task_key, "data")
            deserialized = backend.deserialize_task_result(stored_data)
            assert deserialized.last_attempted_at is not None
            assert deserialized.worker_ids == ["test-worker"]
        finally:
            backend.close()

    def test_running_reaper__fails_expired_tasks(self):
        """Running reaper creates FAILED results for tasks with expired lease."""
        backend = RedisTaskBackend(
            "running_reaper_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(seconds=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="reaper-test"
            )
            assert acquired is not None

            # Wait for lease to expire
            time.sleep(1.1)

            # Run the broker
            broker = RedisBroker(backend)
            broker.main()

            # Verify the task result exists and is FAILED
            result = backend.get_result(task_result.id)
            assert result.status == TaskResultStatus.FAILED
            assert len(result.errors) == 1
            assert "AcknowledgementTimeout" in result.errors[0].exception_class_path

            # Reaping an expired task records a failed result. Live egress
            # now arrives via pub/sub, so backend.queue_stats() rates are zero.
            stats = backend.queue_stats().queues["default"]
            assert stats.rates.egress == 0
            assert stats.counts.failed == 1
            assert stats.counts.successful == 0
        finally:
            backend.close()

    def test_stale_acknowledge__is_noop(self):
        """acknowledge() is a no-op when the task is no longer in the running set."""
        backend = RedisTaskBackend(
            "stale_ack_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(seconds=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="stale-ack-test"
            )
            assert acquired is not None

            # Wait for lease to expire
            time.sleep(1.1)

            # Run the broker to reap the running set
            broker = RedisBroker(backend)
            broker.main()

            # Try to acknowledge the task (should be a no-op since it was reaped)
            finished = dataclasses.replace(
                acquired,
                status=TaskResultStatus.SUCCESSFUL,
                finished_at=timezone.now(),
            )
            # This should not raise
            backend.acknowledge(finished)

            # The result should still be the FAILED one from the reaper
            result = backend.get_result(task_result.id)
            assert result.status == TaskResultStatus.FAILED
        finally:
            backend.close()

    def test_telemetry__empty_backend(self):
        """Telemetry returns zero counts for an empty backend."""
        backend = RedisTaskBackend(
            "telemetry_empty_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            telemetry = backend.queue_stats()
            assert telemetry == BackendTelemetry(queues={"default": _stats()})
        finally:
            backend.close()

    def test_telemetry__counts_tasks(self):
        """Telemetry reports per-status counts; rates come from pub/sub, not polling."""
        backend = RedisTaskBackend(
            "telemetry_counts_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            backend.enqueue(echo, args=[42])
            backend.enqueue(boom, args=[])

            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="telemetry-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.SUCCESSFUL,
                    finished_at=timezone.now(),
                )
            )

            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="telemetry-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.FAILED,
                    finished_at=timezone.now(),
                )
            )

            telemetry = backend.queue_stats()
            assert telemetry.queues["default"] == _stats(
                successful=1,
                failed=1,
            )
        finally:
            backend.close()

    def test_telemetry__counts_successful_and_failed(self):
        """Telemetry counts successful and finished results; polling rates stay zero."""
        backend = RedisTaskBackend(
            "telemetry_egress_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            backend.enqueue(echo, args=[1])
            backend.enqueue(echo, args=[2])
            backend.enqueue(echo, args=[3])

            for _ in range(2):
                acquired = backend.acquire(
                    timeout=datetime.timedelta(seconds=1), worker="egress-test"
                )
                assert acquired is not None
                backend.acknowledge(
                    dataclasses.replace(
                        acquired,
                        status=TaskResultStatus.SUCCESSFUL,
                        finished_at=timezone.now(),
                    )
                )
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="egress-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.FAILED,
                    finished_at=timezone.now(),
                )
            )

            stats = backend.queue_stats().queues["default"]
            # Rates come from the pub/sub buffer, not from Redis polling.
            assert stats.rates.ingress == 0
            assert stats.rates.egress == 0
            assert stats.counts.successful == 2
            assert stats.counts.failed == 1
        finally:
            backend.close()

    def test_telemetry__successful_failed_evicted_by_result_ttl(self):
        """successful/failed segment counts drop when results age out of result_ttl."""
        backend = RedisTaskBackend(
            "telemetry_eviction_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            successful_key = backend._segment_key(
                TaskResultStatus.SUCCESSFUL, "default"
            )
            failed_key = backend._segment_key(TaskResultStatus.FAILED, "default")

            def _ack(status: TaskResultStatus) -> str:
                enqueued = backend.enqueue(echo, args=[1])
                acquired = backend.acquire(
                    timeout=datetime.timedelta(seconds=1), worker="eviction-test"
                )
                assert acquired is not None
                backend.acknowledge(
                    dataclasses.replace(
                        acquired, status=status, finished_at=timezone.now()
                    )
                )
                return enqueued.id

            first_successful = _ack(TaskResultStatus.SUCCESSFUL)
            first_failed = _ack(TaskResultStatus.FAILED)
            assert backend.client.zcard(successful_key) == 1
            assert backend.client.zcard(failed_key) == 1

            # Age the first results beyond the retention horizon.
            old = (timezone.now() - datetime.timedelta(seconds=120)).timestamp() * 1000
            backend.client.zadd(successful_key, {first_successful: old})
            backend.client.zadd(failed_key, {first_failed: old})

            # A subsequent acknowledge of each status evicts results older than result_ttl.
            _ack(TaskResultStatus.SUCCESSFUL)
            _ack(TaskResultStatus.FAILED)

            assert backend.client.zcard(successful_key) == 1
            assert backend.client.zcard(failed_key) == 1
            stats = backend.queue_stats().queues["default"]
            assert stats.counts.successful == 1
            assert stats.counts.failed == 1
        finally:
            backend.close()

    @staticmethod
    def _await_message(pubsub, expected: bytes, *, timeout: float = 2.0):
        """Drain pubsub until a user message with the expected payload arrives."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = pubsub.get_message(timeout=0.1)
            if message and message.get("type") == "message":
                if message["data"] == expected:
                    return message
        raise AssertionError(f"no pubsub message {expected!r} received")

    @staticmethod
    def _drain_subscription(pubsub, *, timeout: float = 2.0):
        """Block until the pubsub connection reports it is subscribed."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pubsub.get_message(timeout=0.1):
                if pubsub.subscribed:
                    return
        raise AssertionError("pubsub never reached subscribed state")

    def test_enqueue__publishes_ingress_telemetry(self):
        """enqueue() publishes an ingress event on the telemetry channel."""
        backend = RedisTaskBackend(
            "telemetry_publish_ingress_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        pubsub = backend.client.pubsub()
        try:
            pubsub.subscribe(backend.telemetry_channel)
            self._drain_subscription(pubsub)
            backend.enqueue(echo, args=[1])
            message = self._await_message(pubsub, b"ingress:default")
            assert message["channel"] == backend.telemetry_channel.encode()
        finally:
            pubsub.unsubscribe(backend.telemetry_channel)
            pubsub.close()
            backend.close()

    def test_acknowledge__publishes_egress_telemetry(self):
        """acknowledge() publishes an egress event on the telemetry channel."""
        backend = RedisTaskBackend(
            "telemetry_publish_egress_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        pubsub = backend.client.pubsub()
        try:
            backend.enqueue(echo, args=[1])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="publish-test"
            )
            pubsub.subscribe(backend.telemetry_channel)
            self._drain_subscription(pubsub)
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.SUCCESSFUL,
                    finished_at=timezone.now(),
                )
            )
            message = self._await_message(pubsub, b"egress:default")
            assert message["channel"] == backend.telemetry_channel.encode()
        finally:
            pubsub.unsubscribe(backend.telemetry_channel)
            pubsub.close()
            backend.close()

    def test_requeue__publishes_ingress_telemetry(self):
        """requeue() publishes an ingress event for the re-queued task."""
        backend = RedisTaskBackend(
            "telemetry_publish_requeue_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        pubsub = backend.client.pubsub()
        try:
            backend.enqueue(echo, args=[1])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="requeue-publish-test"
            )
            assert acquired is not None
            failed = dataclasses.replace(
                acquired,
                status=TaskResultStatus.FAILED,
                finished_at=timezone.now(),
            )
            pubsub.subscribe(backend.telemetry_channel)
            self._drain_subscription(pubsub)
            backend.requeue(failed, timezone.now() + datetime.timedelta(seconds=10))
            message = self._await_message(pubsub, b"ingress:default")
            assert message["channel"] == backend.telemetry_channel.encode()
        finally:
            pubsub.unsubscribe(backend.telemetry_channel)
            pubsub.close()
            backend.close()

    def _acknowledge(self, status: TaskResultStatus) -> str:
        """Enqueue, acquire, and acknowledge a task with the given status."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        acquired = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="peek-test"
        )
        assert acquired.id == task_result.id
        default_task_backend.acknowledge(
            dataclasses.replace(acquired, status=status, finished_at=timezone.now())
        )
        return task_result.id

    def test_peek__ready_tasks(self):
        """Peek READY returns enqueued tasks in queue order."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.enqueue(echo, args=[2])
        results = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.READY, count=10
            )
        )
        assert [r.args for r in results] == [[1], [2]]

    def test_peek__running_tasks(self):
        """Peek RUNNING returns acquired tasks with worker info."""
        default_task_backend.enqueue(echo, args=[1])
        acquired = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="peek-test"
        )
        results = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.RUNNING, count=10
            )
        )
        assert [r.id for r in results] == [acquired.id]
        assert results[0].status == TaskResultStatus.RUNNING

    def test_peek__successful_and_failed_history(self):
        """Peek SUCCESSFUL/FAILED filter acknowledged results by status."""
        successful_id = self._acknowledge(TaskResultStatus.SUCCESSFUL)
        failed_id = self._acknowledge(TaskResultStatus.FAILED)
        successful = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.SUCCESSFUL, count=10
            )
        )
        failed = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.FAILED, count=10
            )
        )
        assert [r.id for r in successful] == [successful_id]
        assert [r.id for r in failed] == [failed_id]

    def test_peek__skips_expired_task_data(self):
        """Peek skips queue entries whose task data hash has expired."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        default_task_backend.client.delete(
            default_task_backend.TASK_KEY.format(
                prefix=default_task_backend.key_prefix, task_id=task_result.id
            )
        )
        results = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.READY, count=10
            )
        )
        assert results == []

    def test_peek__skips_expired_result_data(self):
        """Peek skips history entries whose result key has expired."""
        result_id = self._acknowledge(TaskResultStatus.SUCCESSFUL)
        default_task_backend.client.delete(
            default_task_backend.RESULT_KEY.format(
                prefix=default_task_backend.key_prefix, result_id=result_id
            )
        )
        results = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.SUCCESSFUL, count=10
            )
        )
        assert results == []

    def test_peek__empty_history_returns_nothing(self):
        """Peek SUCCESSFUL/FAILED yields nothing when the history is empty."""
        successful = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.SUCCESSFUL, count=10
            )
        )
        failed = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.FAILED, count=10
            )
        )
        assert successful == []
        assert failed == []

    def test_requeue__moves_from_running_to_deferred(self) -> None:
        """requeue() removes from running set and adds to deferred set."""
        backend = RedisTaskBackend(
            "requeue_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(boom_with_retry, args=[])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="requeue-test"
            )
            assert acquired is not None

            # Simulate a failed execution
            from django.tasks.base import TaskError

            failed = dataclasses.replace(
                acquired,
                status=TaskResultStatus.FAILED,
                finished_at=timezone.now(),
                errors=[
                    TaskError(
                        exception_class_path="ValueError",
                        traceback="ValueError: boom",
                    )
                ],
            )

            run_after = timezone.now() + datetime.timedelta(seconds=10)
            backend.requeue(failed, run_after)

            running_key = backend._segment_key(TaskResultStatus.RUNNING, "default")
            deferred_key = backend.DEFERRED_KEY.format(
                prefix=backend.key_prefix, queue_name="default"
            )
            assert backend.client.zscore(running_key, task_result.id) is None
            assert backend.client.zscore(deferred_key, task_result.id) is not None
        finally:
            backend.close()

    def test_requeue__preserves_id_and_errors(self) -> None:
        """requeue() preserves the task ID and accumulated errors."""
        backend = RedisTaskBackend(
            "requeue_preserve_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(boom_with_retry, args=[])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="preserve-test"
            )
            assert acquired is not None

            from django.tasks.base import TaskError

            error = TaskError(
                exception_class_path="ValueError",
                traceback="ValueError: boom",
            )
            failed = dataclasses.replace(
                acquired,
                status=TaskResultStatus.FAILED,
                finished_at=timezone.now(),
                errors=[error],
            )

            run_after = timezone.now() + datetime.timedelta(seconds=10)
            backend.requeue(failed, run_after)

            # Verify the stored data preserves ID and errors
            task_key = backend.TASK_KEY.format(
                prefix=backend.key_prefix, task_id=task_result.id
            )
            stored_data = backend.client.hget(task_key, "data")
            restored = backend.deserialize_task_result(stored_data)
            assert restored.id == task_result.id
            assert len(restored.errors) == 1
            assert restored.errors[0].exception_class_path == "ValueError"
            assert restored.status == TaskResultStatus.READY
            assert restored.started_at is None
            assert restored.finished_at is None
        finally:
            backend.close()

    def test_requeue__task_is_re_acquirable_after_delay(self) -> None:
        """Requeued task can be acquired after run_after has elapsed."""
        backend = RedisTaskBackend(
            "requeue_acquire_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(boom_with_retry, args=[])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="requeue-acq-test"
            )
            assert acquired is not None

            from django.tasks.base import TaskError

            failed = dataclasses.replace(
                acquired,
                status=TaskResultStatus.FAILED,
                finished_at=timezone.now(),
                errors=[
                    TaskError(
                        exception_class_path="ValueError",
                        traceback="ValueError: boom",
                    )
                ],
            )

            # Requeue with a past run_after so it's immediately due
            run_after = timezone.now() - datetime.timedelta(seconds=1)
            backend.requeue(failed, run_after)

            # Run the broker to move the deferred task to the ready queue
            broker = RedisBroker(backend)
            broker.main()

            # The task should be acquirable again
            re_acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="requeue-acq-test-2"
            )
            assert re_acquired is not None
            assert re_acquired.id == task_result.id
        finally:
            backend.close()

    def test_requeue__cleans_up_failed_and_result_keys(self) -> None:
        """requeue() removes the task from the failed zset and deletes its result key."""
        backend = RedisTaskBackend(
            "requeue_cleanup_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            backend.enqueue(echo, args=[1])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="cleanup-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.FAILED,
                    finished_at=timezone.now(),
                )
            )
            failed = next(
                backend.peek(
                    queue_name="default", status=TaskResultStatus.FAILED, count=10
                )
            )
            failed_key = backend._segment_key(TaskResultStatus.FAILED, "default")
            result_key = backend.RESULT_KEY.format(
                prefix=backend.key_prefix, result_id=failed.id
            )
            assert backend.client.zscore(failed_key, failed.id) is not None
            assert backend.client.exists(result_key)

            backend.requeue(failed, timezone.now() + datetime.timedelta(seconds=10))

            assert backend.client.zscore(failed_key, failed.id) is None
            assert not backend.client.exists(result_key)
        finally:
            backend.close()

    def test_dequeue__removes_ready_task_from_queue(self) -> None:
        """dequeue() removes a ready task from the queue zset."""
        backend = RedisTaskBackend(
            "dequeue_ready_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[1])
            queue_key = backend._segment_key(TaskResultStatus.READY, "default")
            assert backend.client.zscore(queue_key, task_result.id) is not None

            backend.dequeue(task_result)

            assert backend.client.zscore(queue_key, task_result.id) is None
        finally:
            backend.close()

    def test_dequeue__removes_failed_task_from_results(self) -> None:
        """dequeue() removes a failed task from the failed zset."""
        backend = RedisTaskBackend(
            "dequeue_failed_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            backend.enqueue(echo, args=[1])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="dequeue-failed-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.FAILED,
                    finished_at=timezone.now(),
                )
            )
            failed = next(
                backend.peek(
                    queue_name="default", status=TaskResultStatus.FAILED, count=10
                )
            )
            failed_key = backend._segment_key(TaskResultStatus.FAILED, "default")

            backend.dequeue(failed)

            assert backend.client.zscore(failed_key, failed.id) is None
        finally:
            backend.close()

    def test_purge__removes_all_tasks_across_segments(self) -> None:
        """purge_queue() deletes every task across all segments."""
        backend = RedisTaskBackend(
            "purge_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            # Two ready tasks
            backend.enqueue(echo, args=[1])
            backend.enqueue(echo, args=[2])
            # One running task
            backend.acquire(timeout=datetime.timedelta(seconds=1), worker="purge-test")
            # One failed task
            backend.enqueue(echo, args=[3])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="purge-test-2"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.FAILED,
                    finished_at=timezone.now(),
                )
            )
            # One successful task
            backend.enqueue(echo, args=[4])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="purge-test-3"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.SUCCESSFUL,
                    finished_at=timezone.now(),
                )
            )

            backend.purge("default")
            assert (
                list(
                    backend.peek(
                        queue_name="default", status=TaskResultStatus.READY, count=10
                    )
                )
                == []
            )
            assert (
                list(
                    backend.peek(
                        queue_name="default", status=TaskResultStatus.RUNNING, count=10
                    )
                )
                == []
            )
            assert (
                list(
                    backend.peek(
                        queue_name="default", status=TaskResultStatus.FAILED, count=10
                    )
                )
                == []
            )
            assert (
                list(
                    backend.peek(
                        queue_name="default",
                        status=TaskResultStatus.SUCCESSFUL,
                        count=10,
                    )
                )
                == []
            )
        finally:
            backend.close()

    def test_purge__empty_queue_is_noop(self) -> None:
        """purge_queue() on an empty queue is a no-op."""
        backend = RedisTaskBackend(
            "purge_empty_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            backend.purge("default")
            assert (
                list(
                    backend.peek(
                        queue_name="default", status=TaskResultStatus.READY, count=10
                    )
                )
                == []
            )
        finally:
            backend.close()
