"""In-memory rolling telemetry buffer fed by backend pub/sub events."""

import collections
import datetime
import time

from threadmill.backends.base import QueueRates, TelemetryDirection, TelemetryEvent

WINDOW_SECONDS = 60
"""Rolling window the inspector displays for ingress/egress sparklines."""


class TelemetryBuffer:
    """Per-(queue, direction) rolling 60-second count buffer."""

    def __init__(self, *, window: int = WINDOW_SECONDS) -> None:
        self._window = window
        self._counts: dict[
            tuple[str, TelemetryDirection], collections.OrderedDict[int, int]
        ] = collections.defaultdict(collections.OrderedDict)

    def record(self, event: TelemetryEvent, *, now: float | None = None) -> None:
        """Increment the current-second bucket for the event's queue+direction."""
        second = int(now if now is not None else time.monotonic())
        buckets = self._counts[event.queue_name, event.direction]
        buckets[second] = buckets.get(second, 0) + 1
        self._evict(buckets, second)

    def series(
        self,
        queue_name: str,
        direction: TelemetryDirection,
        *,
        now: float | None = None,
    ) -> list[float]:
        """Return ``window`` per-second counts, oldest to newest, for the pair."""
        second = int(now if now is not None else time.monotonic())
        buckets = self._counts.get((queue_name, direction))
        if buckets is not None:
            self._evict(buckets, second)
        return [
            float(buckets.get(second - offset, 0)) if buckets else 0.0
            for offset in reversed(range(self._window))
        ]

    def rates_for(
        self,
        queue_name: str,
        *,
        interval: datetime.timedelta = datetime.timedelta(seconds=WINDOW_SECONDS),
        now: float | None = None,
    ) -> QueueRates:
        """Sum the last ``interval`` of events into per-second ingress/egress rates."""
        second = int(now if now is not None else time.monotonic())
        span = min(int(interval.total_seconds()), self._window)
        ingress = self._sum_direction(
            queue_name, TelemetryDirection.INGRESS, second, span
        )
        egress = self._sum_direction(
            queue_name, TelemetryDirection.EGRESS, second, span
        )
        return QueueRates(interval=interval, ingress=ingress, egress=egress)

    def _sum_direction(
        self, queue_name: str, direction: TelemetryDirection, second: int, span: int
    ) -> int:
        buckets = self._counts.get((queue_name, direction))
        if buckets is None:
            return 0
        self._evict(buckets, second)
        return sum(buckets.get(second - offset, 0) for offset in range(span))

    def _evict(
        self, buckets: collections.OrderedDict[int, int], now_second: int
    ) -> None:
        """Drop buckets older than the window (LRU pop from front)."""
        cutoff = now_second - self._window
        while buckets and next(iter(buckets)) <= cutoff:
            buckets.popitem(last=False)
