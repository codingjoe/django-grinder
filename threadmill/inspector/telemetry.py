"""In-memory rolling telemetry buffer fed by backend pub/sub events."""

import collections
import datetime
import time

from threadmill.backends.base import QueueRates, TelemetryDirection, TelemetryEvent

WINDOW_SECONDS = 60
"""Rolling window the inspector displays for ingress/egress sparklines."""

_DirectionBuckets = dict[int, int]
"""second-since-epoch -> count, per (queue, direction)."""


class TelemetryBuffer:
    """Per-(queue, direction) rolling 60-second count buffer.

    Events arrive as :class:`TelemetryEvent` instances from the backend's
    pub/sub stream and are bucketed by receipt time so worker clock skew
    can't distort the series.
    """

    def __init__(self, *, window: int = WINDOW_SECONDS) -> None:
        self._window = window
        self._counts: dict[tuple[str, TelemetryDirection], _DirectionBuckets] = (
            collections.defaultdict(lambda: collections.defaultdict(int))
        )

    def record(self, event: TelemetryEvent, *, now: float | None = None) -> None:
        """Increment the current-second bucket for the event's queue+direction."""
        second = int(now if now is not None else time.monotonic())
        self._counts[event.queue_name, event.direction][second] += 1
        self._prune(second)

    def series(
        self,
        queue_name: str,
        direction: TelemetryDirection,
        *,
        now: float | None = None,
    ) -> list[float]:
        """Return ``window`` per-second counts, oldest to newest, for the pair."""
        second = int(now if now is not None else time.monotonic())
        self._prune(second)
        buckets = self._counts.get((queue_name, direction), {})
        return [
            float(buckets.get(second - offset, 0))
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
        self._prune(second)
        span = min(int(interval.total_seconds()), self._window)
        ingress = sum(
            self._counts.get((queue_name, TelemetryDirection.INGRESS), {}).get(
                second - offset, 0
            )
            for offset in range(span)
        )
        egress = sum(
            self._counts.get((queue_name, TelemetryDirection.EGRESS), {}).get(
                second - offset, 0
            )
            for offset in range(span)
        )
        return QueueRates(interval=interval, ingress=ingress, egress=egress)

    def _prune(self, now_second: int) -> None:
        """Drop buckets older than the window to bound memory."""
        cutoff = now_second - self._window
        for buckets in self._counts.values():
            for stale in [s for s in buckets if s <= cutoff]:
                del buckets[stale]
