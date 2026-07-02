"""Shared helpers for the inspector TUI."""

from __future__ import annotations

import datetime
import math
import typing

from django.tasks import task_backends

from ..backends.base import ThreadmillTaskBackend


def si_prefix(n: int | float, base: int = 1_000) -> str:
    """Round and shorten a number with an SI prefix (k, M, G, etc.)."""
    if n < 1000:
        return str(n)
    prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"]
    m = min(int(math.log10(abs(n)) // 3), len(prefixes) - 1)
    if (value := n / base**m) and value >= 100:
        return f"{int(value)}{prefixes[m]}"
    return f"{value:.1f}".rstrip("0").rstrip(".") + prefixes[m]


def format_dt(value: datetime.datetime | None) -> str:
    """Format a datetime for display, or return empty string."""
    return value.isoformat() if value else ""


def supported_aliases() -> typing.Generator[tuple[str, str]]:
    """Yield (alias, alias) pairs for all ThreadmillTaskBackend instances."""
    for alias in task_backends:
        if isinstance(task_backends[alias], ThreadmillTaskBackend):
            yield alias, alias
