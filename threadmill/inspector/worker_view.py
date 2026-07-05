"""Worker telemetry widgets for the inspector TUI."""

from __future__ import annotations

import dataclasses
import logging
from collections import deque
from typing import Any

from textual.app import ComposeResult
from textual.reactive import reactive
from textual.widgets import Sparkline, Static, Tree
from textual.widgets.tree import TreeNode

from ..backends.base import BackendTelemetry, NodeTelemetry, WorkerTelemetry
from .utils import si_prefix

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class WorkerTreeNode:
    """A node in the Queue -> Node selection tree."""

    kind: str
    label: str
    queue_name: str = ""
    hostname: str = ""


class SelectionTree(Tree[WorkerTreeNode]):
    """Queue -> Node hierarchy built from worker telemetry."""

    worker_telemetry: reactive[WorkerTelemetry | None] = reactive(None)
    queue_telemetry: reactive[BackendTelemetry | None] = reactive(None)
    _last_structure: frozenset[tuple[str, str]] | None = None

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.border_title = "Selection"
        self.show_root = False

    def watch_worker_telemetry(self, telemetry: WorkerTelemetry | None) -> None:
        if telemetry is not None:
            self.update_telemetry()

    def watch_queue_telemetry(self, telemetry: BackendTelemetry | None) -> None:
        if telemetry is not None:
            self.update_telemetry()

    def update_telemetry(self) -> None:
        """Rebuild the tree from the latest telemetry, preserving the cursor."""
        worker = self.worker_telemetry
        queues = self.queue_telemetry
        if worker is None or queues is None:
            return
        structure: frozenset[tuple[str, str]] = frozenset(
            (queue, host)
            for queue in queues.queues
            for host in worker.queues.get(queue, ())
        )
        if structure == self._last_structure:
            return
        self._last_structure = structure

        previous_data = self.cursor_node.data if self.cursor_node else None
        self.clear()
        for queue_name in sorted(queues.queues):
            draining = worker.queues.get(queue_name, ())
            queue_node = self.root.add(
                f"{queue_name} ({len(draining)})",
                WorkerTreeNode(
                    kind="queue",
                    label=queue_name,
                    queue_name=queue_name,
                ),
                expand=True,
            )
            for hostname in draining:
                if worker.nodes.get(hostname) is None:
                    continue
                queue_node.add_leaf(
                    hostname,
                    WorkerTreeNode(
                        kind="node",
                        label=hostname,
                        queue_name=queue_name,
                        hostname=hostname,
                    ),
                )
        self._restore_cursor(previous_data)

    def _restore_cursor(self, previous_data: WorkerTreeNode | None) -> None:
        """Move the cursor to the previously selected node, if still present."""
        if previous_data is None or not self.root.children:
            return
        node = self._find_node_by_data(self.root, previous_data)
        if node is not None:
            self.call_after_refresh(self.select_node, node)

    @staticmethod
    def _find_node_by_data(
        root: TreeNode[WorkerTreeNode], target: WorkerTreeNode
    ) -> TreeNode[WorkerTreeNode] | None:
        """Depth-first search for a tree node whose data matches *target*."""
        for child in root.children:
            if child.data == target:
                return child
            result = SelectionTree._find_node_by_data(child, target)
            if result is not None:
                return result
        return None


class WorkerGraphs(Static):
    """Throughput/CPU/memory graphs for the worker view."""

    GRAPH_HISTORY_SIZE = 60  # one minute of 1-second samples
    worker_telemetry: reactive[WorkerTelemetry | None] = reactive(None)
    selection: reactive[WorkerTreeNode | None] = reactive(None)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._throughput_history: deque[float] = deque(
            [0.0] * self.GRAPH_HISTORY_SIZE, maxlen=self.GRAPH_HISTORY_SIZE
        )
        self._cpu_history: deque[float] = deque(
            [0.0] * self.GRAPH_HISTORY_SIZE, maxlen=self.GRAPH_HISTORY_SIZE
        )
        self._memory_history: deque[float] = deque(
            [0.0] * self.GRAPH_HISTORY_SIZE, maxlen=self.GRAPH_HISTORY_SIZE
        )

    def compose(self) -> ComposeResult:
        yield from super().compose()
        yield Sparkline(
            id="worker-throughput-graph", data=list(self._throughput_history)
        )
        yield Sparkline(id="worker-cpu-graph", data=list(self._cpu_history))
        yield Sparkline(id="worker-memory-graph", data=list(self._memory_history))

    def watch_selection(self) -> None:
        self._reset_histories()
        self._refresh_graphs()

    def watch_worker_telemetry(self) -> None:
        self._refresh_graphs()

    def _reset_histories(self) -> None:
        """Clear all histories back to pre-filled zeros."""
        for history in (
            self._throughput_history,
            self._cpu_history,
            self._memory_history,
        ):
            history.clear()
            history.extend([0.0] * self.GRAPH_HISTORY_SIZE)

    @staticmethod
    def _memory_percent(node: NodeTelemetry) -> float:
        """Derive memory usage percentage from used/total physical RAM."""
        if node.memory_total <= 0:
            return 0.0
        return node.memory_bytes / node.memory_total * 100.0

    def _refresh_graphs(self) -> None:
        """Append the current sample to each graph and redraw."""
        telemetry = self.worker_telemetry
        selection = self.selection
        if telemetry is None or selection is None:
            return
        node = telemetry.nodes.get(selection.hostname)
        if node is None or selection.kind != "node":
            return
        self._throughput_history.append(node.tasks_per_minute)
        self._cpu_history.append(node.cpu_percent)
        self._memory_history.append(self._memory_percent(node))
        self._update_border_titles(node)
        try:
            throughput = self.query_one("#worker-throughput-graph", Sparkline)
            cpu = self.query_one("#worker-cpu-graph", Sparkline)
            mem = self.query_one("#worker-memory-graph", Sparkline)
        except Exception:  # noqa: BLE001
            logger.debug("Worker graph widgets not yet mounted")
            return
        throughput.data = list(self._throughput_history)
        cpu.data = list(self._cpu_history)
        mem.data = list(self._memory_history)

    def _update_border_titles(self, node: NodeTelemetry) -> None:
        """Show current values in the sparkline border titles."""
        try:
            throughput = self.query_one("#worker-throughput-graph", Sparkline)
            cpu = self.query_one("#worker-cpu-graph", Sparkline)
            mem = self.query_one("#worker-memory-graph", Sparkline)
        except Exception:  # noqa: BLE001
            return
        memory_percent = self._memory_percent(node)
        throughput.border_title = f"Throughput  {node.tasks_per_minute:.0f} tasks/min"
        cpu.border_title = f"CPU  {node.cpu_percent:.0f}%"
        mem.border_title = (
            f"RAM  {si_prefix(node.memory_bytes, base=1024)}B/{si_prefix(node.memory_total, base=1024)}B  "
            f"({memory_percent:.0f}%)  "
            f"procs {node.process_count}  threads {node.thread_count}"
        )
