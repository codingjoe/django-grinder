"""Textual app for the inspector TUI."""

from __future__ import annotations

import datetime
import logging

from django.tasks import task_backends
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import (
    Footer,
    ListItem,
    ListView,
    Select,
    Static,
    Tree,
)

from ..backends.base import (
    BackendTelemetry,
    NodeTelemetry,
    ThreadmillTaskBackend,
    WorkerTelemetry,
)
from .queue_view import (
    TAB_KEYS,
    QueueItem,
    QueueList,
    TaskDetail,
    TaskList,
)
from .utils import (
    si_prefix,
    supported_aliases as _supported_aliases,
)
from .worker_view import (
    SelectionTree,
    WorkerGraphs,
    WorkerTreeNode,
)

logger = logging.getLogger(__name__)

TELEMETRY_INTERVAL_SECONDS = 2.0
"""Seconds between automatic queue-stat refreshes; the task list stays manual."""

__all__ = [
    "InspectorApp",
    "QueueItem",
    "QueueList",
    "SelectionTree",
    "TaskDetail",
    "TaskList",
    "WorkerGraphs",
    "WorkerTreeNode",
    "si_prefix",
]


class InspectorApp(App):
    """Threadmill TUI inspector with backend/queue/task panes."""

    CSS_PATH = "inspector.scss"
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("f5", "refresh", "Refresh"),
        Binding("v", "toggle_view", "Toggle Worker View"),
        *(
            Binding(key, f"switch_tab('tab-{tab_id}')", tab_id.capitalize())
            for tab_id, key in TAB_KEYS.items()
        ),
    ]

    backend: reactive[ThreadmillTaskBackend] = reactive(None)
    telemetry: reactive[BackendTelemetry] = reactive(None, always_update=True)
    worker_telemetry: reactive[WorkerTelemetry | None] = reactive(
        None, always_update=True
    )
    worker_view_enabled: reactive[bool] = reactive(False)

    def __init__(
        self,
        backend: ThreadmillTaskBackend,
        *,
        auto_refresh: bool = True,
    ) -> None:
        super().__init__()
        self._queue_list: QueueList | None = None
        self._task_list: TaskList | None = None
        self._task_detail: TaskDetail | None = None
        self._options_static: Static | None = None
        self._selection_tree: SelectionTree | None = None
        self._worker_graphs: WorkerGraphs | None = None
        self._telemetry_timer = None
        self._auto_refresh = auto_refresh
        self._node_cache: dict[str, NodeTelemetry] = {}
        self.set_reactive(InspectorApp.backend, backend)

    def compose(self) -> ComposeResult:
        self.title = "Threadmill"
        self.sub_title = "Inspector"
        with Vertical(id="split-view"):
            with Horizontal(id="backend-row"):
                yield Select(
                    list(_supported_aliases()),
                    id="backend-select",
                    value=self.backend.alias,
                    allow_blank=False,
                )
                yield Static(id="backend-options")
            with Horizontal(id="queue-view"):
                with Vertical(classes="left-pane"):
                    yield QueueList(id="queue-list").data_bind(
                        telemetry=InspectorApp.telemetry
                    )
                with Vertical(classes="right-pane"):
                    yield TaskList(id="task-list").data_bind(
                        backend=InspectorApp.backend,
                        telemetry=InspectorApp.telemetry,
                    )
                    yield TaskDetail(id="task-detail", name="Task Detail")
            with Horizontal(id="worker-view"):
                with Vertical(classes="left-pane"):
                    yield SelectionTree(
                        "Worker Telemetry", id="selection-tree"
                    ).data_bind(
                        worker_telemetry=InspectorApp.worker_telemetry,
                        queue_telemetry=InspectorApp.telemetry,
                    )
                with Vertical(classes="right-pane"):
                    yield WorkerGraphs(id="worker-graphs").data_bind(
                        worker_telemetry=InspectorApp.worker_telemetry,
                    )
        yield Footer(show_command_palette=True)

    def on_mount(self) -> None:
        """Register the theme, render the initial snapshot, and arm auto-refresh."""
        self.theme = "monokai"
        self._queue_list = self.query_one("#queue-list", QueueList)
        self._task_list = self.query_one("#task-list", TaskList)
        self._task_detail = self.query_one("#task-detail", TaskDetail)
        self._options_static = self.query_one("#backend-options", Static)
        self._selection_tree = self.query_one("#selection-tree", SelectionTree)
        self._worker_graphs = self.query_one("#worker-graphs", WorkerGraphs)
        self._refresh_options()
        self._refresh_telemetry()
        self.worker_telemetry = WorkerTelemetry(
            nodes={},
            queues={},
            sampled_at=datetime.datetime.now(tz=datetime.UTC),
        )
        if self._auto_refresh:
            self._telemetry_timer = self.set_interval(
                TELEMETRY_INTERVAL_SECONDS,
                self._refresh_telemetry,
                name="telemetry-refresh",
            )
            self.run_worker(self._subscribe_worker_telemetry, group="telemetry")
        self._apply_worker_view_visibility()
        self._queue_list.focus()

    def action_quit(self) -> None:
        """Exit the TUI."""
        self.exit()

    def action_refresh(self) -> None:
        """Refresh queue stats and re-fetch the task list on demand."""
        self._refresh_telemetry()
        self._task_list.refresh_tasks()

    def action_switch_tab(self, tab_id: str) -> None:
        """Activate the task status tab matching the given id."""
        self._task_list.switch_tab(tab_id)

    def action_toggle_view(self) -> None:
        """Switch between queue view and worker view."""
        self.worker_view_enabled = not self.worker_view_enabled

    def watch_worker_view_enabled(self, enabled: bool) -> None:
        """Show/hide widgets and update the toggle binding label."""
        self._apply_worker_view_visibility()
        self._bindings.key_to_bindings["v"] = [
            Binding(
                "v",
                "toggle_view",
                "Toggle Queue View" if enabled else "Toggle Worker View",
            )
        ]
        self.refresh_bindings()

    def _apply_worker_view_visibility(self) -> None:
        """Toggle between queue view and worker view."""
        self.query_one("#queue-view").display = not self.worker_view_enabled
        self.query_one("#worker-view").display = self.worker_view_enabled
        if self.worker_view_enabled:
            self._selection_tree.focus()
        else:
            self._queue_list.focus()

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Update worker graphs when a tree node is selected."""
        if event.node.data is not None and isinstance(event.node.data, WorkerTreeNode):
            self._worker_graphs.selection = event.node.data

    def watch_backend(self) -> None:
        self._refresh_options()
        self._refresh_telemetry()

    def on_select_changed(self, event: Select.Changed) -> None:
        """Switch backend when the user selects a different alias."""
        self.backend = task_backends[event.value]

    def _queue_from_item(self, item: ListItem | None) -> str | None:
        """Extract queue name from a queue list item id."""
        return item.id.removeprefix("queue-") if item is not None else None

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Update the task list when a queue is selected."""
        if queue_name := self._queue_from_item(event.item):
            self._task_list.queue_name = queue_name

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        """Preview the selected queue without requiring Enter."""
        if queue_name := self._queue_from_item(event.item):
            self._task_list.queue_name = queue_name

    def _refresh_options(self) -> None:
        """Show the selected backend's constructor options."""
        parts = [
            f"{key}={value!r}" for key, value in sorted(self.backend.options.items())
        ]
        self._options_static.update(" ".join(parts) or "No options")

    def _refresh_telemetry(self) -> None:
        """Poll backend for queue telemetry and rebuild the worker view."""
        try:
            self.telemetry = self.backend.telemetry()
        except Exception:  # noqa: BLE001
            logger.exception("Failed to refresh telemetry")
        if self._node_cache:
            self.worker_telemetry = self._build_worker_telemetry()

    def _build_worker_telemetry(self) -> WorkerTelemetry:
        """Build a snapshot from the latest per-host node cache."""
        queues: dict[str, set[str]] = {}
        for node in self._node_cache.values():
            for queue_name in node.queues:
                queues.setdefault(queue_name, set()).add(node.hostname)
        return WorkerTelemetry(
            nodes=dict(self._node_cache),
            queues={name: tuple(sorted(hosts)) for name, hosts in queues.items()},
            sampled_at=datetime.datetime.now(tz=datetime.UTC),
        )

    async def _subscribe_worker_telemetry(self) -> None:
        """Maintain a pub/sub subscription; cache the latest snapshot per host.

        Each message updates the per-host cache silently; the reactive
        `worker_telemetry` is only rebuilt on the 2-second timer tick
        (`_refresh_telemetry`) to avoid flooding the UI with redraws.
        """
        from ..backends.redis import WORKER_TELEMETRY_TTL as _TTL

        try:
            async for snapshot in self.backend.subscribe_worker_telemetry():
                for hostname, node in snapshot.nodes.items():
                    self._node_cache[hostname] = node
                self._prune_nodes(_TTL)
        except Exception:  # noqa: BLE001
            logger.exception("Worker telemetry subscription failed")

    def _prune_nodes(self, ttl_seconds: int) -> None:
        """Drop cached nodes whose latest sample is older than the TTL."""
        cutoff = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(
            seconds=ttl_seconds
        )
        self._node_cache = {
            hostname: node
            for hostname, node in self._node_cache.items()
            if node.sampled_at > cutoff
        }
