"""Modal confirmation screens for inspector queue actions."""

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.events import Key
from textual.screen import ModalScreen
from textual.widgets import Input, Label


class ConfirmScreen(ModalScreen[bool]):
    """Yes/no confirmation: Enter confirms, Esc cancels."""

    def __init__(self, prompt: str, *, danger: bool = False) -> None:
        super().__init__()
        self._prompt = prompt
        self._danger = danger

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog", classes="danger" if self._danger else "warning"):
            yield Label(self._prompt, id="prompt")
            yield Label("Press Enter to confirm, Esc to cancel.", id="instruction")

    def on_key(self, event: Key) -> None:
        match event.key:
            case "enter":
                self.dismiss(True)
            case "escape":
                self.dismiss(False)


class PurgeScreen(ModalScreen[bool]):
    """Type the queue name to confirm a purge."""

    def __init__(self, queue_name: str) -> None:
        super().__init__()
        self._queue_name = queue_name

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog", classes="danger"):
            yield Label(f"Purge queue [bold]{self._queue_name}[/bold]?", id="prompt")
            yield Label(
                "Type the queue name to confirm, Esc to cancel.", id="instruction"
            )
            yield Input(placeholder=self._queue_name, id="confirm-input")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.value == self._queue_name:
            self.dismiss(True)

    def on_key(self, event: Key) -> None:
        if event.key == "escape":
            self.dismiss(False)
