from django.apps import AppConfig


class ThreadmillConfig(AppConfig):
    name = "threadmill"

    def ready(self) -> None:
        from . import signals  # noqa: F401
