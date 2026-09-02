from django.db import close_old_connections
from django.dispatch import receiver
from django.tasks.signals import task_finished, task_started

from .executor import TaskExecutor


@receiver([task_started, task_finished], sender=TaskExecutor)
def close_task_database_connection(sender, task_result, **kwargs):
    close_old_connections()
