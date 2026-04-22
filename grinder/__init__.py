"""A queue agnostic worker for Django's task framework."""

from . import _version
from .executor import TaskExecutor

__version__ = _version.version
VERSION = _version.version_tuple

Executor = TaskExecutor

__all__ = ["Executor", "TaskExecutor", "VERSION", "__version__"]

