# Ensure all standard task handlers are registered upon package import
import taskforge.tasks.handlers  # noqa: F401
from taskforge.tasks.registry import get_handler, list_registered_handlers, register_handler

__all__ = ["register_handler", "get_handler", "list_registered_handlers"]
