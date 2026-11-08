from collections.abc import Callable
from typing import Any

from taskforge.core.logging import get_logger

logger = get_logger(__name__)

TaskHandler = Callable[[dict[str, Any]], dict[str, Any]]
_HANDLERS: dict[str, TaskHandler] = {}


def register_handler(name: str):
    """Decorator to register a task handler function."""

    def decorator(fn: TaskHandler):
        _HANDLERS[name] = fn
        return fn

    return decorator


def get_handler(name: str) -> TaskHandler:
    if name not in _HANDLERS:
        raise KeyError(
            f"No task handler registered for type '{name}'. Registered: {list(_HANDLERS.keys())}"
        )
    return _HANDLERS[name]


def list_registered_handlers() -> list[str]:
    return list(_HANDLERS.keys())
