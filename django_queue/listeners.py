"""Process-local registration for transient event and notification listeners."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from django_queue.backends.base import EventQueue, NotificationQueue
from django_queue.entries import QueueEntry

EventListener = Callable[[QueueEntry], Any]
EventFilter = Callable[[QueueEntry], bool]


@dataclass(frozen=True, slots=True)
class ListenerRegistration:
    """One local event listener and its optional eligibility filter."""

    callback: EventListener
    filter: EventFilter | None = None


_listeners: dict[str, list[ListenerRegistration]] = {}


def queue_listener(
    queue_name: str, *, filter: EventFilter | None = None
) -> Callable[[EventListener], EventListener]:
    """Register a local listener for one configured EventQueue or NotificationQueue."""
    if not isinstance(queue_name, str) or not queue_name:
        raise ValueError("queue_name must be a non-empty string")
    if filter is not None and not callable(filter):
        raise TypeError("filter must be callable or None")

    def register(callback: EventListener) -> EventListener:
        if not callable(callback):
            raise TypeError("queue listeners must be callable")
        from django_queue import queues

        if not isinstance(queues[queue_name], EventQueue | NotificationQueue):
            raise TypeError(
                "queue listeners require an EventQueue or NotificationQueue"
            )
        _listeners.setdefault(queue_name, []).append(
            ListenerRegistration(callback, filter)
        )
        return callback

    return register


def listeners_for(queue_name: str) -> tuple[ListenerRegistration, ...]:
    """Return the current process's listeners in registration order."""
    return tuple(_listeners.get(queue_name, ()))


def reset_listeners() -> None:
    """Clear registrations; used by Django's app reload and test isolation."""
    _listeners.clear()
