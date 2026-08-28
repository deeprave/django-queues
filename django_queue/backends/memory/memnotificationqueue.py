from django_queue.clock import DEFAULT_CLOCK, QueueClock

from ..base import NotificationQueue
from .provider import QueueProviderMemory


class MemoryNotificationQueue(NotificationQueue):
    """Process-local owner-less notification delivery."""

    connection_scope = "process"
    worker_provider_kind = "memory"
    worker_provider_type = "memory"
    worker_class = "django_queue.backends.memory.MemoryNotificationQueueWorker"
    compatible_worker_class = (
        "django_queue.backends.memory.MemoryNotificationQueueWorker"
    )

    def __init__(self, _: str | None = None, options: dict | None = None, **kwargs):
        options = {} if options is None else options
        options |= kwargs
        self.entry_class = options.pop("entry_class", self.entry_class)
        maxsize = options.pop("maxsize", 0)
        self._queue_name = options.pop("queue_name", "default")
        self._clock: QueueClock = options.pop("clock", DEFAULT_CLOCK)
        self._provider = QueueProviderMemory(clock=self._clock, maxsize=maxsize)
