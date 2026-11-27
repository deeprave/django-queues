from django_queue.clock import DEFAULT_CLOCK, QueueClock

from ..base import EventQueue
from .provider import QueueProviderMemory


class MemoryEventQueue(EventQueue):
    """Process-local transient event delivery with lease ownership."""

    connection_scope = "process"
    worker_provider_kind = "memory"
    worker_provider_type = "memory"
    worker_class = "django_queue.backends.memory.MemoryEventQueueWorker"
    compatible_worker_class = "django_queue.backends.memory.MemoryEventQueueWorker"

    def __init__(self, _: str | None = None, options: dict | None = None, **kwargs):
        super().__init__()
        options = {} if options is None else options
        options |= kwargs
        self.entry_class = options.pop("entry_class", self.entry_class)
        maxsize = options.pop("maxsize", 0)
        self._queue_name = options.pop("queue_name", "default")
        clock: QueueClock = options.pop("clock", DEFAULT_CLOCK)
        self._clock = clock
        self._provider = QueueProviderMemory(clock=clock, maxsize=maxsize)
