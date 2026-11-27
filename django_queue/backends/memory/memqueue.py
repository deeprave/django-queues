from django_queue.clock import DEFAULT_CLOCK, QueueClock
from django_queue.entries import QueueEntry
from django_queue.observers import publish

from ..base import AsyncQueue
from .provider import QueueProviderMemory


class MemoryAsyncQueue(AsyncQueue):
    worker_class = "django_queue.backends.memory.MemoryAsyncQueueWorker"

    def __init__(self, _: str | None = None, options: dict | None = None, **kwargs):
        options = {} if options is None else options
        options |= kwargs
        self.entry_class = options.pop("entry_class", self.entry_class)
        maxsize = options.pop("maxsize", 0)
        self._stack = bool(options.pop("stack", False))
        self._queue_name = options.pop("queue_name", "default")
        clock: QueueClock = options.pop("clock", DEFAULT_CLOCK)
        self._clock = clock
        self._provider = QueueProviderMemory(
            clock=clock,
            stack=self._stack,
            maxsize=maxsize,
        )

    async def apublish(self, entry: QueueEntry) -> None:
        publish(self, entry)

    async def _apromote_scheduled(self) -> None:
        await self._provider.apromote_scheduled()


class MemoryAsyncStack(MemoryAsyncQueue):
    def __init__(self, _: str | None = None, options: dict | None = None, **kwargs):
        options = {} if options is None else options
        options |= kwargs
        options.setdefault("stack", True)
        super().__init__(_, options)
