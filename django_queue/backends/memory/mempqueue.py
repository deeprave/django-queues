from django_queue.clock import DEFAULT_CLOCK

from ..base import AsyncQueue
from .memqueue import MemoryAsyncQueue
from .provider import QueueProviderMemory


class MemoryAsyncPriorityQueue(AsyncQueue):
    worker_class = "django_queue.backends.memory.MemoryAsyncQueueWorker"

    def __init__(self, _: str | None = None, options: dict | None = None, **kwargs):
        if (options is not None and "stack" in options) or "stack" in kwargs:
            raise ValueError("Priority queues do not accept the stack option")
        options = {} if options is None else options
        options |= kwargs
        self.entry_class = options.pop("entry_class", self.entry_class)
        maxsize = options.pop("maxsize", 0)
        self._queue_name = options.pop("queue_name", "default")
        self._clock = options.pop("clock", DEFAULT_CLOCK)
        self._provider = QueueProviderMemory(
            clock=self._clock,
            maxsize=maxsize,
        )

    async def aadd(self, *items):
        await self._provider.aadd_priority(*items)

    async def aget(self):
        return await self._provider.aget_priority()

    async def apoll(self):
        return await self._provider.apoll_priority()

    async def apeek(self):
        return await self._provider.apeek_priority()

    async def asize(self):
        return await self._provider.asize_priority()

    async def aclear(self):
        await self._provider.aclear_priority()

    async def _apush(self, entry) -> None:
        await self._provider.apush_priority(entry.id, entry.priority)

    async def _apop(self):
        await self._apromote_scheduled()
        return await self._provider.apop_priority()

    async def _apromote_scheduled(self) -> None:
        await self._provider.apromote_scheduled_priority()

    async def _adiscard(self, entry_id) -> None:
        await self._provider.adiscard_priority(entry_id)
        await self._provider.adiscard_scheduled(entry_id)

    aenqueue = MemoryAsyncQueue.aenqueue
    afind = MemoryAsyncQueue.afind
    aprune = MemoryAsyncQueue.aprune
    _aprune_expired = MemoryAsyncQueue._aprune_expired
    apublish = MemoryAsyncQueue.apublish
    adequeue = MemoryAsyncQueue.adequeue
    ahas_pending = MemoryAsyncQueue.ahas_pending
    _amark_running = MemoryAsyncQueue._amark_running
    _amark_succeeded = MemoryAsyncQueue._amark_succeeded
    _amark_failed = MemoryAsyncQueue._amark_failed
    _amark_cancelled = MemoryAsyncQueue._amark_cancelled
    _amark_timed_out = MemoryAsyncQueue._amark_timed_out
    _areplace_entry = MemoryAsyncQueue._areplace_entry
