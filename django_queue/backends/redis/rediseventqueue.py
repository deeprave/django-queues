"""Redis-backed transient event queue."""

from __future__ import annotations

from django_queue.entries import QueueEntry

from ..base import EventQueue
from .provider import QueueProviderRedis


class RedisEventQueue(EventQueue):
    """Transient events stored, claimed, and removed through Redis.

    Redis persistence is wholly delegated to :class:`QueueProviderRedis`; this
    class only applies the event lifetime and removal contract.
    """

    recovery_batch_size = 100
    requires_entry_class_at_construction = True
    worker_provider_kind = "redis"
    worker_provider_type = "redis"
    redis_topology = "standalone"
    provider_class = QueueProviderRedis
    worker_class = "django_queue.backends.redis.RedisEventQueueWorker"
    compatible_worker_class = "django_queue.backends.redis.RedisEventQueueWorker"

    def __init__(self, redis_url: str, options: dict | None = None, **kwargs) -> None:
        super().__init__()
        options = {} if options is None else options
        options |= kwargs
        self.entry_class = options.pop("entry_class", self.entry_class)
        self._provider = type(self).provider_class(
            redis_url, options, entry_class=self.entry_class
        )
        self._queue_name = self._provider.queue_name
        self._clock = self._provider.clock

    async def _astore_and_push(self, entry: QueueEntry) -> None:
        """Atomically store a freshly enqueued event and add it to the
        pending list -- see `QueueProviderRedis.astore_event_and_push`."""
        await self._provider.astore_event_and_push(entry)
