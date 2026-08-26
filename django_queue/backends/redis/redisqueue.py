try:
    import functools
    import logging
    import uuid

    from django_queue.backends.base import AsyncQueue
    from django_queue.entries import QueueEntry

    from .provider import QueueProviderRedis

    logger = logging.getLogger(__name__)

    class RedisAsyncQueue(AsyncQueue):
        recovery_batch_size = 100
        requires_entry_class_at_construction = True
        worker_provider_kind = "redis"
        worker_provider_type = "redis"
        redis_topology = "standalone"
        provider_class = QueueProviderRedis
        worker_class = "django_queue.backends.redis.RedisAsyncQueueWorker"
        compatible_worker_class = "django_queue.backends.redis.RedisAsyncQueueWorker"

        def __init__(self, redis_url: str, options: dict | None = None, **kwargs):
            options = {} if options is None else options
            options |= kwargs
            self.entry_class = options.pop("entry_class", self.entry_class)
            self._provider = type(self).provider_class(
                redis_url, options, entry_class=self.entry_class
            )
            self._queue_name = self._provider.queue_name
            self._clock = self._provider.clock

        async def apublish(self, entry: QueueEntry) -> None:
            try:
                await self._provider.apublish(entry)
            except Exception:
                logger.exception(
                    "Unable to publish queue lifecycle snapshot for entry %s", entry.id
                )

        def _observer_receiver(self, on_snapshot):
            self._configure_provider_entry_class()
            return functools.partial(self._provider.aobserve, on_snapshot)

        async def aclose(self) -> None:
            await self._provider.aclose()

        async def _astore_and_push(
            self, entry: QueueEntry, *, available_at=None
        ) -> None:
            """Atomically store a freshly enqueued entry for immediate dispatch
            or scheduled availability.

            `RedisAsyncPriorityQueue` overrides this to select its priority-aware
            pending or scheduled store instead.
            """
            if available_at is None:
                await self._provider.astore_and_push(entry)
            else:
                await self._provider.astore_available(
                    entry, available_at, priority=False
                )

        async def _apromote_scheduled(self) -> None:
            await self._provider.apromote_scheduled()

        async def _apop(self) -> QueueEntry:
            return await self._provider.apop_scheduled()

        async def _astore_and_discard(self, entry: QueueEntry) -> None:
            await self._provider.astore_and_discard(entry)

        async def aclaim(
            self, worker_id: uuid.UUID, lease_seconds: float | None = None
        ) -> QueueEntry:
            """Claim the next entry for `RedisAsyncQueueWorker`'s delivery lease.

            `RedisAsyncPriorityQueue` overrides this (and `aclaim_unexpired`)
            to claim from its own priority-ordered pending store instead: the
            default FIFO Function only looks at the plain pending list, so it
            cannot see an entry a priority backend pushed via `apush_priority`.
            """
            return await self._provider.aclaim(worker_id, lease_seconds)

        async def aclaim_unexpired(
            self, worker_id: uuid.UUID, lease_seconds: float | None = None
        ) -> QueueEntry:
            return await self._provider.aclaim_unexpired(worker_id, lease_seconds)

        async def arecover(self, batch_size: int) -> tuple[int, int]:
            """Recover expired claims for `RedisAsyncQueueWorker`.

            `RedisAsyncPriorityQueue` overrides this to redeliver a
            recovered entry via its priority score instead of the plain
            pending list; the default Function always redelivers to the plain
            list.
            """
            return await self._provider.arecover(batch_size)

        async def arelease(
            self, entry_id: uuid.UUID, worker_id: uuid.UUID, delay_seconds: float
        ) -> bool:
            """Release a claim back for redelivery, e.g. after a lost race.

            `RedisAsyncPriorityQueue` overrides this to redeliver via the
            priority ZSET instead of the plain delayed set. The default
            Function parks the released entry on the plain delayed set, which
            priority claim considers before the priority ZSET; that could let
            a released low-priority entry jump ahead of higher-priority work.
            """
            return await self._provider.arelease(entry_id, worker_id, delay_seconds)

    class RedisAsyncStack(RedisAsyncQueue):
        def __init__(self, redis_url: str, options: dict | None = None, **kwargs):
            options = {} if options is None else options
            options |= kwargs
            options.setdefault("stack", True)
            super().__init__(redis_url, options)

except ImportError:
    pass
