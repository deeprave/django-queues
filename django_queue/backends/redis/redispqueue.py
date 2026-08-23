try:
    from .redisqueue import RedisAsyncQueue

    class RedisAsyncPriorityQueue(RedisAsyncQueue):
        def __init__(self, redis_url: str, options: dict | None = None, **kwargs):
            super().__init__(redis_url, options, **kwargs)

        @property
        def stack(self):
            return False

        def poll(self, timeout: int = 0, retries: int = 10):
            return self._run_synchronously(self.apoll, timeout, retries)

        async def aadd(self, *items):
            await self._provider.aadd_priority(*items)

        async def aget(self):
            return await self._provider.aget_priority()

        async def apoll(self, timeout: int = 0, retries: int = 10):
            """Remove and return the highest-priority item.

            A positive ``timeout`` applies to each blocking attempt. The call
            can therefore wait up to ``timeout * retries`` before raising
            ``QueueEmptyException``; with a positive timeout, zero retries
            means keep trying.
            """
            return await self._provider.apoll_priority(timeout, retries)

        async def apeek(self):
            """
            Retrieve (but don't remove) the highest-priority item from the priority queue.
            :return: The item with the highest priority.
            Raises QueueEmptyException if the queue is empty.
            """
            return await self._provider.apeek_priority()

        async def asize(self) -> int:
            """
            Get the current size of the priority queue.
            :return: Number of items in the queue.
            """
            return await self._provider.asize_priority()

        async def aclear(self) -> None:
            """
            Clear all items in the queue.
            """
            await self._provider.aclear_priority()

        async def _apush(self, entry) -> None:
            await self._provider.apush_priority(entry.id, entry.priority)

        async def _astore_and_push(self, entry, *, available_at=None) -> None:
            if available_at is None:
                await self._provider.astore_and_push_priority(entry)
            else:
                await self._provider.astore_available(
                    entry, available_at, priority=True
                )

        async def _apromote_scheduled(self) -> None:
            await self._provider.apromote_scheduled_priority()

        async def _apop(self):
            return await self._provider.apop_scheduled_priority()

        async def _adiscard(self, entry_id) -> None:
            await self._provider.adiscard_priority(entry_id)
            await self._provider.adiscard_scheduled(entry_id)

        async def aclaim(self, worker_id, lease_seconds=None):
            return await self._provider.aclaim_priority(worker_id, lease_seconds)

        async def aclaim_unexpired(self, worker_id, lease_seconds=None):
            return await self._provider.aclaim_priority_unexpired(
                worker_id, lease_seconds
            )

        async def arecover(self, batch_size: int) -> tuple[int, int]:
            return await self._provider.arecover_priority(batch_size)

        async def arelease(self, entry_id, worker_id, delay_seconds):
            return await self._provider.arelease_priority(
                entry_id, worker_id, delay_seconds
            )

except ImportError:
    pass
