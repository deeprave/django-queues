"""Redis-aware default workers selected by Redis queue backends."""

import asyncio
import logging
from contextlib import suppress
from dataclasses import replace
from uuid import UUID

from django_queue.backends.exceptions import (
    QueueClaimConflictError,
    QueueEmptyException,
    QueueEntryExpiredError,
    QueueEntryMissingError,
    QueueEntryNotFoundError,
)
from django_queue.clock import MICROSECONDS_PER_SECOND
from django_queue.entries import QueueEntry, QueueEntryStatus
from django_queue.event_worker import EventQueueWorker
from django_queue.notification_worker import NotificationQueueWorker
from django_queue.worker import AsyncQueueWorker

logger = logging.getLogger(__name__)

_RECOVERY_INTERVAL_SECONDS = 1
_SHORT_LEASE_SECONDS = 60
_MEDIUM_LEASE_SECONDS = 600
_IMMEDIATE_RELEASE_DELAY_SECONDS = 1 / MICROSECONDS_PER_SECOND


class RedisAsyncQueueWorker(AsyncQueueWorker):
    """Default async-queue worker for queues composed with QueueProviderRedis."""

    provider_kind = "redis"
    provider_type = "redis"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._providers = {queue: queue._provider for queue in self._queues.values()}

    async def _recover_expired_claims(self, queue) -> None:
        """Return expired Redis claims before attempting another delivery."""
        now = asyncio.get_running_loop().time()
        if (
            now - self._last_recovery_at.get(queue, float("-inf"))
            >= _RECOVERY_INTERVAL_SECONDS
        ):
            self._last_recovery_at[queue] = now
            recovered, discarded = await queue.arecover(queue.recovery_batch_size)
            if recovered:
                logger.warning(
                    "Recovered %s expired queue claim%s",
                    recovered,
                    "s" if recovered != 1 else "",
                )
            if discarded:
                logger.error(
                    "Discarded %s unrecoverable expired queue claim%s",
                    discarded,
                    "s" if discarded != 1 else "",
                )

    async def _next(self, queue) -> tuple[QueueEntry, float | None] | None:
        """Claim the next Redis entry and establish its delivery lease."""
        await self._recover_expired_claims(queue)
        provider = self._providers[queue]
        entry = await queue.aclaim(self._worker_id, queue.default_claim_lease_seconds)
        lease_seconds = self.budget_for(queue, entry) + self._cancellation_grace_period
        if not await provider.arenew(entry.id, self._worker_id, lease_seconds):
            logger.warning("Lost claim for queue entry %s before dispatch", entry.id)
            return None
        self._last_claim_conflict_at.pop(entry.id, None)
        return entry, lease_seconds

    async def _discard_missing(self, queue, entry_id: UUID) -> None:
        """Discard a Redis claim whose durable entry is unexpectedly absent."""
        try:
            acknowledged = await self._providers[queue].aack(entry_id, self._worker_id)
        except Exception:
            logger.exception("Unable to discard missing queue entry %s", entry_id)
        else:
            if acknowledged:
                logger.error("Discarded missing queue entry %s", entry_id)
            else:
                logger.warning("Lost claim for missing queue entry %s", entry_id)

    @staticmethod
    def _renewal_delay(lease_seconds: float) -> float:
        if lease_seconds <= _SHORT_LEASE_SECONDS:
            return lease_seconds / 2
        if lease_seconds <= _MEDIUM_LEASE_SECONDS:
            return lease_seconds * 2 / 3
        return lease_seconds * 3 / 4

    async def _renew_claim(
        self, queue, entry: QueueEntry, lease_seconds: float
    ) -> bool:
        try:
            while True:
                await asyncio.sleep(self._renewal_delay(lease_seconds))
                if not await self._providers[queue].arenew(
                    entry.id, self._worker_id, lease_seconds
                ):
                    logger.warning(
                        "Lost claim for queue entry %s during renewal", entry.id
                    )
                    return False
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unable to renew claim for queue entry %s", entry.id)
            return False

    async def _mark_running(self, queue, entry: QueueEntry) -> QueueEntry | None:
        """Persist running status only while this Redis worker owns the claim."""
        queued_entry = await queue.afind(entry.id)
        running_entry = replace(
            queued_entry,
            status=QueueEntryStatus.RUNNING,
            dispatched_at=await queue.clock.anow(),
        )
        provider = self._providers[queue]
        if not await provider.amark_running(self._worker_id, running_entry):
            if not await queue.arelease(
                entry.id, self._worker_id, _IMMEDIATE_RELEASE_DELAY_SECONDS
            ):
                logger.warning("Lost claim for queue entry %s before release", entry.id)
            return None
        return running_entry

    async def _settle(self, queue, entry: QueueEntry) -> bool:
        """Atomically store a terminal record and release its Redis claim."""
        return await self._providers[queue].asettle(self._worker_id, entry)


class RedisEventQueueWorker(EventQueueWorker):
    """Default event worker for queues composed with QueueProviderRedis."""

    provider_kind = "redis"
    provider_type = "redis"

    def __init__(self, queue, **kwargs) -> None:
        super().__init__(queue, **kwargs)
        self._provider = queue._provider
        self._last_recovery_at = float("-inf")

    async def _next(self) -> tuple[QueueEntry, float | None] | None:
        expired_entry_ids = await self._provider.aexpire_due()
        for entry_id in expired_entry_ids:
            logger.warning(
                "Discarded expired event",
                extra={"queue": self._queue.queue_name, "entry_id": str(entry_id)},
            )
        await self._recover_expired_claims()
        lease_seconds = self._queue.default_claim_lease_seconds
        try:
            entry = await self._provider.aclaim_unexpired(
                self._worker_id, lease_seconds
            )
        except QueueEmptyException:
            return None
        except QueueClaimConflictError as exc:
            logger.debug(
                "Event entry is already claimed by another worker",
                extra={"queue": self._queue.queue_name, "entry_id": str(exc.entry_id)},
            )
            return None
        except QueueEntryExpiredError as exc:
            logger.debug(
                "Skipping event entry that expired before dispatch",
                extra={"queue": self._queue.queue_name, "entry_id": str(exc.entry_id)},
            )
            return None
        except QueueEntryMissingError as exc:
            logger.warning(
                "Discarding claim for missing event entry",
                extra={"queue": self._queue.queue_name, "entry_id": str(exc.entry_id)},
            )
            await self._provider.aremove(exc.entry_id, self._worker_id)
            return None
        except QueueEntryNotFoundError as exc:
            logger.warning(
                "Skipping event entry that disappeared before dispatch",
                extra={"queue": self._queue.queue_name, "entry_id": str(exc.entry_id)},
            )
            return None
        if not await self._provider.arenew(entry.id, self._worker_id, lease_seconds):
            logger.warning("Lost claim for event entry %s before dispatch", entry.id)
            return None
        return entry, lease_seconds

    async def _recover_expired_claims(self) -> None:
        now = asyncio.get_running_loop().time()
        if now - self._last_recovery_at < self.recovery_interval:
            return
        self._last_recovery_at = now
        recovered, discarded = await self._provider.arecover(
            getattr(self._queue, "recovery_batch_size", 100)
        )
        if recovered:
            logger.warning(
                "Recovered %s expired event claim%s",
                recovered,
                "s" if recovered != 1 else "",
            )
        if discarded:
            logger.error(
                "Discarded %s unrecoverable expired event claim%s",
                discarded,
                "s" if discarded != 1 else "",
            )

    async def _renew_claim(self, entry: QueueEntry, lease_seconds: float) -> bool:
        try:
            while True:
                await asyncio.sleep(lease_seconds * 2 / 3)
                if not await self._provider.arenew(
                    entry.id, self._worker_id, lease_seconds
                ):
                    logger.warning(
                        "Lost claim for event entry %s during renewal", entry.id
                    )
                    return False
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unable to renew claim for event entry %s", entry.id)
            return False

    async def _release(self, entry: QueueEntry) -> None:
        if not await self._provider.arelease(
            entry.id, self._worker_id, self.release_delay
        ):
            logger.warning("Lost claim for event entry %s before release", entry.id)

    async def _remove(self, entry: QueueEntry) -> None:
        if not await self._provider.aremove(entry.id, self._worker_id):
            logger.warning("Lost claim for event entry %s before removal", entry.id)


class RedisNotificationQueueWorker(NotificationQueueWorker):
    """Default notification worker for queues composed with QueueProviderRedis."""

    provider_kind = "redis"
    provider_type = "redis"

    def __init__(self, queue, **kwargs) -> None:
        super().__init__(queue, **kwargs)
        self._provider = queue._provider
        self._pubsub = None
        self._pubsub_client = None

    async def run(self) -> None:
        self._running = True
        try:
            await self._ensure_subscribed()
            while True:
                await self.adispatch_once()
        finally:
            self._running = False
            await self._aclose_pubsub()

    async def _next(self) -> QueueEntry | None:
        await self._ensure_subscribed()
        pubsub = self._pubsub
        if pubsub is None:
            return None
        message = await pubsub.get_message(
            ignore_subscribe_messages=True, timeout=self._idle_delay
        )
        if not message or message.get("type") != "message":
            return None
        try:
            return self._provider.decode_notification(message["data"])
        except Exception:
            logger.exception(
                "Ignoring invalid notification payload",
                extra={"queue": self._queue.queue_name},
            )
            return None

    async def _expire_due(self) -> None:
        expired_entry_ids = await self._provider.aexpire_due_notifications()
        for entry_id in expired_entry_ids:
            logger.warning(
                "Discarded expired notification",
                extra={"queue": self._queue.queue_name, "entry_id": str(entry_id)},
            )

    async def _ensure_subscribed(self) -> None:
        if self._pubsub is not None:
            return
        client = await self._provider._prepare_async_client(
            self._provider._create_async_client()
        )
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        try:
            await pubsub.subscribe(self._provider.notification_channel)
        except BaseException:
            try:
                with suppress(BaseException):
                    await pubsub.aclose()
            finally:
                with suppress(BaseException):
                    await self._provider._aclose_client(client)
            raise
        self._pubsub_client = client
        self._pubsub = pubsub

    async def _aclose_pubsub(self) -> None:
        pubsub = self._pubsub
        client = self._pubsub_client
        self._pubsub = None
        self._pubsub_client = None
        try:
            if pubsub is not None:
                await pubsub.aclose()
        finally:
            if client is not None:
                await self._provider._aclose_client(client)
