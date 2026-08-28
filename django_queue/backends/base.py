from __future__ import annotations

import builtins
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID, uuid7

from asgiref.sync import async_to_sync
from django.utils.module_loading import import_string

from django_queue.backends.exceptions import (
    InvalidQueueBackendError,
    QueueEntryNotFoundError,
)
from django_queue.clock import DEFAULT_CLOCK, ClockTime, QueueClock
from django_queue.entries import (
    QueueEntry,
    QueueEntryStatus,
    validate_budget,
    validate_json_value,
)
from django_queue.signals import send_entry_enqueued

logger = logging.getLogger(__name__)

_WorkerT = TypeVar("_WorkerT")

if TYPE_CHECKING:
    from django_queue.event_worker import EventQueueWorker
    from django_queue.notification_worker import NotificationQueueWorker
    from django_queue.worker import AsyncQueueWorker, Handler


class BaseQueue(ABC):
    default_claim_lease_seconds = 600
    entry_class: type[QueueEntry] = QueueEntry
    worker_class: type[AsyncQueueWorker] | str = "django_queue.worker.AsyncQueueWorker"
    compatible_worker_class: type[AsyncQueueWorker] | str = (
        "django_queue.worker.AsyncQueueWorker"
    )
    worker_provider_kind = "generic"
    worker_provider_type = "generic"
    # Set by the configured queue registry from the alias's TIMEOUT setting,
    # as entry_class and worker_class are. An entry's own budget takes
    # precedence over it, and a worker override over both.
    timeout_seconds: float | None = None
    _queue_name: str = ""
    _clock: QueueClock | None = None
    _provider: Any

    @property
    def queue_name(self) -> str:
        """Return the stable entry namespace this queue writes under.

        Empty when a backend never set one; entry creation rejects that, so an
        entry-capable backend must supply a name.
        """
        return self._queue_name

    @property
    def clock(self) -> QueueClock:
        """Return the clock this queue timestamps its entries with.

        Local time when a backend never set one, so a component recording times
        alongside this queue's entries can always ask rather than assume. Read
        through here rather than the attribute, so the fallback applies and the
        result is never optional.
        """
        return self._clock or DEFAULT_CLOCK

    def _worker_class_is_compatible(self, worker_class: type) -> bool:
        compatible_worker_class = self.compatible_worker_class
        if isinstance(compatible_worker_class, str):
            compatible_worker_class = import_string(compatible_worker_class)
        return (
            issubclass(worker_class, compatible_worker_class)
            and worker_class.provider_type == self.worker_provider_type
        )

    def _resolve_worker(
        self, alias: str, required_base: type[_WorkerT]
    ) -> type[_WorkerT]:
        """Import and validate this queue's configured worker class."""
        worker_class = self.worker_class
        if isinstance(worker_class, str):
            if not worker_class:
                raise InvalidQueueBackendError(
                    f"Queue alias '{alias}' WORKER must be a non-empty dotted path"
                )
            try:
                worker_class = import_string(worker_class)
            except ImportError as exc:
                raise InvalidQueueBackendError(
                    f"Queue alias '{alias}' WORKER could not be imported: {exc}"
                ) from exc
        if not isinstance(worker_class, type) or not issubclass(
            worker_class, required_base
        ):
            raise InvalidQueueBackendError(
                f"Queue alias '{alias}' WORKER must be a {required_base.__name__} subclass"
            )
        if worker_class.provider_kind != self.worker_provider_kind:
            raise InvalidQueueBackendError(
                f"Queue alias '{alias}' requires a {self.worker_provider_kind} worker"
            )
        if not self._worker_class_is_compatible(worker_class):
            raise InvalidQueueBackendError(
                f"Queue alias '{alias}' WORKER is not compatible with "
                f"{type(self).__name__}"
            )
        return worker_class

    @property
    def stack(self):
        return self._provider.stack

    @property
    def capacity(self):
        return self._provider.capacity

    def add(self, *items):
        return self._run_synchronously(self.aadd, *items)

    async def aadd(self, *items) -> None:
        await self._provider.aadd(*items)

    def get(self):
        return self._run_synchronously(self.aget)

    async def aget(self):
        return await self._provider.aget()

    def poll(self):
        return self._run_synchronously(self.apoll)

    async def apoll(self):
        return await self._provider.apoll()

    def peek(self):
        return self._run_synchronously(self.apeek)

    async def apeek(self):
        return await self._provider.apeek()

    def size(self):
        return self._run_synchronously(self.asize)

    async def asize(self):
        return await self._provider.asize()

    def is_empty(self):
        return self.size() == 0

    async def ais_empty(self) -> bool:
        return await self.asize() == 0

    def clear(self):
        return self._run_synchronously(self.aclear)

    async def aclear(self) -> None:
        await self._provider.aclear()

    def close(self):
        return async_to_sync(self.aclose)()

    async def aclose(self) -> None:
        """Release resources owned by the running event loop."""
        await self._provider.aclose()

    def _run_synchronously(
        self, operation: Callable[..., Awaitable[Any]], *args, **kwargs
    ):
        """Run one async API call and release any bridge-loop resources."""
        return async_to_sync(self._run_and_close)(operation, *args, **kwargs)

    async def _run_and_close(
        self, operation: Callable[..., Awaitable[Any]], *args, **kwargs
    ) -> Any:
        try:
            return await operation(*args, **kwargs)
        finally:
            await self.aclose()

    def enqueue(
        self,
        payload,
        *,
        timeout_seconds: float | None = None,
        priority: int = 0,
        available_at: ClockTime | None = None,
    ) -> UUID:
        return self._run_synchronously(
            self.aenqueue,
            payload,
            timeout_seconds=timeout_seconds,
            priority=priority,
            available_at=available_at,
        )

    @abstractmethod
    async def aenqueue(
        self,
        payload,
        *,
        timeout_seconds: float | None = None,
        priority: int = 0,
        available_at: ClockTime | None = None,
    ) -> UUID:
        """Store a JSON-serialisable payload and return its queue-owned ID.

        An execution budget given here is carried on the entry and persisted
        with it, so it survives enqueue and reaches whichever worker dispatches
        the entry. `available_at` delays eligibility where scheduling is
        supported. `priority` is only consulted by priority-variant `AsyncQueue`
        backends; ignored elsewhere (e.g. by `EventQueue`, `NotificationQueue`,
        and non-priority `AsyncQueue` backends, whose dispatch order -- FIFO,
        or LIFO for a stack -- is unaffected by it).
        """
        raise NotImplementedError("aenqueue")

    def find(self, entry_id: UUID) -> QueueEntry:
        return self._run_synchronously(self.afind, entry_id)

    @abstractmethod
    async def afind(self, entry_id: UUID) -> QueueEntry:
        """Return the retained entry record for *entry_id*."""
        raise NotImplementedError("afind")

    async def apublish(self, entry: QueueEntry) -> None:
        """Best-effort publish one worker-observed lifecycle snapshot."""

    def dequeue(self) -> QueueEntry:
        return self._run_synchronously(self.adequeue)

    @abstractmethod
    async def adequeue(self) -> QueueEntry:
        """Remove and return the next pending entry (best effort)."""
        raise NotImplementedError("adequeue")

    def has_pending(self) -> bool:
        return self._run_synchronously(self.ahas_pending)

    @abstractmethod
    async def ahas_pending(self) -> bool:
        """Return whether an entry worker can dequeue pending work."""
        raise NotImplementedError("ahas_pending")

    def __len__(self):
        return self.size()

    def __bool__(self):
        return not self.is_empty()


class AsyncQueue(BaseQueue):
    """A queue whose worker persists asynchronous lifecycle outcomes."""

    retention_timeout: float | None = 600

    def resolve_worker(self, alias: str) -> type[AsyncQueueWorker]:
        """Import and validate this queue's configured worker class."""
        # Imported here so the storage layer does not depend on the worker layer.
        from django_queue.worker import AsyncQueueWorker

        return self._resolve_worker(alias, AsyncQueueWorker)

    def create_worker(self, alias: str, handler: Handler) -> AsyncQueueWorker:
        """Create this queue's configured worker when it becomes active.

        The worker is given this queue's clock, so its recorded time and the
        entries it dispatches share one basis. A configured WORKER subclass
        overriding `__init__` must therefore accept a `clock` keyword.
        """
        return self.resolve_worker(alias)(
            {alias: self}, {alias: handler}, clock=self.clock
        )

    def _observer_receiver(
        self, on_snapshot: Callable[[QueueEntry], None]
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        """Return this backend's optional cross-process lifecycle receiver.

        The returned callable, if any, must take no arguments -- bind
        `on_snapshot` into it before returning (e.g. via `functools.partial`)
        rather than expecting the caller to pass it. `QueueRuntime` calls the
        result as `receiver()` with no arguments to get the coroutine it
        schedules as a task.
        """
        return None

    def _configure_provider_entry_class(self) -> None:
        self._provider.entry_class = self.entry_class

    async def aenqueue(
        self,
        payload,
        *,
        timeout_seconds: float | None = None,
        priority: int = 0,
        available_at: ClockTime | None = None,
    ) -> UUID:
        validate_json_value(payload)
        if available_at is not None and not isinstance(available_at, ClockTime):
            raise TypeError("available_at must be a ClockTime or None")
        entry = self.entry_class.create(
            queue=self.queue_name,
            payload=payload,
            queued_at=await self.clock.anow(),
            timeout_seconds=timeout_seconds,
            priority=priority,
        )
        self._configure_provider_entry_class()
        if available_at is None:
            await self._astore_and_push(entry)
        else:
            await self._astore_and_push(entry, available_at=available_at)
        send_entry_enqueued(self, entry=entry)
        return entry.id

    async def _astore_and_push(
        self, entry: QueueEntry, *, available_at: ClockTime | None = None
    ) -> None:
        """Store a freshly enqueued entry and add it to the tracked pending
        store.

        Storing and pushing as one step (rather than two separate provider
        calls) matters on a backend whose record durably outlives the
        process, like Redis: without it, a crash between the two calls
        leaves a stored entry with no pending-store index pointing to it — a
        silent, permanent orphan, unlike the reverse case (an index entry
        with no record), which `_apop`'s `afind()` already surfaces as a
        named exception. The default here still does it as two calls, since
        the in-memory backend has no durability to protect across a crash;
        `RedisAsyncQueue`/`RedisAsyncPriorityQueue` override this with a
        single atomic Lua script instead.
        """
        await self._provider.astore(entry)
        if available_at is not None and available_at > await self.clock.anow():
            await self._provider.aschedule(entry.id, available_at)
        else:
            await self._apush(entry)

    async def _apush(self, entry: QueueEntry) -> None:
        """Add a freshly enqueued entry to the tracked pending store.

        Priority-variant backends override this (and `_apop`, `_adiscard`)
        to route through their own priority-ordered pending store instead —
        the only difference between a priority and non-priority `AsyncQueue`
        backend's tracked dispatch path.
        """
        await self._provider.apush(entry.id)

    async def _apop(self) -> QueueEntry:
        """Remove and return the next entry from the tracked pending store."""
        await self._apromote_scheduled()
        return await self._provider.apop()

    async def _apromote_scheduled(self) -> None:
        """Promote due scheduled entries before direct dequeue where supported."""

    async def _adiscard(self, entry_id: UUID) -> None:
        """Remove one entry from the tracked pending store without dispatching it."""
        await self._provider.adiscard(entry_id)
        await self._provider.adiscard_scheduled(entry_id)

    async def afind(self, entry_id: UUID) -> QueueEntry:
        self._configure_provider_entry_class()
        return await self._provider.afind(entry_id)

    async def alist(self) -> builtins.list[QueueEntry]:
        """Return retained entry snapshots for observation and administration."""
        self._configure_provider_entry_class()
        return await self._provider.alist()

    async def aprune(self, entry_id: UUID) -> None:
        self._configure_provider_entry_class()
        entry = await self._provider.aprune(entry_id)
        await self.apublish(replace(entry, status=QueueEntryStatus.TERMINATED))

    async def _aprune_expired(self) -> int:
        if self.retention_timeout is None:
            return 0
        now = await self.clock.anow()
        expired_entry_ids = [
            entry.id
            for entry in await self.alist()
            if QueueEntryStatus.TERMINATED in entry.status.next_state()
            and entry.finished_at is not None
            and now - entry.finished_at >= self.retention_timeout
        ]
        pruned = 0
        for entry_id in expired_entry_ids:
            try:
                await self.aprune(entry_id)
            except QueueEntryNotFoundError:
                continue
            pruned += 1
        return pruned

    async def adequeue(self) -> QueueEntry:
        self._configure_provider_entry_class()
        return await self._apop()

    async def ahas_pending(self) -> bool:
        return await self._provider.ahas_pending()

    def _mark_running(self, entry_id: UUID) -> QueueEntry:
        return self._run_synchronously(self._amark_running, entry_id)

    async def _amark_running(self, entry_id: UUID) -> QueueEntry:
        return await self._areplace_entry(
            entry_id,
            status=QueueEntryStatus.RUNNING,
            dispatched_at=await self.clock.anow(),
        )

    def _mark_succeeded(self, entry_id: UUID, result) -> QueueEntry:
        return self._run_synchronously(self._amark_succeeded, entry_id, result)

    async def _amark_succeeded(self, entry_id: UUID, result) -> QueueEntry:
        validate_json_value(result)
        return await self._areplace_entry(
            entry_id,
            status=QueueEntryStatus.SUCCEEDED,
            result=result,
            error=None,
            finished_at=await self.clock.anow(),
        )

    def _mark_failed(self, entry_id: UUID, error: Exception) -> QueueEntry:
        return self._run_synchronously(self._amark_failed, entry_id, error)

    async def _amark_failed(self, entry_id: UUID, error: Exception) -> QueueEntry:
        return await self._areplace_entry(
            entry_id,
            status=QueueEntryStatus.FAILED,
            error={"type": type(error).__name__, "message": str(error)},
            finished_at=await self.clock.anow(),
        )

    def _mark_cancelled(self, entry_id: UUID) -> QueueEntry:
        return self._run_synchronously(self._amark_cancelled, entry_id)

    async def _amark_cancelled(self, entry_id: UUID) -> QueueEntry:
        return await self._areplace_entry(
            entry_id,
            status=QueueEntryStatus.CANCELLED,
            finished_at=await self.clock.anow(),
        )

    def _mark_timed_out(self, entry_id: UUID) -> QueueEntry:
        return self._run_synchronously(self._amark_timed_out, entry_id)

    async def _amark_timed_out(self, entry_id: UUID) -> QueueEntry:
        """Record that a handler exceeded its budget and was abandoned."""
        return await self._areplace_entry(
            entry_id,
            status=QueueEntryStatus.TIMEOUT,
            finished_at=await self.clock.anow(),
        )

    def list(self) -> builtins.list[QueueEntry]:
        """Synchronously return retained entry snapshots."""
        return self._run_synchronously(self.alist)

    def prune(self, entry_id: UUID) -> None:
        """Remove one retained terminal entry and publish its final snapshot."""
        return self._run_synchronously(self.aprune, entry_id)

    async def _areplace_entry(
        self, entry_id: UUID, *, status: QueueEntryStatus, **changes
    ) -> QueueEntry:
        if not isinstance(status, QueueEntryStatus):
            raise TypeError("Queue entry status must be a QueueEntryStatus")
        if status is QueueEntryStatus.TERMINATED:
            raise ValueError(
                "Terminated queue entry snapshots are only published by pruning"
            )
        previous_entry = await self.afind(entry_id)
        if status not in previous_entry.status.next_state():
            raise ValueError(
                f"Cannot transition queue entry from {previous_entry.status} to {status}"
            )
        entry = replace(previous_entry, status=status, **changes)
        if (
            previous_entry.status is QueueEntryStatus.QUEUED
            and status is QueueEntryStatus.FAILED
        ):
            await self._astore_and_discard(entry)
        else:
            await self._provider.astore(entry)
        return entry

    async def _astore_and_discard(self, entry: QueueEntry) -> None:
        """Persist a queued terminal entry and remove dispatch membership."""
        await self._provider.astore(entry)
        await self._adiscard(entry.id)


class EventQueue(BaseQueue):
    """A queue whose listeners consume transient events."""

    default_lifetime_seconds = 60
    worker_class: type[EventQueueWorker] | str = (
        "django_queue.event_worker.EventQueueWorker"
    )
    compatible_worker_class: type[EventQueueWorker] | str = (
        "django_queue.event_worker.EventQueueWorker"
    )

    def __init__(self) -> None:
        # This identity belongs to the queue runtime, not to an individual
        # EventQueueWorker object. Runtime recovery can therefore recreate a
        # worker without changing the owner of this queue's active claim.
        self._event_worker_id = uuid7()
        self._event_worker_pid = os.getpid()

    def _configure_provider_entry_class(self) -> None:
        self._provider.entry_class = self.entry_class

    async def aclear(self) -> None:
        await super().aclear()
        for entry in await self._provider.alist():
            await self._provider.adelete(entry.id)

    async def aenqueue(
        self,
        payload,
        *,
        timeout_seconds: float | None = None,
        priority: int = 0,
        available_at: ClockTime | None = None,
    ) -> UUID:
        """`priority` and `available_at` are accepted for signature compatibility
        with `AsyncQueue` and ignored -- events always dispatch in arrival order."""
        validate_json_value(payload)
        lifetime = validate_budget(self._resolve_lifetime(timeout_seconds))
        entry = self.entry_class.create(
            queue=self.queue_name,
            payload=payload,
            queued_at=await self.clock.anow(),
            timeout_seconds=lifetime,
        )
        self._configure_provider_entry_class()
        await self._astore_and_push(entry)
        return entry.id

    async def _astore_and_push(self, entry: QueueEntry) -> None:
        """Store a freshly enqueued event and add it to the pending store.

        Same rationale as `AsyncQueue._astore_and_push`: the in-memory
        default does it as two (for events with a timeout, three) separate
        calls, since there is no crash durability to protect; `RedisEventQueue`
        overrides this with a single atomic Lua script.
        """
        await self._provider.astore_event(entry)
        await self._provider.apush(entry.id)

    async def afind(self, entry_id: UUID) -> QueueEntry:
        self._configure_provider_entry_class()
        return await self._provider.afind(entry_id)

    async def ahas_pending(self) -> bool:
        await self._aprune_expired()
        return await self._provider.ahas_pending()

    def _worker_id_for_runtime(self) -> UUID:
        """Return this queue runtime's private, stable worker identity."""
        pid = os.getpid()
        if (worker_id := getattr(self, "_event_worker_id", None)) is None or getattr(
            self, "_event_worker_pid", None
        ) != pid:
            # Accommodate third-party EventQueue subclasses that have not yet
            # called the new base initializer, and regenerate after a prefork
            # child inherits the parent's queue object and its old identity.
            worker_id = uuid7()
            self._event_worker_id = worker_id
            self._event_worker_pid = pid
        return worker_id

    def resolve_worker(self, alias: str) -> type[EventQueueWorker]:
        """Import and validate this event queue's configured worker class."""
        from django_queue.event_worker import EventQueueWorker

        return self._resolve_worker(alias, EventQueueWorker)

    def create_worker(self, alias: str) -> EventQueueWorker:
        """Create this queue's local listener worker."""
        return self.resolve_worker(alias)(self, alias=alias)

    def _resolve_lifetime(self, timeout_seconds: float | None) -> float:
        """Resolve an event's unconsumed lifetime from its available context."""
        return (
            timeout_seconds
            if timeout_seconds is not None
            else self.timeout_seconds
            if self.timeout_seconds is not None
            else self.default_lifetime_seconds
        )

    async def adequeue(self) -> QueueEntry:
        """Return and consume an event without exposing a claim owner.

        Listener workers need an owned claim while they decide whether to
        consume or retry. Direct queue consumers instead delegate one atomic
        consume operation to the provider.
        """
        return await self._provider.adequeue()

    async def _aprune_expired(self) -> int:
        """Remove unconsumed events whose configured lifetime elapsed."""
        expired_entry_ids = await self._provider.aexpire_due()
        for entry_id in expired_entry_ids:
            logger.warning(
                "Discarded expired event",
                extra={"queue": self.queue_name, "entry_id": str(entry_id)},
            )
        return len(expired_entry_ids)


class NotificationQueue(BaseQueue):
    """A queue whose listeners see a payload without owning it."""

    default_lifetime_seconds = 60
    worker_class: type[NotificationQueueWorker] | str = (
        "django_queue.notification_worker.NotificationQueueWorker"
    )
    compatible_worker_class: type[NotificationQueueWorker] | str = (
        "django_queue.notification_worker.NotificationQueueWorker"
    )

    def _configure_provider_entry_class(self) -> None:
        self._provider.entry_class = self.entry_class

    async def aclear(self) -> None:
        await super().aclear()
        await self._provider.aclear_notifications()

    async def aenqueue(
        self,
        payload,
        *,
        timeout_seconds: float | None = None,
        priority: int = 0,
        available_at: ClockTime | None = None,
    ) -> UUID:
        """`priority` and `available_at` are accepted for signature compatibility
        with `AsyncQueue` and ignored -- notifications always dispatch to every
        connected receiver that sees them, without ownership or ordering."""
        validate_json_value(payload)
        lifetime = validate_budget(self._resolve_lifetime(timeout_seconds))
        entry = self.entry_class.create(
            queue=self.queue_name,
            payload=payload,
            queued_at=await self.clock.anow(),
            timeout_seconds=lifetime,
        )
        self._configure_provider_entry_class()
        await self._astore_notification(entry)
        return entry.id

    async def _astore_notification(self, entry: QueueEntry) -> None:
        """Store a freshly enqueued notification for seeing and later expiry.

        Memory does this as local store plus an in-process seen copy. Redis
        implements store in the provider Function that writes the payload,
        indexes the deadline, and publishes.
        """
        await self._provider.astore_notification(entry)

    async def afind(self, entry_id: UUID) -> QueueEntry:
        self._configure_provider_entry_class()
        return await self._provider.aget_notification(entry_id)

    async def ahas_pending(self) -> bool:
        return await self._provider.ahas_notification()

    async def adequeue(self) -> QueueEntry:
        raise TypeError("NotificationQueue does not consume payloads")

    def resolve_worker(self, alias: str) -> type[NotificationQueueWorker]:
        """Import and validate this notification queue's configured worker class."""
        from django_queue.notification_worker import NotificationQueueWorker

        return self._resolve_worker(alias, NotificationQueueWorker)

    def create_worker(self, alias: str) -> NotificationQueueWorker:
        """Create this queue's local notification receiver."""
        return self.resolve_worker(alias)(self, alias=alias)

    def _resolve_lifetime(self, timeout_seconds: float | None) -> float:
        """Resolve a notification's stored lifetime from its available context."""
        return (
            timeout_seconds
            if timeout_seconds is not None
            else self.timeout_seconds
            if self.timeout_seconds is not None
            else self.default_lifetime_seconds
        )
