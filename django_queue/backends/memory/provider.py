"""In-process implementation of the queue-provider protocol."""

from __future__ import annotations

import asyncio
import heapq
import queue
from collections import deque
from threading import RLock
from uuid import UUID

from django_queue.backends.exceptions import (
    QueueClaimConflictError,
    QueueEmptyException,
    QueueEntryExpiredError,
    QueueEntryNotFoundError,
    QueueFullException,
)
from django_queue.clock import DEFAULT_CLOCK, ClockTime, QueueClock
from django_queue.entries import QueueEntry, QueueEntryStatus, validate_budget


class QueueProviderMemory:
    """Process-local entry storage, claims, and delayed availability."""

    def __init__(
        self,
        *,
        clock: QueueClock | None = None,
        stack: bool = False,
        maxsize: int = 0,
        entries: dict[UUID, QueueEntry] | None = None,
        pending: queue.Queue[UUID] | None = None,
    ) -> None:
        self._clock = clock or DEFAULT_CLOCK
        self._lock = RLock()
        self._stack = stack
        self._maxsize = maxsize
        self._items = (queue.LifoQueue if stack else queue.Queue)(maxsize=maxsize)
        self._priority_items: queue.PriorityQueue = queue.PriorityQueue(maxsize=maxsize)
        self._entries = {} if entries is None else entries
        self._pending = pending or (queue.LifoQueue() if stack else queue.Queue())
        self._pending_priority: queue.PriorityQueue = queue.PriorityQueue()
        self._pending_priority_sequence = 0
        self._claims: dict[UUID, UUID] = {}
        self._claim_deadlines: dict[UUID, ClockTime] = {}
        self._available_at: dict[UUID, ClockTime] = {}
        self._scheduled: dict[UUID, ClockTime] = {}
        self._unclaimed_deadlines: dict[UUID, ClockTime] = {}
        self._unclaimed_remaining: dict[UUID, float] = {}
        self._notification_entries: dict[UUID, QueueEntry] = {}
        self._notification_deadlines: dict[UUID, ClockTime] = {}
        self._notification_seen: deque[QueueEntry] = deque()

    @property
    def clock(self) -> QueueClock:
        return self._clock

    @property
    def stack(self) -> bool:
        return self._stack

    @property
    def capacity(self) -> int:
        return self._maxsize

    async def aadd(self, *items) -> None:
        for item in items:
            try:
                self._items.put_nowait(item)
            except queue.Full as exc:
                raise QueueFullException from exc

    async def aget(self):
        try:
            return self._items.get_nowait()
        except queue.Empty as exc:
            raise QueueEmptyException from exc

    async def apoll(self):
        while True:
            try:
                return self._items.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.01)

    async def apeek(self):
        with self._items.mutex:
            if not self._items.queue:
                raise QueueEmptyException
            return self._items.queue[-1] if self._stack else self._items.queue[0]

    async def asize(self) -> int:
        return self._items.qsize()

    async def aclear(self) -> None:
        while True:
            try:
                self._items.get_nowait()
            except queue.Empty:
                return

    async def aadd_priority(self, *items) -> None:
        for item in items:
            priority, value = 0, item
            if isinstance(value, (tuple, list)):
                priority, *value = item
                value = value[0] if len(value) == 1 else tuple(value)
            try:
                self._priority_items.put_nowait((-int(priority), value))
            except queue.Full as exc:
                raise QueueFullException from exc

    async def aget_priority(self):
        try:
            return self._priority_items.get_nowait()[1]
        except queue.Empty as exc:
            raise QueueEmptyException from exc

    async def apoll_priority(self):
        while True:
            try:
                return self._priority_items.get_nowait()[1]
            except queue.Empty:
                await asyncio.sleep(0.01)

    async def apeek_priority(self):
        with self._priority_items.mutex:
            if not self._priority_items.queue:
                raise QueueEmptyException
            return self._priority_items.queue[0][1]

    async def asize_priority(self) -> int:
        return self._priority_items.qsize()

    async def aclear_priority(self) -> None:
        while True:
            try:
                self._priority_items.get_nowait()
            except queue.Empty:
                return

    async def astore(self, entry: QueueEntry) -> None:
        with self._lock:
            self._entries[entry.id] = entry

    async def astore_event(self, entry: QueueEntry) -> None:
        if entry.timeout_seconds is None:
            raise ValueError("Event entries require a resolved lifetime")
        with self._lock:
            self._entries[entry.id] = entry
            self._unclaimed_deadlines[entry.id] = (
                entry.queued_at + entry.timeout_seconds
            )

    async def afind(self, entry_id: UUID) -> QueueEntry:
        with self._lock:
            try:
                return self._entries[entry_id]
            except KeyError as exc:
                raise QueueEntryNotFoundError(entry_id) from exc

    async def adelete(self, entry_id: UUID) -> None:
        # No caller relies on the priority-store cleanup below today --
        # adelete is only reached via EventQueue.aclear(), which never
        # touches the priority pending store -- but adelete's contract is
        # "remove entry_id from every store it could be sitting in", and
        # leaving it out would silently orphan an entry if a future caller
        # ever reached here while the entry was still queued in a priority
        # backend. Nested inside this same self._lock (an RLock, so
        # re-entrant) rather than acquired after releasing it, so a
        # concurrent apop_priority can never observe the entry still
        # present in the priority store after its durable record is
        # already gone.
        with self._lock:
            self._entries.pop(entry_id, None)
            self._claims.pop(entry_id, None)
            self._claim_deadlines.pop(entry_id, None)
            self._available_at.pop(entry_id, None)
            self._scheduled.pop(entry_id, None)
            self._unclaimed_deadlines.pop(entry_id, None)
            self._unclaimed_remaining.pop(entry_id, None)
            self._remove_pending(entry_id)
            await self.adiscard_priority(entry_id)

    async def aprune(self, entry_id: UUID) -> QueueEntry:
        entry = await self.afind(entry_id)
        if QueueEntryStatus.TERMINATED not in entry.status.next_state():
            raise ValueError("Only terminal queue entries can be pruned")
        await self.adelete(entry_id)
        return entry

    async def aexpire(self, entry_id: UUID) -> bool:
        """Delete an event only when no worker currently owns its claim."""
        with self._lock:
            if entry_id in self._claims:
                return False
            if entry_id not in self._entries:
                return False
            self._entries.pop(entry_id)
            self._available_at.pop(entry_id, None)
            self._scheduled.pop(entry_id, None)
            self._unclaimed_deadlines.pop(entry_id, None)
            self._unclaimed_remaining.pop(entry_id, None)
            self._remove_pending(entry_id)
            return True

    async def alist(self) -> list[QueueEntry]:
        with self._lock:
            return list(self._entries.values())

    async def apush(self, entry_id: UUID) -> None:
        with self._lock:
            self._pending.put_nowait(entry_id)

    async def aschedule(self, entry_id: UUID, available_at: ClockTime) -> None:
        with self._lock:
            self._scheduled[entry_id] = available_at

    async def apromote_scheduled(self) -> None:
        now = await self.clock.anow()
        with self._lock:
            entry_id = self._pop_next_due_scheduled(now)
            if entry_id is not None:
                self._pending.put_nowait(entry_id)

    async def apromote_scheduled_priority(self) -> None:
        now = await self.clock.anow()
        with self._lock:
            entry_id = self._pop_next_due_scheduled(now, priority=True)
            if entry_id is not None:
                entry = self._entries[entry_id]
                self._pending_priority_sequence += 1
                self._pending_priority.put_nowait(
                    (-int(entry.priority), self._pending_priority_sequence, entry_id)
                )

    def _pop_next_due_scheduled(
        self, now: ClockTime, *, priority: bool = False
    ) -> UUID | None:
        """Remove one valid entry from the earliest due availability group."""
        while due_entries := [
            (entry_id, available_at)
            for entry_id, available_at in self._scheduled.items()
            if available_at <= now
        ]:
            earliest_available_at = min(available_at for _, available_at in due_entries)
            candidates = [
                entry_id
                for entry_id, available_at in due_entries
                if available_at == earliest_available_at
            ]
            queued_candidates = [
                entry_id
                for entry_id in candidates
                if (entry := self._entries.get(entry_id)) is not None
                and entry.status is QueueEntryStatus.QUEUED
            ]
            for entry_id in candidates:
                if entry_id not in queued_candidates:
                    self._scheduled.pop(entry_id, None)
            if not queued_candidates:
                continue
            if priority:
                entry_id = max(
                    queued_candidates,
                    key=lambda candidate: self._entries[candidate].priority,
                )
            else:
                entry_id = queued_candidates[0]
            self._scheduled.pop(entry_id)
            return entry_id
        return None

    async def apop(self) -> QueueEntry:
        with self._lock:
            try:
                entry_id = self._pending.get_nowait()
                return self._entries[entry_id]
            except queue.Empty as exc:
                raise QueueEmptyException from exc
            except KeyError as exc:
                raise QueueEntryNotFoundError(entry_id) from exc

    async def adiscard(self, entry_id: UUID) -> None:
        with self._lock:
            self._remove_pending(entry_id)

    async def adiscard_scheduled(self, entry_id: UUID) -> None:
        with self._lock:
            self._scheduled.pop(entry_id, None)

    async def apush_priority(self, entry_id: UUID, priority: int) -> None:
        # A bare (-priority, entry_id) tuple ties equal priorities by
        # comparing entry_id -- which happens to sort chronologically for
        # uuid.uuid7()'s current CPython implementation, but that is an
        # implementation detail this queue does not control or document as
        # a promise, not an explicit ordering contract. A monotonic sequence
        # as the tuple's middle element breaks every tie before entry_id is
        # ever reached, giving arrival order deterministically. Matches the
        # Redis backend's own sequence counter (apush_priority there).
        with self._lock:
            self._pending_priority_sequence += 1
            self._pending_priority.put_nowait(
                (-int(priority), self._pending_priority_sequence, entry_id)
            )

    async def apop_priority(self) -> QueueEntry:
        with self._lock:
            try:
                _, _, entry_id = self._pending_priority.get_nowait()
                return self._entries[entry_id]
            except queue.Empty as exc:
                raise QueueEmptyException from exc
            except KeyError as exc:
                raise QueueEntryNotFoundError(entry_id) from exc

    async def adiscard_priority(self, entry_id: UUID) -> None:
        with self._lock, self._pending_priority.mutex:
            self._pending_priority.queue = [
                item for item in self._pending_priority.queue if item[2] != entry_id
            ]
            heapq.heapify(self._pending_priority.queue)

    async def ahas_pending(self) -> bool:
        with self._lock:
            return (
                not self._pending.empty()
                or not self._pending_priority.empty()
                or bool(self._scheduled)
            )

    async def aclaim(
        self, worker_id: UUID, lease_seconds: float | None = None
    ) -> QueueEntry:
        return await self._aclaim(worker_id, lease_seconds, expire_unclaimed=False)

    async def aclaim_unexpired(
        self, worker_id: UUID, lease_seconds: float | None = None
    ) -> QueueEntry:
        return await self._aclaim(worker_id, lease_seconds, expire_unclaimed=True)

    async def adequeue(self) -> QueueEntry:
        """Atomically remove and return the next unclaimed live event."""
        now = await self.clock.anow()
        with self._lock:
            self._recover_expired_claims(now)
            for _ in range(self._pending.qsize()):
                try:
                    entry_id = self._pending.get_nowait()
                except queue.Empty:
                    break
                available_at = self._available_at.get(entry_id)
                if available_at is not None and available_at > now:
                    self._requeue_skipped_pending(entry_id)
                    continue
                if entry_id in self._claims:
                    self._requeue_skipped_pending(entry_id)
                    continue
                entry = self._entries.get(entry_id)
                if entry is None:
                    self._delete_event(entry_id)
                    continue
                deadline = self._unclaimed_deadlines.get(entry_id)
                if deadline is None or deadline <= now:
                    self._delete_event(entry_id)
                    continue
                self._delete_event(entry_id)
                return entry
        raise QueueEmptyException

    async def _aclaim(
        self,
        worker_id: UUID,
        lease_seconds: float | None,
        *,
        expire_unclaimed: bool,
    ) -> QueueEntry:
        now = await self.clock.anow()
        if lease_seconds is not None:
            validate_budget(lease_seconds)
        await self.apromote_scheduled()
        with self._lock:
            self._recover_expired_claims(now)
            for _ in range(self._pending.qsize()):
                try:
                    entry_id = self._pending.get_nowait()
                except queue.Empty:
                    break
                available_at = self._available_at.get(entry_id)
                if available_at is not None and available_at > now:
                    self._requeue_skipped_pending(entry_id)
                    continue
                if entry_id in self._claims:
                    # A duplicate pending ID must not steal an active claim.
                    # Preserve it for the owner to settle or for lease recovery.
                    self._requeue_skipped_pending(entry_id)
                    raise QueueClaimConflictError(entry_id)
                try:
                    entry = self._entries[entry_id]
                except KeyError as exc:
                    raise QueueEntryNotFoundError(entry_id) from exc
                if (
                    expire_unclaimed
                    and (deadline := self._unclaimed_deadlines.get(entry_id))
                    is not None
                    and deadline <= now
                ):
                    self._entries.pop(entry_id)
                    self._available_at.pop(entry_id, None)
                    self._unclaimed_deadlines.pop(entry_id, None)
                    raise QueueEntryExpiredError(entry_id)
                if expire_unclaimed:
                    deadline = self._unclaimed_deadlines.pop(entry_id, None)
                    if deadline is None:
                        raise QueueEntryExpiredError(entry_id)
                    self._unclaimed_remaining[entry_id] = deadline - now
                self._available_at.pop(entry_id, None)
                self._claims[entry_id] = worker_id
                if lease_seconds is not None:
                    self._claim_deadlines[entry_id] = now + lease_seconds
                return entry
        raise QueueEmptyException

    async def arenew(
        self, entry_id: UUID, worker_id: UUID, lease_seconds: float
    ) -> bool:
        validate_budget(lease_seconds)
        now = await self.clock.anow()
        with self._lock:
            self._recover_expired_claims(now)
            if self._claims.get(entry_id) != worker_id:
                return False
            self._claim_deadlines[entry_id] = now + lease_seconds
            return True

    async def arelease(
        self, entry_id: UUID, worker_id: UUID, delay_seconds: float
    ) -> bool:
        validate_budget(delay_seconds)
        released_at = await self.clock.anow()
        available_at = released_at + delay_seconds
        with self._lock:
            if self._claims.get(entry_id) != worker_id:
                return False
            self._claims.pop(entry_id)
            self._claim_deadlines.pop(entry_id, None)
            if (remaining := self._unclaimed_remaining.pop(entry_id, None)) is not None:
                self._unclaimed_deadlines[entry_id] = released_at + remaining
            self._available_at[entry_id] = available_at
            self._pending.put_nowait(entry_id)
            return True

    async def aremove(self, entry_id: UUID, worker_id: UUID) -> bool:
        with self._lock:
            if self._claims.get(entry_id) != worker_id:
                return False
            self._entries.pop(entry_id, None)
            self._claims.pop(entry_id, None)
            self._claim_deadlines.pop(entry_id, None)
            self._available_at.pop(entry_id, None)
            self._unclaimed_deadlines.pop(entry_id, None)
            self._unclaimed_remaining.pop(entry_id, None)
            self._remove_pending(entry_id)
            return True

    async def aclose(self) -> None:
        return None

    async def aexpire_due(self) -> list[UUID]:
        now = await self.clock.anow()
        with self._lock:
            expired_ids = [
                entry_id
                for entry_id, deadline in self._unclaimed_deadlines.items()
                if deadline <= now and entry_id not in self._claims
            ]
            for entry_id in expired_ids:
                self._delete_event(entry_id)
            return expired_ids

    def _remove_pending(self, entry_id: UUID) -> None:
        with self._pending.mutex:
            try:
                self._pending.queue.remove(entry_id)
            except ValueError:
                pass

    def _requeue_skipped_pending(self, entry_id: UUID) -> None:
        """Preserve skipped LIFO IDs without placing them back on top."""
        if not self._stack:
            self._pending.put_nowait(entry_id)
            return
        with self._pending.mutex:
            self._pending.queue.appendleft(entry_id)
            self._pending.not_empty.notify()

    def _recover_expired_claims(self, now: ClockTime) -> None:
        """Return uncompleted local claims to pending delivery after their lease."""
        expired_ids = [
            entry_id
            for entry_id, deadline in self._claim_deadlines.items()
            if deadline <= now
        ]
        for entry_id in expired_ids:
            self._claims.pop(entry_id, None)
            self._claim_deadlines.pop(entry_id, None)
            if entry_id in self._entries:
                if (
                    remaining := self._unclaimed_remaining.pop(entry_id, None)
                ) is not None:
                    self._unclaimed_deadlines[entry_id] = now + remaining
                self._pending.put_nowait(entry_id)

    def _delete_event(self, entry_id: UUID) -> None:
        self._entries.pop(entry_id, None)
        self._available_at.pop(entry_id, None)
        self._unclaimed_deadlines.pop(entry_id, None)
        self._unclaimed_remaining.pop(entry_id, None)
        self._remove_pending(entry_id)

    async def astore_notification(self, entry: QueueEntry) -> None:
        if entry.timeout_seconds is None:
            raise ValueError("Notification entries require a resolved lifetime")
        with self._lock:
            self._notification_entries[entry.id] = entry
            self._notification_deadlines[entry.id] = (
                entry.queued_at + entry.timeout_seconds
            )
            self._notification_seen.append(entry)

    async def aget_notification(self, entry_id: UUID) -> QueueEntry:
        with self._lock:
            entry = self._notification_entries.get(entry_id)
            if entry is None:
                raise QueueEntryNotFoundError(entry_id)
            return entry

    async def ahas_notification(self) -> bool:
        with self._lock:
            return bool(self._notification_entries)

    async def asee_next_notification(self) -> QueueEntry | None:
        with self._lock:
            if not self._notification_seen:
                return None
            return self._notification_seen.popleft()

    async def aexpire_due_notifications(self) -> list[UUID]:
        now = await self.clock.anow()
        with self._lock:
            due = [
                entry_id
                for entry_id, deadline in self._notification_deadlines.items()
                if deadline <= now
            ]
            if not due:
                return []
            due.sort(key=lambda entry_id: self._notification_deadlines[entry_id])
            entry_id = due[0]
            self._notification_entries.pop(entry_id, None)
            self._notification_deadlines.pop(entry_id, None)
            return [entry_id]

    async def aclear_notifications(self) -> None:
        with self._lock:
            self._notification_entries.clear()
            self._notification_deadlines.clear()
            self._notification_seen.clear()
