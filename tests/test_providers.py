import asyncio
import json
from typing import runtime_checkable
from uuid import uuid4

import pytest

import django_queue
from django_queue import QueueProvider
from django_queue.backends.exceptions import (
    QueueClaimConflictError,
    QueueEmptyException,
)
from django_queue.backends.memory.provider import QueueProviderMemory
from django_queue.backends.redis.provider import QueueProviderRedis
from django_queue.entries import QueueEntry


def test_providers_implement_the_minimal_public_provider_contract():
    assert runtime_checkable(QueueProvider)
    assert isinstance(QueueProviderMemory(), QueueProvider)
    assert isinstance(
        QueueProviderRedis("redis://localhost:6379/0", entry_class=QueueEntry),
        QueueProvider,
    )
    assert hasattr(QueueProvider, "aclose")
    assert not hasattr(QueueProvider, "clock")
    assert not hasattr(QueueProvider, "aclaim")
    assert not hasattr(QueueProvider, "astore")
    assert "AsyncQueueProvider" not in django_queue.__all__
    assert "EventQueueProvider" not in django_queue.__all__
    assert "QueueProviderMemory" not in django_queue.__all__
    assert "QueueProviderRedis" not in django_queue.__all__


def test_redis_provider_claims_and_removes_an_owned_entry(
    redis_client, redis_function_library
):
    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"provider-contract-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(queue="events", payload={"event": "sent"})
            await provider.astore(entry)
            await provider.apush(entry.id)

            worker_id = uuid4()
            assert await provider.aclaim(worker_id) == entry
            assert await provider.aremove(entry.id, worker_id)
        finally:
            await provider.aclose()

    asyncio.run(exercise())


def test_redis_provider_recovers_an_expired_priority_claim_to_the_priority_store(
    redis_client, redis_function_library
):
    """arecover unconditionally redelivers to the plain pending list --
    correct for a plain FIFO/stack queue, but a priority-variant queue's
    entries only ever live in the priority ZSET, and redelivering there
    would silently downgrade a recovered entry to FIFO dispatch, losing
    the priority ordering it was originally enqueued with. Exercises
    arecover_priority, the priority-aware recovery path, directly."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"recover-priority-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(queue="tasks", payload="work", priority=7)
            await provider.astore(entry)
            await provider.apush_priority(entry.id, entry.priority)

            worker_id = uuid4()
            claimed = await provider.aclaim_priority(worker_id, lease_seconds=0.01)
            assert claimed == entry

            await asyncio.sleep(0.05)
            recovered, discarded = await provider.arecover_priority(batch_size=10)
            assert (recovered, discarded) == (1, 0)

            with pytest.raises(QueueEmptyException):
                await provider.aclaim(uuid4())
            redelivered = await provider.apop_priority()
            assert redelivered.id == entry.id
        finally:
            await provider.aclose()

    asyncio.run(exercise())


def test_redis_provider_recover_priority_tolerates_a_record_missing_the_priority_field(
    redis_client, redis_function_library
):
    """A stored entry record is always written by QueueEntry.to_dict(), which
    serialises every dataclass field including `priority`, so no current
    writer omits it -- but a hand-crafted or future migration-written record
    could. _RECOVER_SCRIPT_WITH_PRIORITY reads entry.priority directly into a
    score calculation; without a fallback, a missing field decodes to Lua
    nil and the multiplication crashes the whole recovery batch with
    "attempt to perform arithmetic on a nil value" instead of just treating
    the entry as unprioritised."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"recover-priority-nil-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(queue="tasks", payload="work", priority=7)
            record = entry.to_dict()
            del record["priority"]
            await provider._async_redis().set(
                provider._entry_key(entry.id),
                json.dumps(record),
            )
            await provider.apush_priority(entry.id, 7)

            worker_id = uuid4()
            await provider.aclaim_priority(worker_id, lease_seconds=0.01)
            await asyncio.sleep(0.05)

            recovered, discarded = await provider.arecover_priority(batch_size=10)
            assert (recovered, discarded) == (1, 0)
        finally:
            await provider.aclose()

    asyncio.run(exercise())


def test_memory_provider_does_not_replace_an_active_claim_with_a_duplicate_id():
    async def exercise():
        provider = QueueProviderMemory()
        entry = QueueEntry.create(queue="events", payload={"event": "sent"})
        first_worker = uuid4()
        await provider.astore(entry)
        await provider.apush(entry.id)
        assert await provider.aclaim(first_worker) == entry

        await provider.apush(entry.id)
        with pytest.raises(QueueClaimConflictError):
            await provider.aclaim(uuid4())
        assert not await provider.aremove(entry.id, uuid4())
        assert await provider.aremove(entry.id, first_worker)

    asyncio.run(exercise())


def test_redis_provider_priority_claim_conflict_reinserts_at_the_original_score(
    redis_client,
):
    """_CLAIM_SCRIPT_WITH_PRIORITY pops an entry off the priority ZSET, then
    SETs its claim key with NX -- if that SET loses a race to a concurrent
    claim of the same entry_id, the entry must go back into the ZSET at
    exactly the score it had before the pop, not a freshly computed one,
    since it was never truly removed from the caller's perspective; a fresh
    score would corrupt arrival order relative to entries that arrived
    later but were never touched. This conflict branch
    (`if priority_score then ZADD KEYS[7], priority_score, entry_id`) had no
    regression coverage (claude.json CR-2). Re-pushing the same entry_id
    while its claim is still live forces the conflict outcome
    deterministically, without needing real concurrency. Only the priority
    path is covered here: the plain (non-priority) pending store is a Redis
    LIST, not a ZSET, so "original score" doesn't apply there the same way,
    and CR-2 was specifically about the priority-sourced conflict branch."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"claim-conflict-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(queue="tasks", payload="work", priority=5)
            await provider.astore(entry)
            await provider.apush_priority(entry.id, entry.priority)

            client = provider._async_redis()
            original_score = await client.zscore(
                provider._entry_pending_priority_name, str(entry.id)
            )

            first_worker = uuid4()
            claimed = await provider.aclaim_priority(first_worker, lease_seconds=60)
            assert claimed.id == entry.id

            await provider.apush_priority(entry.id, entry.priority)
            with pytest.raises(QueueClaimConflictError):
                await provider.aclaim_priority(uuid4(), lease_seconds=60)

            score_after_conflict = await client.zscore(
                provider._entry_pending_priority_name, str(entry.id)
            )
            plain_list_len = await client.llen(provider._entry_pending_name)
            return original_score, score_after_conflict, plain_list_len
        finally:
            await provider.aclose()

    original_score, score_after_conflict, plain_list_len = asyncio.run(exercise())
    assert score_after_conflict == original_score
    assert plain_list_len == 0


def test_redis_provider_astore_and_push_makes_the_entry_findable_and_claimable(
    redis_client,
):
    """astore() followed by a separate apush() -- two round-trips -- leaves
    a window where a crash between them durably stores the entry with no
    pending-store index pointing to it: a silent, permanent orphan, never
    claimed or dequeued, discoverable only by afind()ing its exact ID.
    astore_and_push() closes that window with one atomic script; this test
    only proves the combined call leaves both sides correctly populated,
    since the crash-mid-write window itself isn't reproducible from a test."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"atomic-enqueue-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(queue="tasks", payload="work")
            await provider.astore_and_push(entry)

            found = await provider.afind(entry.id)
            claimed = await provider.aclaim(uuid4())
            return found, claimed
        finally:
            await provider.aclose()

    found, claimed = asyncio.run(exercise())
    assert found.id == claimed.id


def test_redis_provider_astore_and_push_priority_makes_the_entry_findable_and_claimable(
    redis_client,
):
    """Priority-variant equivalent of
    test_redis_provider_astore_and_push_makes_the_entry_findable_and_claimable
    -- astore_and_push_priority() stores the record and adds it to the
    priority ZSET in one atomic script."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"atomic-enqueue-priority-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(queue="tasks", payload="work", priority=7)
            await provider.astore_and_push_priority(entry)

            found = await provider.afind(entry.id)
            claimed = await provider.aclaim_priority(uuid4())
            return found, claimed
        finally:
            await provider.aclose()

    found, claimed = asyncio.run(exercise())
    assert found.id == claimed.id


@pytest.mark.parametrize("priority", [100_001, -100_001])
def test_redis_provider_apush_priority_rejects_a_priority_beyond_the_encoding_range(
    redis_client, priority
):
    """QueueEntry itself no longer bounds priority -- a plain (non-priority)
    AsyncQueue/EventQueue must ignore it entirely per spec, so QueueEntry
    cannot reject a value on the Redis priority backend's behalf. The bound
    exists because this queue packs priority and a sequence number into one
    ZSET score (see MAX_PRIORITY_MAGNITUDE in provider.py); beyond it, the
    packing would silently lose precision instead of raising. Enforced
    here, at apush_priority itself -- the entry-tracked path's equivalent
    is astore_and_push_priority, covered by the next test."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"priority-bound-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            with pytest.raises(ValueError, match="Queue entry priority"):
                await provider.apush_priority(uuid4(), priority)
        finally:
            await provider.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize("priority", [100_001, -100_001])
def test_redis_provider_astore_and_push_priority_rejects_a_priority_beyond_the_encoding_range(
    redis_client, priority
):
    """astore_and_push_priority()'s equivalent of the apush_priority bound
    test above -- the entry-tracked path re-derives the same score from
    entry.priority, so it needs the same guard."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"priority-bound-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(queue="tasks", payload="work", priority=priority)
            with pytest.raises(ValueError, match="Queue entry priority"):
                await provider.astore_and_push_priority(entry)
        finally:
            await provider.aclose()

    asyncio.run(exercise())


def test_redis_provider_plain_apush_ignores_a_priority_beyond_the_encoding_range(
    redis_client,
):
    """A plain (non-priority) queue ignores `priority` entirely per spec
    -- apush() takes no priority argument at all, so an entry carrying a
    priority beyond the Redis-specific encoding bound must enqueue and
    dispatch normally through the plain path, never touching the
    priority-only bound. Confirms the fix for the finding that a plain
    backend previously rejected such an entry at construction time,
    contradicting "ignore it"."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"plain-ignores-bound-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(queue="tasks", payload="work", priority=200_000)
            await provider.astore_and_push(entry)

            found = await provider.afind(entry.id)
            claimed = await provider.aclaim(uuid4())
            return found, claimed
        finally:
            await provider.aclose()

    found, claimed = asyncio.run(exercise())
    assert found.id == claimed.id
    assert found.priority == 200_000


def test_redis_provider_astore_event_and_push_makes_the_event_findable_and_claimable(
    redis_client,
):
    """Event-queue equivalent -- astore_event() (itself two writes: the
    entry record and the unclaimed-deadline ZSET) followed by a separate
    apush() was three round-trips total. astore_event_and_push() combines
    all three into one atomic script."""

    async def exercise():
        provider = QueueProviderRedis(
            redis_client,
            queue_name=f"atomic-enqueue-event-{uuid4().hex}",
            entry_class=QueueEntry,
        )
        try:
            entry = QueueEntry.create(
                queue="events", payload={"event": "sent"}, timeout_seconds=60
            )
            await provider.astore_event_and_push(entry)

            found = await provider.afind(entry.id)
            claimed = await provider.aclaim(uuid4())
            unclaimed_deadline = await provider._async_redis().zscore(
                provider._entry_unclaimed_deadlines_name, str(entry.id)
            )
            return found, claimed, unclaimed_deadline
        finally:
            await provider.aclose()

    found, claimed, unclaimed_deadline = asyncio.run(exercise())
    assert found.id == claimed.id
    assert unclaimed_deadline is not None
