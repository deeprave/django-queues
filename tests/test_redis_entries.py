import asyncio
import threading
import time
from uuid import uuid4

import pytest

import django_queue
from django_queue import queue_observer
from django_queue.backends.exceptions import (
    QueueEmptyException,
    QueueEntryNotFoundError,
)
from django_queue.backends.redis import (
    RedisAsyncPriorityQueue,
    RedisAsyncQueue,
    RedisAsyncQueueWorker,
)
from django_queue.entries import QueueEntryStatus
from django_queue.observers import _discard_observers_for
from django_queue.queue_runtime import queue_runtime
from tests.helpers import CustomQueueEntry


@pytest.fixture
def redis_entry_queue(redis_client):
    return RedisAsyncQueue(redis_client, queue_name=f"entries-{uuid4().hex}")


async def _run_until_terminal(queue, entry_id, handler):
    worker = RedisAsyncQueueWorker(
        {"requests": queue}, {"requests": handler}, idle_delay=0.001
    )
    task = asyncio.create_task(worker.run())
    try:
        while (await queue.afind(entry_id)).status not in {
            QueueEntryStatus.SUCCEEDED,
            QueueEntryStatus.FAILED,
        }:
            await asyncio.sleep(0.001)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def test_find_reports_a_missing_retained_record(redis_entry_queue):
    with pytest.raises(QueueEntryNotFoundError):
        redis_entry_queue.find(uuid4())


def test_prunes_a_terminal_entry(redis_entry_queue):
    async def exercise():
        entry_id = await redis_entry_queue.aenqueue("work")

        async def handle(entry):
            return entry.payload

        await _run_until_terminal(redis_entry_queue, entry_id, handle)
        await redis_entry_queue.aclose()
        return entry_id

    entry_id = asyncio.run(exercise())

    redis_entry_queue.prune(entry_id)

    with pytest.raises(QueueEntryNotFoundError):
        redis_entry_queue.find(entry_id)


def test_prune_refuses_a_non_terminal_entry(redis_entry_queue):
    entry_id = redis_entry_queue.enqueue("work")

    with pytest.raises(ValueError, match="terminal"):
        redis_entry_queue.prune(entry_id)

    assert redis_entry_queue.find(entry_id).status is QueueEntryStatus.QUEUED


def test_list_returns_retained_entry_snapshots(redis_client):
    queue = RedisAsyncQueue(redis_client, queue_name=f"list-{uuid4().hex}")

    async def exercise():
        completed_id = await queue.aenqueue("completed")
        queued_id = await queue.aenqueue("queued")

        async def handle(entry):
            return entry.payload

        await _run_until_terminal(queue, completed_id, handle)
        entries = await queue.alist()
        await queue.aclose()
        return completed_id, queued_id, entries

    completed_id, queued_id, entries = asyncio.run(exercise())

    assert {entry.id for entry in entries} == {completed_id, queued_id}
    assert {entry.status for entry in entries if entry.id == completed_id} == {
        QueueEntryStatus.SUCCEEDED
    }


def test_direct_dequeue_is_atomic_and_fifo(redis_entry_queue):
    first_id = redis_entry_queue.enqueue("first")
    second_id = redis_entry_queue.enqueue("second")

    assert redis_entry_queue.dequeue().id == first_id
    assert redis_entry_queue.dequeue().id == second_id
    with pytest.raises(QueueEmptyException):
        redis_entry_queue.dequeue()


def test_redis_schedules_a_future_entry_without_making_it_claimable(redis_entry_queue):
    async def exercise():
        available_at = await redis_entry_queue.clock.anow()
        entry_id = await redis_entry_queue.aenqueue(
            "later", available_at=available_at + 60
        )
        assert await redis_entry_queue.ahas_pending()
        with pytest.raises(QueueEmptyException):
            await redis_entry_queue.aclaim(uuid4())
        score = await redis_entry_queue._provider._async_redis().zscore(
            redis_entry_queue._provider._entry_scheduled_name, str(entry_id)
        )
        await redis_entry_queue.aclose()
        return entry_id, score, available_at

    entry_id, score, available_at = asyncio.run(exercise())

    assert entry_id
    assert (
        score
        == (available_at + 60).seconds * 1_000_000 + (available_at + 60).microseconds
    )


def test_redis_enqueue_with_an_already_due_time_is_immediately_claimable(
    redis_entry_queue,
):
    async def exercise():
        available_at = await redis_entry_queue.clock.anow() - 1
        entry_id = await redis_entry_queue.aenqueue("now", available_at=available_at)
        claimed = await redis_entry_queue.aclaim(uuid4())
        score = await redis_entry_queue._provider._async_redis().zscore(
            redis_entry_queue._provider._entry_scheduled_name, str(entry_id)
        )
        await redis_entry_queue.aclose()
        return entry_id, claimed.id, score

    entry_id, claimed_id, score = asyncio.run(exercise())

    assert claimed_id == entry_id
    assert score is None


def test_redis_claim_promotes_a_due_scheduled_entry(redis_entry_queue):
    async def exercise():
        now = await redis_entry_queue.clock.anow()
        entry_id = await redis_entry_queue.aenqueue("due", available_at=now + 60)
        await redis_entry_queue._provider._async_redis().zadd(
            redis_entry_queue._provider._entry_scheduled_name, {str(entry_id): 0}
        )
        claimed = await redis_entry_queue.aclaim(uuid4())
        await redis_entry_queue.aclose()
        return entry_id, claimed.id

    entry_id, claimed_id = asyncio.run(exercise())

    assert claimed_id == entry_id


def test_redis_dequeue_promotes_due_scheduled_entry(redis_entry_queue):
    async def exercise():
        now = await redis_entry_queue.clock.anow()
        entry_id = await redis_entry_queue.aenqueue("due", available_at=now + 60)
        await redis_entry_queue._provider._async_redis().zadd(
            redis_entry_queue._provider._entry_scheduled_name, {str(entry_id): 0}
        )
        dequeued = await redis_entry_queue.adequeue()
        await redis_entry_queue.aclose()
        return entry_id, dequeued.id

    entry_id, dequeued_id = asyncio.run(exercise())

    assert dequeued_id == entry_id


def test_concurrent_redis_dequeue_delivers_a_due_scheduled_entry_once(redis_client):
    async def exercise():
        queue_name = f"concurrent-dequeue-{uuid4().hex}"
        first = RedisAsyncQueue(redis_client, queue_name=queue_name)
        second = RedisAsyncQueue(redis_client, queue_name=queue_name)
        try:
            now = await first.clock.anow()
            entry_id = await first.aenqueue("due", available_at=now + 60)
            await first._provider._async_redis().zadd(
                first._provider._entry_scheduled_name, {str(entry_id): 0}
            )
            results = await asyncio.gather(
                first.adequeue(), second.adequeue(), return_exceptions=True
            )
            return entry_id, results
        finally:
            await first.aclose()
            await second.aclose()

    entry_id, results = asyncio.run(exercise())

    assert [result.id for result in results if not isinstance(result, Exception)] == [
        entry_id
    ]
    assert sum(isinstance(result, QueueEmptyException) for result in results) == 1


def test_redis_priority_claim_promotes_due_work_by_priority(redis_client):
    queue = RedisAsyncPriorityQueue(redis_client, queue_name=f"priority-{uuid4().hex}")

    async def exercise():
        now = await queue.clock.anow()
        due_high = await queue.aenqueue("high", priority=10, available_at=now + 60)
        due_low = await queue.aenqueue("low", priority=1, available_at=now + 60)
        await queue._provider._async_redis().zadd(
            queue._provider._entry_scheduled_name,
            {str(due_high): 0, str(due_low): 0},
        )
        claimed = await queue.aclaim(uuid4())
        await queue.aclose()
        return due_high, claimed.id

    due_high, claimed_id = asyncio.run(exercise())

    assert claimed_id == due_high


def test_redis_priority_claim_releases_the_earliest_due_group_first(redis_client):
    queue = RedisAsyncPriorityQueue(redis_client, queue_name=f"priority-{uuid4().hex}")

    async def exercise():
        now = await queue.clock.anow()
        earlier_low = await queue.aenqueue("earlier", priority=1, available_at=now + 60)
        later_high = await queue.aenqueue("later", priority=10, available_at=now + 120)
        await queue._provider._async_redis().zadd(
            queue._provider._entry_scheduled_name,
            {str(earlier_low): 0, str(later_high): 1},
        )
        claimed = await queue.aclaim(uuid4())
        await queue.aclose()
        return earlier_low, claimed.id

    earlier_low, claimed_id = asyncio.run(exercise())

    assert claimed_id == earlier_low


def test_redis_priority_dequeue_promotes_due_scheduled_work(redis_client):
    queue = RedisAsyncPriorityQueue(redis_client, queue_name=f"priority-{uuid4().hex}")

    async def exercise():
        now = await queue.clock.anow()
        entry_id = await queue.aenqueue("due", priority=10, available_at=now + 60)
        await queue._provider._async_redis().zadd(
            queue._provider._entry_scheduled_name, {str(entry_id): 0}
        )
        dequeued = await queue.adequeue()
        await queue.aclose()
        return entry_id, dequeued.id

    entry_id, dequeued_id = asyncio.run(exercise())

    assert dequeued_id == entry_id


def test_concurrent_redis_priority_dequeue_delivers_a_due_scheduled_entry_once(
    redis_client,
):
    async def exercise():
        queue_name = f"concurrent-priority-dequeue-{uuid4().hex}"
        first = RedisAsyncPriorityQueue(redis_client, queue_name=queue_name)
        second = RedisAsyncPriorityQueue(redis_client, queue_name=queue_name)
        try:
            now = await first.clock.anow()
            entry_id = await first.aenqueue("due", priority=10, available_at=now + 60)
            await first._provider._async_redis().zadd(
                first._provider._entry_scheduled_name, {str(entry_id): 0}
            )
            results = await asyncio.gather(
                first.adequeue(), second.adequeue(), return_exceptions=True
            )
            return entry_id, results
        finally:
            await first.aclose()
            await second.aclose()

    entry_id, results = asyncio.run(exercise())

    assert [result.id for result in results if not isinstance(result, Exception)] == [
        entry_id
    ]
    assert sum(isinstance(result, QueueEmptyException) for result in results) == 1


def test_raw_values_and_retained_entries_are_independent(redis_entry_queue):
    redis_entry_queue.add("raw-value")
    entry_id = redis_entry_queue.enqueue({"request_id": 42})

    assert redis_entry_queue.get() == "raw-value"
    assert redis_entry_queue.find(entry_id).payload == {"request_id": 42}


def test_adelete_does_not_create_a_priority_sequence_key_for_a_plain_queue(
    redis_entry_queue,
):
    """adelete unconditionally calls adiscard_priority as one of its cleanup
    steps (RedisAsyncQueue never populates the priority store, but adelete's
    contract is to clean up every store an entry could be sitting in). That
    call must not create a stray, otherwise-unused sequence key on a queue
    type that never uses the priority path at all."""

    async def scenario():
        try:
            entry_id = await redis_entry_queue.aenqueue("work")
            await redis_entry_queue._provider.adelete(entry_id)
            client = redis_entry_queue._provider._async_redis()
            return await client.get(
                redis_entry_queue._provider._entry_pending_priority_sequence_name
            )
        finally:
            await redis_entry_queue.aclose()

    assert asyncio.run(scenario()) is None


def test_redis_deleting_scheduled_entry_removes_its_membership(redis_entry_queue):
    async def exercise():
        now = await redis_entry_queue.clock.anow()
        entry_id = await redis_entry_queue.aenqueue("later", available_at=now + 60)
        await redis_entry_queue._provider.adelete(entry_id)
        client = redis_entry_queue._provider._async_redis()
        score = await client.zscore(
            redis_entry_queue._provider._entry_scheduled_name, str(entry_id)
        )
        await redis_entry_queue.aclose()
        return score

    assert asyncio.run(exercise()) is None


def test_aenqueue_routes_through_the_atomic_store_and_push_path(redis_entry_queue):
    """aenqueue()'s astore()+apush() split was previously two separate
    round-trips -- a crash between them left a durably stored entry with no
    pending-store index pointing to it. RedisAsyncQueue._astore_and_push
    should route through the atomic astore_and_push() script instead,
    never calling the plain, non-atomic astore()/apush() individually.
    Monkeypatching astore()/apush() to raise proves aenqueue() never calls
    them for a Redis-backed queue."""
    provider = redis_entry_queue._provider

    async def _fail(*args, **kwargs):
        raise AssertionError("aenqueue() must not call the non-atomic astore/apush")

    provider.astore = _fail
    provider.apush = _fail

    entry_id = redis_entry_queue.enqueue("work")

    assert redis_entry_queue.find(entry_id).id == entry_id
    assert redis_entry_queue.dequeue().id == entry_id


def test_redis_queue_restores_the_configured_entry_class(redis_client):
    queue = RedisAsyncQueue(
        redis_client,
        queue_name=f"entry-class-{uuid4().hex}",
        entry_class=CustomQueueEntry,
    )

    async def exercise():
        entry_id = await queue.aenqueue("work")
        entry = await queue.afind(entry_id)
        await queue.aclose()
        return entry

    assert isinstance(asyncio.run(exercise()), CustomQueueEntry)


def test_redis_worker_records_success(redis_client):
    queue = RedisAsyncQueue(redis_client, queue_name=f"success-{uuid4().hex}")

    async def exercise():
        entry_id = await queue.aenqueue("work")

        async def handle(entry):
            return entry.payload

        await _run_until_terminal(queue, entry_id, handle)
        entry = await queue.afind(entry_id)
        await queue.aclose()
        return entry

    entry = asyncio.run(exercise())

    assert entry.status is QueueEntryStatus.SUCCEEDED
    assert entry.result == "work"


def test_redis_worker_records_failure(redis_client):
    queue = RedisAsyncQueue(redis_client, queue_name=f"failure-{uuid4().hex}")

    async def exercise():
        entry_id = await queue.aenqueue("work")

        async def handle(entry):
            raise ValueError(entry.payload)

        await _run_until_terminal(queue, entry_id, handle)
        entry = await queue.afind(entry_id)
        await queue.aclose()
        return entry

    entry = asyncio.run(exercise())

    assert entry.status is QueueEntryStatus.FAILED
    assert entry.error == {"type": "ValueError", "message": "work"}


def test_pruning_publishes_a_terminated_snapshot_to_an_observer(
    redis_client, monkeypatch
):
    queue_runtime.start_thread()
    handler = django_queue.QueueRegistry(
        {
            "requests": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": redis_client,
            }
        }
    )
    monkeypatch.setattr(django_queue, "queues", handler)
    queue = handler["requests"]
    terminated = threading.Event()
    snapshots = []
    subscription = queue_observer(
        "requests",
        lambda entry: (
            snapshots.append(entry),
            terminated.set() if entry.status is QueueEntryStatus.TERMINATED else None,
        ),
    )
    try:

        async def complete_entry():
            entry_id = await queue.aenqueue([])

            async def handle(entry):
                return "done"

            await _run_until_terminal(queue, entry_id, handle)
            await queue.aclose()
            return entry_id

        entry_id = asyncio.run(complete_entry())
        time.sleep(0.05)
        queue.prune(entry_id)

        assert terminated.wait(1)
        assert snapshots[-1].status is QueueEntryStatus.TERMINATED
        assert snapshots[-1].payload == []
    finally:
        subscription.unsubscribe()
        queue_runtime.stop_one("requests")
        _discard_observers_for("requests")
