import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from django_queue.backends import MemoryNotificationQueue
from django_queue.backends.base import NotificationQueue
from django_queue.backends.exceptions import QueueEntryNotFoundError
from django_queue.backends.memory import MemoryNotificationQueueWorker
from django_queue.backends.redis import (
    RedisNotificationQueue,
    RedisNotificationQueueWorker,
)
from django_queue.clock import ClockTime
from django_queue.notification_worker import NotificationQueueWorker
from tests.helpers import FIXED_CLOCK_TIME, CustomQueueEntry, FixedClock


def test_configured_notification_alias_resolves(no_runtime_startup):
    import django_queue

    handler = django_queue.QueueRegistry(
        {
            "notices": {
                "BACKEND": "django_queue.backends.MemoryNotificationQueue",
                "LOCATION": "",
            }
        }
    )

    assert isinstance(handler["notices"], MemoryNotificationQueue)


def test_notification_backends_inherit_the_composed_entry_facade():
    assert MemoryNotificationQueue.aenqueue is NotificationQueue.aenqueue
    assert RedisNotificationQueue.aenqueue is NotificationQueue.aenqueue


def test_memory_notification_queue_uses_the_default_notification_worker():
    queue = MemoryNotificationQueue()

    assert queue.resolve_worker("notices") is MemoryNotificationQueueWorker


def test_redis_notification_queue_uses_a_redis_specific_default_worker():
    queue = RedisNotificationQueue("redis://localhost:6379/0")

    assert queue.resolve_worker("notices") is RedisNotificationQueueWorker


def test_redis_notification_queue_rejects_the_generic_notification_worker(redis_url):
    class GenericNotificationWorker(NotificationQueueWorker):
        async def _next(self):
            return None

        async def _expire_due(self):
            return None

    queue = RedisNotificationQueue(redis_url, queue_name="notices")

    with pytest.raises(TypeError, match="requires a redis worker"):
        GenericNotificationWorker(queue)


def test_memory_notification_queue_rejects_a_redis_worker():
    queue = MemoryNotificationQueue(queue_name="notices")

    with pytest.raises(TypeError, match="requires a memory worker"):
        RedisNotificationQueueWorker(queue)


def test_notification_queue_has_no_list_prune_or_consume():
    queue = MemoryNotificationQueue(queue_name="notices")

    assert not hasattr(queue, "list")
    assert not hasattr(queue, "prune")
    with pytest.raises(TypeError, match="does not consume"):
        queue.dequeue()


def test_notification_queue_ignores_priority_and_available_at():
    queue = MemoryNotificationQueue(queue_name="notices")
    entry_id = queue.enqueue(
        "notice",
        priority=9,
        available_at=ClockTime(1_786_032_100),
    )

    entry = queue.find(entry_id)
    assert entry.payload == "notice"
    assert entry.priority == 0


@pytest.mark.parametrize(
    ("entry_timeout", "queue_timeout", "expected_timeout"),
    [(15, 30, 15), (None, 30, 30), (None, None, 60)],
    ids=["entry-override", "queue-default", "built-in-default"],
)
def test_notification_lifetime_resolves_from_entry_then_queue_then_default(
    entry_timeout, queue_timeout, expected_timeout
):
    queue = MemoryNotificationQueue(queue_name="notices")
    queue.timeout_seconds = queue_timeout
    entry_id = queue.enqueue("notice", timeout_seconds=entry_timeout)

    assert queue.find(entry_id).timeout_seconds == expected_timeout


def test_memory_notification_enqueue_is_findable_until_expiry():
    queue = MemoryNotificationQueue(queue_name="notices")
    entry_id = queue.enqueue("notice")

    assert queue.find(entry_id).payload == "notice"
    assert queue.has_pending()


def test_memory_notification_expires_one_due_entry_per_tick():
    clock = FixedClock()
    queue = MemoryNotificationQueue(queue_name="notices", clock=clock)
    first = queue.enqueue("first", timeout_seconds=1)
    second = queue.enqueue("second", timeout_seconds=1)

    async def exercise():
        clock.timestamp = FIXED_CLOCK_TIME + 2
        await MemoryNotificationQueueWorker(queue).adispatch_once()
        with pytest.raises(QueueEntryNotFoundError):
            await queue.afind(first)
        assert (await queue.afind(second)).payload == "second"
        await MemoryNotificationQueueWorker(queue).adispatch_once()
        with pytest.raises(QueueEntryNotFoundError):
            await queue.afind(second)
        await queue.aclose()

    asyncio.run(exercise())


def test_memory_notification_expiry_drains_all_due_entries():
    clock = FixedClock()
    queue = MemoryNotificationQueue(queue_name="notices", clock=clock)
    for index in range(5):
        queue.enqueue(f"notice-{index}", timeout_seconds=1)

    async def exercise():
        clock.timestamp = FIXED_CLOCK_TIME + 2
        worker = MemoryNotificationQueueWorker(queue)
        for _ in range(5):
            await worker.adispatch_once()
        assert queue._provider._notification_entries == {}
        await queue.aclose()

    asyncio.run(exercise())


def test_memory_notification_expires_after_sender_lifetime():
    clock = FixedClock()
    queue = MemoryNotificationQueue(queue_name="notices", clock=clock)
    entry_id = queue.enqueue("notice", timeout_seconds=1)

    async def exercise():
        clock.timestamp = FIXED_CLOCK_TIME + 2
        await MemoryNotificationQueueWorker(queue).adispatch_once()
        with pytest.raises(QueueEntryNotFoundError):
            await queue.afind(entry_id)
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_queue_uses_notification_semantics(redis_client):
    assert isinstance(RedisNotificationQueue(redis_client), NotificationQueue)


def test_redis_notification_read_does_not_set_a_lease(redis_client):
    name = f"notices-{uuid4().hex}"
    queue = RedisNotificationQueue(redis_client, queue_name=name)

    async def exercise():
        entry_id = await queue.aenqueue("notice")
        first = await queue.afind(entry_id)
        second = await queue.afind(entry_id)
        lease_key = queue._provider._notification_lease_key(entry_id)
        lease = await queue._provider._async_redis().get(lease_key)
        await queue.aclose()
        return first.payload, second.payload, lease

    payload, again, lease = asyncio.run(exercise())
    assert payload == again == "notice"
    assert lease is None


def test_redis_notification_expires_one_due_entry_per_tick(redis_client):
    name = f"notices-{uuid4().hex}"
    queue = RedisNotificationQueue(redis_client, queue_name=name)

    async def exercise():
        first = await queue.aenqueue("first", timeout_seconds=0.05)
        second = await queue.aenqueue("second", timeout_seconds=0.05)
        await asyncio.sleep(0.15)
        expired = await queue._provider.aexpire_due_notifications()
        assert len(expired) == 1
        gone = expired[0]
        remaining = second if gone == first else first
        assert gone in {first, second}
        with pytest.raises(QueueEntryNotFoundError):
            await queue.afind(gone)
        assert (await queue.afind(remaining)).payload in {"first", "second"}
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_clear_removes_stored_payloads(redis_client):
    name = f"notices-{uuid4().hex}"
    queue = RedisNotificationQueue(redis_client, queue_name=name)

    async def exercise():
        first = await queue.aenqueue("first")
        second = await queue.aenqueue("second")
        await queue.aclear()
        with pytest.raises(QueueEntryNotFoundError):
            await queue.afind(first)
        with pytest.raises(QueueEntryNotFoundError):
            await queue.afind(second)
        assert not await queue.ahas_pending()
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_read_during_expiry_treats_the_entry_as_unavailable(
    redis_client,
):
    name = f"notices-{uuid4().hex}"
    queue = RedisNotificationQueue(redis_client, queue_name=name)

    async def exercise():
        entry_id = await queue.aenqueue("notice")
        provider = queue._provider
        await provider._async_redis().set(
            provider._notification_lease_key(entry_id), "1", px=5_000
        )
        with pytest.raises(QueueEntryNotFoundError):
            await queue.afind(entry_id)
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_restores_the_configured_entry_class(redis_client):
    queue = RedisNotificationQueue(
        redis_client, entry_class=CustomQueueEntry, queue_name=f"notices-{uuid4().hex}"
    )

    async def exercise():
        entry_id = await queue.aenqueue("notice")
        entry = await queue.afind(entry_id)
        await queue.aclose()
        return entry

    assert isinstance(asyncio.run(exercise()), CustomQueueEntry)


def test_redis_late_receiver_is_not_required_to_catch_up(redis_client):
    name = f"notices-{uuid4().hex}"
    queue = RedisNotificationQueue(redis_client, queue_name=name)
    received = []

    async def receive(entry):
        received.append(entry.payload)

    async def exercise():
        from django_queue.listeners import ListenerRegistration

        await queue.aenqueue("missed")
        worker = RedisNotificationQueueWorker(queue, idle_delay=0.05)
        worker._alias = name
        from django_queue import notification_worker as module

        original = module.listeners_for
        module.listeners_for = lambda alias: (
            (ListenerRegistration(receive),) if alias == name else ()
        )
        try:
            await worker.adispatch_once()
        finally:
            module.listeners_for = original
            await worker._aclose_pubsub()
            await queue.aclose()

    asyncio.run(exercise())
    assert received == []


def test_readme_choose_a_queue_names_all_three_types():
    readme = Path(__file__).resolve().parents[1].joinpath("README.md").read_text()
    section = readme.split("## Choose a queue type", 1)[1].split(
        "## Backend choices", 1
    )[0]

    assert "**Async queue**" in section
    assert "**Event queue**" in section
    assert "**Notification queue**" in section
    assert "fan-out" not in readme
    assert "distributed processing" in section
    assert "see-but-do-not-own" in section or "see-but-do-not-own" in readme
