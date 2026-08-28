import asyncio

import pytest

from django_queue.backends import MemoryAsyncQueue, MemoryNotificationQueue
from django_queue.backends.exceptions import QueueEntryNotFoundError
from django_queue.backends.memory import MemoryNotificationQueueWorker
from django_queue.backends.redis import (
    RedisNotificationQueue,
    RedisNotificationQueueWorker,
)
from django_queue.listeners import ListenerRegistration, reset_listeners
from tests.helpers import FixedClock


@pytest.fixture(autouse=True)
def clear_registered_listeners():
    reset_listeners()
    yield
    reset_listeners()


def test_notification_worker_rejects_an_async_queue():
    with pytest.raises(
        TypeError, match="NotificationQueueWorker requires a NotificationQueue"
    ):
        MemoryNotificationQueueWorker(MemoryAsyncQueue())


def test_notification_worker_invokes_every_eligible_listener(monkeypatch):
    queue = MemoryNotificationQueue(queue_name="notices")
    received = []

    async def first(entry):
        received.append(("first", entry.payload))
        return True

    async def second(entry):
        received.append(("second", entry.payload))
        return False

    monkeypatch.setattr(
        "django_queue.notification_worker.listeners_for",
        lambda queue_name: (ListenerRegistration(first), ListenerRegistration(second)),
    )

    async def exercise():
        await queue.aenqueue("notice")
        assert await MemoryNotificationQueueWorker(queue).adispatch_once()
        assert await queue.ahas_pending()
        await queue.aclose()

    asyncio.run(exercise())
    assert received == [("first", "notice"), ("second", "notice")]


def test_notification_worker_skips_filters_and_continues(monkeypatch):
    queue = MemoryNotificationQueue(queue_name="notices")
    visited = []

    def filtered(entry):
        visited.append("filtered")
        return True

    def other(entry):
        visited.append("other")
        return True

    monkeypatch.setattr(
        "django_queue.notification_worker.listeners_for",
        lambda queue_name: (
            ListenerRegistration(filtered, filter=lambda entry: False),
            ListenerRegistration(other),
        ),
    )

    async def exercise():
        await queue.aenqueue("notice")
        assert await MemoryNotificationQueueWorker(queue).adispatch_once()
        await queue.aclose()

    asyncio.run(exercise())
    assert visited == ["other"]


def test_notification_worker_logs_listener_exceptions_and_continues(
    monkeypatch, caplog
):
    queue = MemoryNotificationQueue(queue_name="notices")
    received = []

    def boom(entry):
        raise RuntimeError("listener failed")

    def ok(entry):
        received.append(entry.payload)

    monkeypatch.setattr(
        "django_queue.notification_worker.listeners_for",
        lambda queue_name: (ListenerRegistration(boom), ListenerRegistration(ok)),
    )

    async def exercise():
        await queue.aenqueue("notice")
        assert await MemoryNotificationQueueWorker(queue).adispatch_once()
        await queue.aclose()

    asyncio.run(exercise())
    assert received == ["notice"]
    assert "Notification listener failed" in caplog.text


def test_two_redis_receivers_both_see_a_payload(monkeypatch, redis_client):
    from uuid import uuid4

    name = f"notices-{uuid4().hex}"
    first = RedisNotificationQueue(redis_client, queue_name=name)
    second = RedisNotificationQueue(redis_client, queue_name=name)
    received = []

    async def receive(entry):
        received.append(entry.payload)

    monkeypatch.setattr(
        "django_queue.notification_worker.listeners_for",
        lambda alias: (ListenerRegistration(receive),) if alias == name else (),
    )

    async def exercise():
        worker_one = RedisNotificationQueueWorker(first, alias=name, idle_delay=0.05)
        worker_two = RedisNotificationQueueWorker(second, alias=name, idle_delay=0.05)
        try:
            await worker_one._ensure_subscribed()
            await worker_two._ensure_subscribed()
            await first.aenqueue("shared")
            seen = []
            for _ in range(20):
                if await worker_one.adispatch_once():
                    seen.append("one")
                if await worker_two.adispatch_once():
                    seen.append("two")
                if len(received) >= 2:
                    break
            assert set(seen) >= {"one", "two"}
        finally:
            await worker_one._aclose_pubsub()
            await worker_two._aclose_pubsub()
            await first.aclose()
            await second.aclose()

    asyncio.run(exercise())
    assert received == ["shared", "shared"]


def test_redis_notification_expires_while_idle(redis_client):
    from uuid import uuid4

    name = f"notices-{uuid4().hex}"
    queue = RedisNotificationQueue(redis_client, queue_name=name)

    async def exercise():
        entry_id = await queue.aenqueue("notice", timeout_seconds=0.2)
        worker = RedisNotificationQueueWorker(queue, idle_delay=0.05)
        try:
            await worker._ensure_subscribed()
            deadline = asyncio.get_running_loop().time() + 1.5
            while asyncio.get_running_loop().time() < deadline:
                await worker.adispatch_once()
                try:
                    await queue.afind(entry_id)
                except QueueEntryNotFoundError:
                    return
                await asyncio.sleep(0.05)
            raise AssertionError("stored notification was not expired while idle")
        finally:
            await worker._aclose_pubsub()
            await queue.aclose()

    asyncio.run(exercise())


def test_memory_notification_worker_expires_without_a_later_enqueue(monkeypatch):
    clock = FixedClock()
    queue = MemoryNotificationQueue(queue_name="notices", clock=clock)
    monkeypatch.setattr(
        "django_queue.notification_worker.listeners_for",
        lambda queue_name: (),
    )

    async def exercise():
        entry_id = await queue.aenqueue("notice", timeout_seconds=1)
        clock.timestamp = clock.timestamp + 2
        await MemoryNotificationQueueWorker(queue).adispatch_once()
        with pytest.raises(QueueEntryNotFoundError):
            await queue.afind(entry_id)
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_subscribe_failure_closes_the_client(monkeypatch):
    closed: list[str] = []

    class PubSub:
        async def subscribe(self, channel):
            raise ConnectionError("subscribe failed")

        async def aclose(self):
            closed.append("pubsub")

    class Client:
        def pubsub(self, ignore_subscribe_messages=True):
            return PubSub()

    queue = RedisNotificationQueue("redis://localhost:6379/0", queue_name="notices")

    async def prepare(client):
        return Client()

    async def aclose_client(client):
        closed.append("client")

    monkeypatch.setattr(queue._provider, "_create_async_client", lambda: object())
    monkeypatch.setattr(queue._provider, "_prepare_async_client", prepare)
    monkeypatch.setattr(queue._provider, "_aclose_client", aclose_client)

    async def exercise():
        worker = RedisNotificationQueueWorker(queue)
        with pytest.raises(ConnectionError, match="subscribe failed"):
            await worker._ensure_subscribed()
        assert worker._pubsub is None
        assert worker._pubsub_client is None
        assert closed == ["pubsub", "client"]
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_cancelled_subscribe_closes_the_client(monkeypatch):
    closed: list[str] = []

    class PubSub:
        async def subscribe(self, channel):
            raise asyncio.CancelledError

        async def aclose(self):
            closed.append("pubsub")

    class Client:
        def pubsub(self, ignore_subscribe_messages=True):
            return PubSub()

    queue = RedisNotificationQueue("redis://localhost:6379/0", queue_name="notices")

    async def prepare(client):
        return Client()

    async def aclose_client(client):
        closed.append("client")

    monkeypatch.setattr(queue._provider, "_create_async_client", lambda: object())
    monkeypatch.setattr(queue._provider, "_prepare_async_client", prepare)
    monkeypatch.setattr(queue._provider, "_aclose_client", aclose_client)

    async def exercise():
        worker = RedisNotificationQueueWorker(queue)
        with pytest.raises(asyncio.CancelledError):
            await worker._ensure_subscribed()
        assert worker._pubsub is None
        assert worker._pubsub_client is None
        assert closed == ["pubsub", "client"]
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_cancelled_subscribe_closes_the_client_when_pubsub_aclose_is_cancelled(
    monkeypatch,
):
    closed: list[str] = []

    class PubSub:
        async def subscribe(self, channel):
            raise asyncio.CancelledError

        async def aclose(self):
            closed.append("pubsub")
            raise asyncio.CancelledError

    class Client:
        def pubsub(self, ignore_subscribe_messages=True):
            return PubSub()

    queue = RedisNotificationQueue("redis://localhost:6379/0", queue_name="notices")

    async def prepare(client):
        return Client()

    async def aclose_client(client):
        closed.append("client")

    monkeypatch.setattr(queue._provider, "_create_async_client", lambda: object())
    monkeypatch.setattr(queue._provider, "_prepare_async_client", prepare)
    monkeypatch.setattr(queue._provider, "_aclose_client", aclose_client)

    async def exercise():
        worker = RedisNotificationQueueWorker(queue)
        with pytest.raises(asyncio.CancelledError):
            await worker._ensure_subscribed()
        assert worker._pubsub is None
        assert worker._pubsub_client is None
        assert closed == ["pubsub", "client"]
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_subscribe_close_errors_do_not_hide_subscribe_failure(
    monkeypatch,
):
    closed: list[str] = []

    class PubSub:
        async def subscribe(self, channel):
            raise ConnectionError("subscribe failed")

        async def aclose(self):
            closed.append("pubsub")
            raise RuntimeError("pubsub close failed")

    class Client:
        def pubsub(self, ignore_subscribe_messages=True):
            return PubSub()

    queue = RedisNotificationQueue("redis://localhost:6379/0", queue_name="notices")

    async def prepare(client):
        return Client()

    async def aclose_client(client):
        closed.append("client")
        raise RuntimeError("client close failed")

    monkeypatch.setattr(queue._provider, "_create_async_client", lambda: object())
    monkeypatch.setattr(queue._provider, "_prepare_async_client", prepare)
    monkeypatch.setattr(queue._provider, "_aclose_client", aclose_client)

    async def exercise():
        worker = RedisNotificationQueueWorker(queue)
        with pytest.raises(ConnectionError, match="subscribe failed"):
            await worker._ensure_subscribed()
        assert worker._pubsub is None
        assert worker._pubsub_client is None
        assert closed == ["pubsub", "client"]
        await queue.aclose()

    asyncio.run(exercise())


def test_redis_notification_pubsub_aclose_failure_still_closes_the_client(monkeypatch):
    closed: list[str] = []

    class PubSub:
        async def aclose(self):
            raise RuntimeError("pubsub close failed")

    class Client:
        pass

    queue = RedisNotificationQueue("redis://localhost:6379/0", queue_name="notices")

    async def aclose_client(client):
        closed.append("client")

    monkeypatch.setattr(queue._provider, "_aclose_client", aclose_client)

    async def exercise():
        worker = RedisNotificationQueueWorker(queue)
        worker._pubsub = PubSub()
        worker._pubsub_client = Client()
        with pytest.raises(RuntimeError, match="pubsub close failed"):
            await worker._aclose_pubsub()
        assert worker._pubsub is None
        assert worker._pubsub_client is None
        assert closed == ["client"]
        await queue.aclose()

    asyncio.run(exercise())
