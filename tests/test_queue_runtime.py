import asyncio
import time
from typing import ClassVar
from uuid import UUID

import pytest

import django_queue
from django_queue.apps import DjangoQueueConfig
from django_queue.backends import MemoryEventQueue
from django_queue.backends.exceptions import InvalidQueueBackendError
from django_queue.backends.memory import MemoryEventQueueWorker
from django_queue.listeners import ListenerRegistration
from django_queue.queue_runtime import QueueRuntime


class ClosingEventQueue(MemoryEventQueue):
    closed = 0

    async def aclose(self) -> None:
        type(self).closed += 1


class FlakyEventWorker(MemoryEventQueueWorker):
    runs = 0
    worker_ids: ClassVar[list[UUID]] = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        type(self).worker_ids.append(self._worker_id)

    async def run(self) -> None:
        type(self).runs += 1
        if type(self).runs == 1:
            raise OSError("temporary backend failure")
        await asyncio.Event().wait()


def test_ready_starts_the_thread_once_when_queues_are_configured(monkeypatch):
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
            },
            "tasks": {
                "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                "LOCATION": "",
            },
        }
    )
    started_thread = []
    started = []

    monkeypatch.setattr("django_queue.initialise_queues", lambda: configured)
    monkeypatch.setattr(
        "django_queue.queue_runtime.queue_runtime.start_thread",
        lambda: started_thread.append(True),
    )
    monkeypatch.setattr(
        "django_queue.queue_runtime.queue_runtime.start",
        lambda queues: started.append(queues),
    )

    DjangoQueueConfig("django_queue", django_queue).ready()

    assert started_thread == [True]
    assert started == [configured]


def test_ready_does_not_start_the_thread_when_queues_is_empty(monkeypatch):
    configured = django_queue.QueueRegistry({})
    started_thread = []

    monkeypatch.setattr("django_queue.initialise_queues", lambda: configured)
    monkeypatch.setattr(
        "django_queue.queue_runtime.queue_runtime.start_thread",
        lambda: started_thread.append(True),
    )

    DjangoQueueConfig("django_queue", django_queue).ready()

    assert started_thread == []


def test_task_only_configuration_does_not_start_an_event_loop():
    runtime = QueueRuntime()
    configured = django_queue.QueueRegistry(
        {
            "tasks": {
                "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                "LOCATION": "",
            }
        }
    )

    runtime.start(configured)

    assert runtime._thread is None


def test_runtime_dispatches_an_event_on_its_single_background_loop(monkeypatch):
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
            }
        }
    )
    received = []

    async def receive(entry):
        received.append(entry.payload)
        return True

    monkeypatch.setattr(
        "django_queue.event_worker.listeners_for",
        lambda queue_name: (ListenerRegistration(receive),),
    )
    runtime = QueueRuntime()
    try:
        runtime.start_thread()
        runtime.start(configured)
        configured["events"].enqueue("event")
        deadline = time.monotonic() + 1
        while not received and time.monotonic() < deadline:
            time.sleep(0.01)
        assert received == ["event"]
    finally:
        runtime.shutdown()


def test_runtime_looks_up_listeners_by_configured_alias(monkeypatch):
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
            }
        }
    )
    received = []

    async def receive(entry):
        received.append(entry.payload)
        return True

    monkeypatch.setattr(
        "django_queue.event_worker.listeners_for",
        lambda alias: (ListenerRegistration(receive),) if alias == "events" else (),
    )
    runtime = QueueRuntime()
    try:
        runtime.start_thread()
        runtime.start(configured)
        configured["events"].enqueue("event")
        deadline = time.monotonic() + 1
        while not received and time.monotonic() < deadline:
            time.sleep(0.01)
        assert received == ["event"]
    finally:
        runtime.shutdown()


def test_runtime_reuses_one_loop_and_starts_one_worker_per_event_queue():
    runtime = QueueRuntime()
    configured = django_queue.QueueRegistry(
        {
            "first": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
            },
            "second": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
            },
        }
    )
    try:
        runtime.start_thread()
        first_thread = runtime._thread
        runtime.start(configured)
        runtime.start(configured)
        deadline = time.monotonic() + 1
        while len(runtime._tasks) != 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert runtime._thread is first_thread
        assert set(runtime._tasks) == {"first", "second"}
    finally:
        runtime.shutdown()


def test_event_queue_rejects_task_handler_metadata():
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
                "HANDLER": "tests.test_queue_runtime.receive",
            }
        }
    )

    with pytest.raises(InvalidQueueBackendError, match="event queues"):
        configured["events"]


def test_notification_queue_rejects_task_handler_metadata():
    configured = django_queue.QueueRegistry(
        {
            "notices": {
                "BACKEND": "django_queue.backends.MemoryNotificationQueue",
                "LOCATION": "",
                "HANDLER": "tests.test_queue_runtime.receive",
            }
        }
    )

    with pytest.raises(InvalidQueueBackendError, match="notification queues"):
        configured["notices"]


def test_event_queue_uses_its_configured_event_worker_class():
    created = []

    class TrackingEventWorker(MemoryEventQueueWorker):
        def __init__(self, *args, **kwargs):
            created.append(self)
            super().__init__(*args, **kwargs)

    runtime = QueueRuntime()
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
                "WORKER": TrackingEventWorker,
            }
        }
    )
    try:
        runtime.start_thread()
        runtime.start(configured)
        deadline = time.monotonic() + 1
        while not created and time.monotonic() < deadline:
            time.sleep(0.01)
        assert isinstance(created[0], TrackingEventWorker)
    finally:
        runtime.shutdown()


def test_runtime_closes_event_queue_resources_on_shutdown():
    ClosingEventQueue.closed = 0
    runtime = QueueRuntime()
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "tests.test_queue_runtime.ClosingEventQueue",
                "LOCATION": "",
            }
        }
    )
    try:
        runtime.start_thread()
        runtime.start(configured)
        deadline = time.monotonic() + 1
        while not runtime._tasks and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        runtime.shutdown()

    assert ClosingEventQueue.closed == 1


def test_runtime_restarts_a_worker_after_an_infrastructure_failure(caplog):
    FlakyEventWorker.runs = 0
    FlakyEventWorker.worker_ids = []
    runtime = QueueRuntime()
    runtime.restart_initial_delay = 0.001
    runtime.restart_max_delay = 0.001
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
                "WORKER": FlakyEventWorker,
            }
        }
    )
    try:
        runtime.start_thread()
        runtime.start(configured)
        deadline = time.monotonic() + 1
        while FlakyEventWorker.runs < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert FlakyEventWorker.runs == 2
        assert len(FlakyEventWorker.worker_ids) == 2
        assert FlakyEventWorker.worker_ids[0] == FlakyEventWorker.worker_ids[1]
    finally:
        runtime.shutdown()

    assert "Event worker stopped unexpectedly" in caplog.text


def test_runtime_hosts_a_worker_and_a_receiver_concurrently(monkeypatch, redis_client):
    """One QueueRuntime instance can host both task kinds on its one loop:
    an EventQueue worker and an AsyncQueue observer receiver, for two
    separately configured aliases, without interfering with each other.
    """
    from django_queue import queue_observer
    from django_queue.observers import _discard_observers_for

    _discard_observers_for("observed")
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
            },
            "observed": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": redis_client,
            },
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)

    received_event = []

    async def receive(entry):
        received_event.append(entry.payload)
        return True

    monkeypatch.setattr(
        "django_queue.event_worker.listeners_for",
        lambda alias: (ListenerRegistration(receive),) if alias == "events" else (),
    )

    subscription = queue_observer("observed", lambda entry: None)
    runtime = QueueRuntime()
    try:
        runtime.start_thread()
        runtime.start(configured)
        configured["events"].enqueue("event")

        deadline = time.monotonic() + 1
        while (
            not received_event or set(runtime._tasks) != {"events", "observed"}
        ) and time.monotonic() < deadline:
            time.sleep(0.01)

        assert received_event == ["event"]
        assert set(runtime._tasks) == {"events", "observed"}
    finally:
        subscription.unsubscribe()
        runtime.shutdown()
        _discard_observers_for("observed")


def test_runtime_hosts_event_observer_and_notification_concurrently(
    monkeypatch, redis_client
):
    from django_queue import queue_observer
    from django_queue.observers import _discard_observers_for

    _discard_observers_for("observed")
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
            },
            "notices": {
                "BACKEND": "django_queue.backends.MemoryNotificationQueue",
                "LOCATION": "",
            },
            "observed": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": redis_client,
            },
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)

    received_event = []
    received_notice = []

    async def receive_event(entry):
        received_event.append(entry.payload)
        return True

    async def receive_notice(entry):
        received_notice.append(entry.payload)

    monkeypatch.setattr(
        "django_queue.event_worker.listeners_for",
        lambda alias: (
            (ListenerRegistration(receive_event),) if alias == "events" else ()
        ),
    )
    monkeypatch.setattr(
        "django_queue.notification_worker.listeners_for",
        lambda alias: (
            (ListenerRegistration(receive_notice),) if alias == "notices" else ()
        ),
    )

    subscription = queue_observer("observed", lambda entry: None)
    runtime = QueueRuntime()
    try:
        runtime.start_thread()
        runtime.start(configured)
        configured["events"].enqueue("event")
        configured["notices"].enqueue("notice")

        deadline = time.monotonic() + 1
        while (
            not received_event
            or not received_notice
            or set(runtime._tasks) != {"events", "notices", "observed"}
        ) and time.monotonic() < deadline:
            time.sleep(0.01)

        assert received_event == ["event"]
        assert received_notice == ["notice"]
        assert set(runtime._tasks) == {"events", "notices", "observed"}
    finally:
        subscription.unsubscribe()
        runtime.shutdown()
        _discard_observers_for("observed")


def test_two_threads_registering_for_one_alias_share_the_receiver(
    redis_client, monkeypatch
):
    """Two threads that both register an observer for the same alias for
    the first time must be served by one runtime-hosted receiver -- no
    second backend connection.
    """
    import threading

    from django_queue import queue_observer
    from django_queue.observers import _discard_observers_for
    from django_queue.queue_runtime import queue_runtime

    _discard_observers_for("shared")
    configured = django_queue.QueueRegistry(
        {
            "shared": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": redis_client,
            }
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)
    queue_runtime.start_thread()

    subscriptions = []
    errors = []

    def register():
        try:
            subscriptions.append(queue_observer("shared", lambda entry: None))
        except Exception as exc:  # noqa: BLE001 - surfaced to the test thread below
            errors.append(exc)

    try:
        first = threading.Thread(target=register)
        second = threading.Thread(target=register)
        first.start()
        second.start()
        first.join()
        second.join()

        assert not errors
        assert len(subscriptions) == 2

        deadline = time.monotonic() + 1
        while "shared" not in queue_runtime._tasks and time.monotonic() < deadline:
            time.sleep(0.01)

        # One receiver task for the alias, regardless of how many
        # registrations (from however many threads) triggered it.
        assert "shared" in queue_runtime._tasks
    finally:
        for subscription in subscriptions:
            subscription.unsubscribe()
        queue_runtime.stop_one("shared")
        _discard_observers_for("shared")


def test_event_queue_rejects_a_task_worker_class():
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
                "WORKER": "django_queue.worker.AsyncQueueWorker",
            }
        }
    )

    with pytest.raises(InvalidQueueBackendError, match="EventQueueWorker"):
        configured["events"]
