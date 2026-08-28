import pytest

import django_queue
from django_queue.listeners import listeners_for, queue_listener, reset_listeners


@pytest.fixture(autouse=True)
def clear_registered_listeners():
    reset_listeners()
    yield
    reset_listeners()


def test_decorator_registers_a_listener_for_a_configured_event_queue(
    monkeypatch, no_runtime_startup
):
    configured = django_queue.QueueRegistry(
        {
            "events": {
                "BACKEND": "django_queue.backends.MemoryEventQueue",
                "LOCATION": "",
            }
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)

    @queue_listener("events")
    async def receive(entry):
        return entry.payload == "event"

    assert listeners_for("events")[0].callback is receive


def test_decorator_registers_a_listener_for_a_configured_notification_queue(
    monkeypatch, no_runtime_startup
):
    configured = django_queue.QueueRegistry(
        {
            "notices": {
                "BACKEND": "django_queue.backends.MemoryNotificationQueue",
                "LOCATION": "",
            }
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)

    @queue_listener("notices")
    async def receive(entry):
        return entry.payload == "notice"

    assert listeners_for("notices")[0].callback is receive


def test_decorator_rejects_a_task_queue(monkeypatch, no_runtime_startup):
    configured = django_queue.QueueRegistry(
        {
            "tasks": {
                "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                "LOCATION": "",
            }
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)

    with pytest.raises(TypeError, match="EventQueue or NotificationQueue"):

        @queue_listener("tasks")
        def receive(entry):
            return True
