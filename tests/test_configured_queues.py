import asyncio
import threading
import time

import pytest
from django.core.exceptions import ImproperlyConfigured

import django_queue
from django_queue.apps import DjangoQueueConfig
from django_queue.backends import (
    InvalidQueueBackendError,
    MemoryAsyncQueue,
    MemoryEventQueue,
)
from django_queue.backends.exceptions import QueueException
from django_queue.entries import QueueEntry
from django_queue.event_worker import EventQueueWorker
from django_queue.worker import AsyncQueueWorker


class AttributeErrorBackend:
    def __init__(self, location, options):
        raise AttributeError("invalid location")


class ValueErrorBackend:
    def __init__(self, location, options):
        raise ValueError("invalid option")


class HandlerMetadataBackend(MemoryAsyncQueue):
    def __init__(self, location, options):
        assert "HANDLER" not in options
        assert "WORKER" not in options
        assert "ENTRY_CLASS" not in options
        assert "entry_class" not in options
        super().__init__(location, options)


class ClosingMemoryAsyncQueue(MemoryAsyncQueue):
    closed = 0

    async def aclose(self):
        type(self).closed += 1


class FailingClosingMemoryAsyncQueue(MemoryAsyncQueue):
    async def aclose(self):
        raise ConnectionError("queue close failed")


class RecordingClosingMemoryAsyncQueue(MemoryAsyncQueue):
    closed = 0

    async def aclose(self):
        type(self).closed += 1


class TrackingWorker(AsyncQueueWorker):
    instances = 0

    def __init__(self, *args, **kwargs):
        type(self).instances += 1
        super().__init__(*args, **kwargs)


class TrackingEntry(QueueEntry):
    instances = 0

    def __post_init__(self):
        type(self).instances += 1
        super().__post_init__()


async def no_op_handler(entry):
    return None


@pytest.fixture(autouse=True)
def reset_tracking_extension_instances():
    TrackingWorker.instances = 0
    TrackingEntry.instances = 0
    yield
    TrackingWorker.instances = 0
    TrackingEntry.instances = 0


def _wait_for_task(runtime, alias, timeout=1.0):
    """Poll until `runtime._tasks[alias]` exists, since scheduling a task
    onto the runtime's loop (`call_soon_threadsafe`) is asynchronous with
    respect to the calling thread.
    """
    deadline = time.monotonic() + timeout
    while alias not in runtime._tasks and time.monotonic() < deadline:
        time.sleep(0.001)
    return runtime._tasks.get(alias)


class TestConfiguredQueueInitialization:
    def test_exposes_queue_registry_without_the_superseded_handler_name(self):
        assert hasattr(django_queue, "QueueRegistry")
        assert not hasattr(django_queue, "QueueHandler")

    def test_asynchronous_disposal_attempts_later_queues_after_a_failure(self, caplog):
        RecordingClosingMemoryAsyncQueue.closed = 0
        handler = django_queue.QueueRegistry(
            {
                "broken": {
                    "BACKEND": "tests.test_configured_queues.FailingClosingMemoryAsyncQueue",
                    "LOCATION": "",
                },
                "remaining": {
                    "BACKEND": "tests.test_configured_queues.RecordingClosingMemoryAsyncQueue",
                    "LOCATION": "",
                },
            }
        )
        django_queue.initialise_queues(handler)

        asyncio.run(django_queue.aclose_queues(handler))

        assert RecordingClosingMemoryAsyncQueue.closed == 1
        assert "Unable to dispose queue resources" in caplog.text

    def test_asynchronous_disposal_closes_initialised_queues(self):
        ClosingMemoryAsyncQueue.closed = 0
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "tests.test_configured_queues.ClosingMemoryAsyncQueue",
                    "LOCATION": "",
                }
            }
        )
        django_queue.initialise_queues(handler)

        asyncio.run(django_queue.aclose_queues(handler))

        assert ClosingMemoryAsyncQueue.closed == 1

    def test_synchronous_disposal_remains_a_synchronous_callable(self):
        ClosingMemoryAsyncQueue.closed = 0
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "tests.test_configured_queues.ClosingMemoryAsyncQueue",
                    "LOCATION": "",
                }
            }
        )
        django_queue.initialise_queues(handler)

        django_queue.close_queues(handler)

        assert ClosingMemoryAsyncQueue.closed == 1

    def test_synchronous_disposal_rejects_an_obsolete_registry_keyword(self):
        with pytest.raises(TypeError, match="queue_handler"):
            django_queue.close_queues(queue_handler=django_queue.QueueRegistry({}))

    def test_invalid_backend_errors_are_queue_and_django_configuration_errors(self):
        assert issubclass(QueueException, Exception)
        assert issubclass(InvalidQueueBackendError, QueueException)
        assert issubclass(InvalidQueueBackendError, ImproperlyConfigured)

    def test_initialises_each_configured_queue_and_reuses_it_on_repeat(self):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "LOCATION": "",
                    "maxsize": 4,
                },
                "events": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "LOCATION": "",
                },
            }
        )

        django_queue.initialise_queues(handler)
        default = handler["default"]
        events = handler["events"]
        django_queue.initialise_queues(handler)

        assert isinstance(default, MemoryAsyncQueue)
        assert default.capacity == 4
        assert default is handler["default"]
        assert events is handler["events"]

    def test_configured_memory_event_queue_is_shared_across_threads(
        self, no_runtime_startup
    ):
        handler = django_queue.QueueRegistry(
            {
                "events": {
                    "BACKEND": "django_queue.backends.MemoryEventQueue",
                    "LOCATION": "",
                }
            }
        )
        first = handler["events"]
        from_other_thread = []

        thread = threading.Thread(
            target=lambda: from_other_thread.append(handler["events"])
        )
        thread.start()
        thread.join()

        assert isinstance(first, MemoryEventQueue)
        assert from_other_thread == [first]

    def test_create_connection_starts_the_runtime_for_an_event_queue(self):
        """Covers the non-HTTP-process fallback: resolving an alias directly
        through the registry (as a management command or any code path that
        never fires request_started would) must still start the runtime.

        `start_thread()` mirrors what `DjangoQueueConfig.ready()` already
        did once at process start in production; it's idempotent, so
        calling it here is safe regardless of whether an earlier test in
        this process already started the shared thread.
        """
        from django_queue.queue_runtime import queue_runtime

        queue_runtime.start_thread()
        handler = django_queue.QueueRegistry(
            {
                "fallback-events": {
                    "BACKEND": "django_queue.backends.MemoryEventQueue",
                    "LOCATION": "",
                }
            }
        )
        try:
            handler["fallback-events"]

            assert _wait_for_task(queue_runtime, "fallback-events") is not None
        finally:
            queue_runtime.stop_one("fallback-events")

    def test_create_connection_starts_the_runtime_for_an_observed_async_queue(
        self, redis_client, monkeypatch
    ):
        """Only a backend with a real receiver (Redis) schedules a task here
        -- a memory-backed AsyncQueue's `_observer_receiver` returns None, so
        `_start_receiver` bails out before ever adding a task, regardless of
        registration state.
        """
        from django_queue import queue_observer
        from django_queue.observers import _discard_observers_for
        from django_queue.queue_runtime import queue_runtime

        queue_runtime.start_thread()
        handler = django_queue.QueueRegistry(
            {
                "fallback-observed": {
                    "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                    "LOCATION": redis_client,
                }
            }
        )
        # queue_observer resolves through the module-level django_queue.queues,
        # not any local registry a test constructs.
        monkeypatch.setattr(django_queue, "queues", handler)
        subscription = None
        try:
            handler["fallback-observed"]
            assert "fallback-observed" not in queue_runtime._tasks

            subscription = queue_observer("fallback-observed", lambda entry: None)

            assert _wait_for_task(queue_runtime, "fallback-observed") is not None
        finally:
            if subscription is not None:
                subscription.unsubscribe()
            queue_runtime.stop_one("fallback-observed")
            _discard_observers_for("fallback-observed")

    def test_decorator_activation_on_first_resolution_builds_the_queue_once(
        self, redis_client, monkeypatch
    ):
        """A decorator-registered observer activating on an alias's first
        resolution must not cause create_connection to build a second queue
        instance for that alias.

        `create_connection` calls `queue_runtime.start_one(alias, queue)`
        before Django's `BaseConnectionHandler.__getitem__` has cached the
        alias. If activation re-resolved the queue via `queues[alias]`
        instead of using the instance already in hand, that lookup would
        recurse into `create_connection` for the same alias while it was
        still uncached, building a second instance and firing `queue_created`
        a second time.
        """
        from django_queue import queue_observer
        from django_queue.observers import _discard_observers_for
        from django_queue.queue_runtime import queue_runtime
        from django_queue.signals import queue_created

        queue_runtime.start_thread()
        handler = django_queue.QueueRegistry(
            {
                "first-resolution": {
                    "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                    "LOCATION": redis_client,
                }
            }
        )
        monkeypatch.setattr(django_queue, "queues", handler)

        created = []

        def on_created(sender, name, instance, **kwargs):
            created.append(instance)

        queue_created.connect(on_created)

        @queue_observer("first-resolution")
        def observe(entry):
            pass

        try:
            # First resolution of this alias, through the same path a
            # production process's first real access would use -- not a
            # pre-resolving fixture, and not a direct call to
            # _activate_pending_for.
            resolved = handler["first-resolution"]

            assert len(created) == 1
            assert created[0] is resolved
        finally:
            queue_created.disconnect(on_created)
            observe._queue_observer_subscription.unsubscribe()
            queue_runtime.stop_one("first-resolution")
            _discard_observers_for("first-resolution")

    def test_create_connection_does_not_double_schedule_a_started_alias(self):
        from django_queue.queue_runtime import queue_runtime

        queue_runtime.start_thread()
        handler = django_queue.QueueRegistry(
            {
                "fallback-once": {
                    "BACKEND": "django_queue.backends.MemoryEventQueue",
                    "LOCATION": "",
                }
            }
        )
        try:
            handler["fallback-once"]
            first_task = _wait_for_task(queue_runtime, "fallback-once")

            # A second resolution of the same alias (e.g. a later
            # request_started firing after create_connection already
            # started it) must reuse the existing task, not schedule
            # another one.
            handler["fallback-once"]
            second_task = queue_runtime._tasks.get("fallback-once")

            assert first_task is not None
            assert first_task is second_task
        finally:
            queue_runtime.stop_one("fallback-once")

    @pytest.mark.parametrize(
        ("settings", "message"),
        [
            ("not a configuration mapping", "QUEUES must be a mapping"),
            ({0: {"BACKEND": "django_queue.backends.MemoryAsyncQueue"}}, "alias 0"),
            ({"": {"BACKEND": "django_queue.backends.MemoryAsyncQueue"}}, "alias ''"),
            ({"requests": {}}, "requests.*BACKEND"),
            ({"requests": {"BACKEND": ""}}, "requests.*BACKEND"),
            ({"requests": {"BACKEND": 42}}, "requests.*BACKEND"),
            ({"requests": "not a configuration"}, "requests.*mapping"),
        ],
        ids=[
            "non-mapping-root",
            "non-string-alias",
            "empty-alias",
            "missing-backend",
            "empty-backend",
            "non-string-backend",
            "non-mapping-options",
        ],
    )
    def test_rejects_invalid_queue_configuration(self, settings, message):
        handler = django_queue.QueueRegistry(settings)

        with pytest.raises(InvalidQueueBackendError, match=message):
            django_queue.initialise_queues(handler)

    def test_invalid_settings_message_uses_the_handler_settings_name(self):
        class CustomQueueRegistry(django_queue.QueueRegistry):
            settings_name = "CUSTOM_QUEUES"

        handler = CustomQueueRegistry("not a configuration mapping")

        with pytest.raises(
            InvalidQueueBackendError, match="CUSTOM_QUEUES must be a mapping"
        ):
            django_queue.initialise_queues(handler)

    @pytest.mark.parametrize(
        "alias", ["email:outbound", "email.queue", "email{", "email}"]
    )
    def test_rejects_queue_aliases_that_are_not_key_namespace_segments(self, alias):
        handler = django_queue.QueueRegistry(
            {alias: {"BACKEND": "django_queue.backends.MemoryAsyncQueue"}}
        )

        with pytest.raises(InvalidQueueBackendError, match="letters, digits, _, or -"):
            django_queue.initialise_queues(handler)

    @pytest.mark.parametrize(
        ("backend", "message"),
        [
            (
                "tests.test_configured_queues.AttributeErrorBackend",
                "default.*invalid location",
            ),
            (
                "tests.test_configured_queues.ValueErrorBackend",
                "default.*invalid option",
            ),
        ],
        ids=["attribute-error", "value-error"],
    )
    def test_wraps_backend_configuration_errors_with_the_queue_alias(
        self, backend, message
    ):
        handler = django_queue.QueueRegistry({"default": {"BACKEND": backend}})

        with pytest.raises(InvalidQueueBackendError, match=message):
            django_queue.initialise_queues(handler)

    def test_app_ready_initializes_the_configured_registry(
        self, monkeypatch, no_runtime_startup
    ):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "LOCATION": "",
                }
            }
        )
        monkeypatch.setattr(django_queue, "queues", handler)
        config = DjangoQueueConfig("django_queue", django_queue)

        config.ready()

        assert isinstance(handler["default"], MemoryAsyncQueue)

    def test_preserves_handler_metadata_without_passing_it_to_the_backend(self):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "tests.test_configured_queues.HandlerMetadataBackend",
                    "HANDLER": "tests.test_runqueues.handle_entry",
                    "ENTRY_CLASS": TrackingEntry,
                    "LOCATION": "",
                }
            }
        )

        django_queue.initialise_queues(handler)

        assert isinstance(handler["default"], HandlerMetadataBackend)
        assert (
            handler.settings["default"]["HANDLER"]
            == "tests.test_runqueues.handle_entry"
        )
        assert handler["default"].entry_class is TrackingEntry

    @pytest.mark.parametrize(
        ("worker", "entry_class"),
        [
            (TrackingWorker, TrackingEntry),
            (
                "tests.test_configured_queues.TrackingWorker",
                "tests.test_configured_queues.TrackingEntry",
            ),
        ],
        ids=["class-objects", "dotted-paths"],
    )
    def test_preserves_worker_extension_until_queue_activation(
        self, worker, entry_class
    ):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "tests.test_configured_queues.HandlerMetadataBackend",
                    "WORKER": worker,
                    "ENTRY_CLASS": entry_class,
                    "LOCATION": "",
                }
            }
        )

        django_queue.initialise_queues(handler)

        assert handler.settings["default"]["WORKER"] is worker
        assert handler.settings["default"]["ENTRY_CLASS"] is entry_class
        assert handler["default"].worker_class is worker
        handler["default"].create_worker("default", no_op_handler)
        assert TrackingWorker.instances == 1
        assert handler["default"].entry_class is TrackingEntry
        assert TrackingEntry.instances == 0

    def test_passes_the_configured_entry_class_to_a_redis_event_provider(
        self, no_runtime_startup
    ):
        handler = django_queue.QueueRegistry(
            {
                "events": {
                    "BACKEND": "django_queue.backends.redis.RedisEventQueue",
                    "LOCATION": "redis://localhost:6379/0",
                    "ENTRY_CLASS": TrackingEntry,
                }
            }
        )

        django_queue.initialise_queues(handler)

        queue = handler["events"]
        assert queue.entry_class is TrackingEntry

    def test_rejects_an_event_worker_configured_for_an_async_queue(self):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "LOCATION": "",
                    "WORKER": EventQueueWorker,
                }
            }
        )

        with pytest.raises(InvalidQueueBackendError, match="default.*WORKER"):
            django_queue.initialise_queues(handler)

    @pytest.mark.parametrize(
        ("setting", "value", "message"),
        [
            ("WORKER", "tests.test_configured_queues.TrackingEntry", "WORKER"),
            (
                "ENTRY_CLASS",
                "tests.test_configured_queues.TrackingWorker",
                "ENTRY_CLASS",
            ),
            ("WORKER", "tests.test_configured_queues.UnknownWorker", "WORKER"),
            ("ENTRY_CLASS", "tests.test_configured_queues.UnknownEntry", "ENTRY_CLASS"),
        ],
        ids=[
            "entry-as-worker",
            "worker-as-entry",
            "missing-worker-path",
            "missing-entry-path",
        ],
    )
    def test_rejects_invalid_queue_type_extensions(self, setting, value, message):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    setting: value,
                }
            }
        )

        with pytest.raises(InvalidQueueBackendError, match=f"default.*{message}"):
            django_queue.initialise_queues(handler)

    @pytest.mark.parametrize(
        "budget", [0, -1, "30", True, None, float("nan"), float("inf")]
    )
    def test_rejects_an_invalid_queue_timeout(self, budget):
        """A bad budget fails at settings initialisation, not at first dispatch."""
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "TIMEOUT": budget,
                }
            }
        )

        with pytest.raises(InvalidQueueBackendError, match="default.*TIMEOUT"):
            django_queue.initialise_queues(handler)

    def test_accepts_a_positive_queue_timeout(self):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "TIMEOUT": 30,
                }
            }
        )

        django_queue.initialise_queues(handler)

        assert handler["default"].timeout_seconds == 30

    def test_defaults_terminal_entry_retention_to_ten_minutes(self):
        handler = django_queue.QueueRegistry(
            {"default": {"BACKEND": "django_queue.backends.MemoryAsyncQueue"}}
        )

        django_queue.initialise_queues(handler)

        assert handler["default"].retention_timeout == 600

    def test_allows_explicit_terminal_entry_retention_opt_out(self):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "RETENTION_TIMEOUT": None,
                }
            }
        )

        django_queue.initialise_queues(handler)

        assert handler["default"].retention_timeout is None

    @pytest.mark.parametrize("retention_timeout", [-1, "600", True, float("nan")])
    def test_rejects_an_invalid_terminal_entry_retention(self, retention_timeout):
        handler = django_queue.QueueRegistry(
            {
                "default": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "RETENTION_TIMEOUT": retention_timeout,
                }
            }
        )

        with pytest.raises(InvalidQueueBackendError, match="RETENTION_TIMEOUT"):
            django_queue.initialise_queues(handler)
