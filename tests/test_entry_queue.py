import asyncio
import threading
from dataclasses import replace
from uuid import uuid4, uuid7

import pytest

import django_queue
from django_queue import queue_observer
from django_queue.backends import (
    MemoryAsyncPriorityQueue,
    MemoryAsyncQueue,
    MemoryAsyncStack,
    MemoryEventQueue,
)
from django_queue.backends.base import AsyncQueue, BaseQueue, EventQueue
from django_queue.backends.exceptions import (
    QueueEmptyException,
    QueueEntryNotFoundError,
)
from django_queue.backends.redis.redisqueue import RedisAsyncQueue
from django_queue.entries import QueueEntry, QueueEntryStatus
from django_queue.observers import (
    _EVENT_QUEUE_SIZE,
    _activate_pending_for,
    _alias_has_observer_registration,
    _discard_observers_for,
    _observers_by_alias,
    _observers_for,
    _order_snapshots,
    _pending_by_alias,
    _Registration,
)
from django_queue.signals import entry_enqueued
from django_queue.worker import AsyncQueueWorker
from tests.helpers import FIXED_CLOCK_TIME, CustomQueueEntry, FixedClock


# MemoryAsyncPriorityQueue takes its entry methods from MemoryAsyncQueue by class-level
# assignment rather than inheritance, so the borrow list is only as correct as
# the coverage that calls through it. Running the lifecycle against both classes
# catches a method bound to the wrong original, which the abstract base cannot.
@pytest.fixture(
    params=[MemoryAsyncQueue, MemoryAsyncPriorityQueue, MemoryAsyncStack],
    ids=["fifo", "priority", "stack"],
)
def queue(request):
    return request.param(queue_name="requests", clock=FixedClock())


@pytest.fixture(
    params=[MemoryAsyncQueue, MemoryAsyncPriorityQueue, MemoryAsyncStack],
    ids=["fifo", "priority", "stack"],
)
def observer_queue(request, monkeypatch):
    # _observers_for is alias-scoped and process-global, so a prior test's
    # "requests" registration/dispatch state would otherwise leak in here.
    _discard_observers_for("requests")
    handler = django_queue.QueueRegistry(
        {
            "requests": {
                "BACKEND": f"django_queue.backends.{request.param.__name__}",
                "LOCATION": "",
            }
        }
    )
    monkeypatch.setattr(django_queue, "queues", handler)
    yield handler["requests"]
    _discard_observers_for("requests")


async def _event_queue_noop(*args, **kwargs):
    return None


async def _process_one(queue, queue_name="requests"):
    async def handle(entry):
        return entry.payload

    worker = AsyncQueueWorker(
        {queue_name: queue}, {queue_name: handle}, idle_delay=0.001
    )
    entry_id = await queue.aenqueue("work")
    task = asyncio.create_task(worker.run())
    while (await queue.afind(entry_id)).status is not QueueEntryStatus.SUCCEEDED:
        await asyncio.sleep(0.001)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class ObserverEventQueue(EventQueue):
    capacity = 0
    aadd = aget = apoll = apeek = asize = aenqueue = afind = _event_queue_noop
    adequeue = ahas_pending = _event_queue_noop
    _amark_running = _amark_succeeded = _amark_failed = _event_queue_noop
    _amark_cancelled = _amark_timed_out = _event_queue_noop

    def __init__(self, _: str, options: dict) -> None:
        self._queue_name = options.pop("queue_name", "events")


class TestMemoryAsyncQueueEntries:
    def test_async_backends_inherit_the_composed_entry_facade(self):
        assert MemoryAsyncQueue.aenqueue is AsyncQueue.aenqueue
        assert RedisAsyncQueue.aenqueue is AsyncQueue.aenqueue

    def test_lifecycle_transitions_are_not_public_queue_operations(self, queue):
        for name in (
            "mark_running",
            "mark_succeeded",
            "mark_failed",
            "mark_cancelled",
            "mark_timed_out",
            "amark_running",
            "amark_succeeded",
            "amark_failed",
            "amark_cancelled",
            "amark_timed_out",
        ):
            assert not hasattr(queue, name)

    def test_observer_routes_live_snapshots_by_backend_queue_name(
        self, monkeypatch, no_runtime_startup
    ):
        handler = django_queue.QueueRegistry(
            {
                "alias": {
                    "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                    "LOCATION": "",
                    "queue_name": "physical",
                }
            }
        )
        monkeypatch.setattr(django_queue, "queues", handler)
        observed_queue = handler["alias"]
        delivered = threading.Event()

        subscription = queue_observer("alias", lambda entry: delivered.set())
        try:
            asyncio.run(_process_one(observed_queue, "alias"))

            assert delivered.wait(1)
        finally:
            subscription.unsubscribe()
            _discard_observers_for("alias")

    def test_observers_are_sequential_and_isolate_callback_failure(
        self, observer_queue, caplog, no_runtime_startup
    ):
        delivered = threading.Event()
        calls = []

        def fail(entry):
            calls.append(("fail", entry.status))
            raise RuntimeError("expected observer failure")

        def succeed(entry):
            calls.append(("succeed", entry.status))
            if entry.status is QueueEntryStatus.SUCCEEDED:
                delivered.set()

        first = queue_observer("requests", fail)
        second = queue_observer("requests", succeed)
        asyncio.run(_process_one(observer_queue))

        assert delivered.wait(1)
        first.unsubscribe()
        second.unsubscribe()
        assert calls == [
            ("fail", QueueEntryStatus.QUEUED),
            ("succeed", QueueEntryStatus.QUEUED),
            ("fail", QueueEntryStatus.RUNNING),
            ("succeed", QueueEntryStatus.RUNNING),
            ("fail", QueueEntryStatus.SUCCEEDED),
            ("succeed", QueueEntryStatus.SUCCEEDED),
        ]
        assert "Queue lifecycle observer failed" in caplog.text

    def test_observer_receives_worker_lifecycle(
        self, observer_queue, no_runtime_startup
    ):
        statuses = []
        completed = threading.Event()

        def callback(entry):
            statuses.append(entry.status)
            if entry.status is QueueEntryStatus.SUCCEEDED:
                completed.set()

        subscription = queue_observer("requests", callback)

        asyncio.run(_process_one(observer_queue))

        assert completed.wait(1)
        subscription.unsubscribe()
        assert statuses == [
            QueueEntryStatus.QUEUED,
            QueueEntryStatus.RUNNING,
            QueueEntryStatus.SUCCEEDED,
        ]

    def test_observer_dispatches_an_async_callback(
        self, observer_queue, no_runtime_startup
    ):
        statuses = []
        completed = threading.Event()

        async def callback(entry):
            statuses.append(entry.status)
            if entry.status is QueueEntryStatus.SUCCEEDED:
                completed.set()

        subscription = queue_observer("requests", callback)

        asyncio.run(_process_one(observer_queue))

        assert completed.wait(1)
        subscription.unsubscribe()
        assert statuses == [
            QueueEntryStatus.QUEUED,
            QueueEntryStatus.RUNNING,
            QueueEntryStatus.SUCCEEDED,
        ]

    def test_worker_publishes_a_first_seen_terminal_entry(
        self, observer_queue, no_runtime_startup
    ):
        observed = threading.Event()
        statuses = []

        def callback(entry):
            statuses.append(entry.status)
            if entry.status is QueueEntryStatus.FAILED:
                observed.set()

        subscription = queue_observer("requests", callback)
        entry_id = observer_queue.enqueue("work")
        observer_queue._mark_failed(entry_id, ValueError("dispatch unavailable"))

        async def exercise():
            worker = AsyncQueueWorker(
                {"requests": observer_queue},
                {"requests": lambda entry: asyncio.sleep(0)},
                idle_delay=0.001,
            )
            task = asyncio.create_task(worker.run())
            assert await asyncio.to_thread(observed.wait, 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        try:
            asyncio.run(exercise())
        finally:
            subscription.unsubscribe()

        assert statuses == [QueueEntryStatus.FAILED]

    def test_worker_publishes_every_entry_created_between_scans(self, queue):
        observed_ids = []

        async def exercise():
            worker = AsyncQueueWorker(
                {"requests": queue},
                {"requests": lambda entry: asyncio.sleep(0)},
            )

            async def publish(entry):
                observed_ids.append(entry.id)

            queue.apublish = publish
            first_id = await queue.aenqueue("first")
            await worker._publish_new(queue)
            second_id = await queue.aenqueue("second")
            third_id = await queue.aenqueue("third")
            worker._last_first_seen_scan_at[queue] = float("-inf")
            await worker._publish_new(queue)
            return first_id, second_id, third_id

        first_id, second_id, third_id = asyncio.run(exercise())

        assert observed_ids == [first_id, second_id, third_id]

    def test_worker_throttles_first_seen_scans_per_queue(self, queue):
        async def exercise():
            worker = AsyncQueueWorker(
                {"requests": queue},
                {"requests": lambda entry: asyncio.sleep(0)},
            )
            scan_count = 0
            original_list_entries = queue.alist

            async def list_entries():
                nonlocal scan_count
                scan_count += 1
                return await original_list_entries()

            queue.alist = list_entries
            await worker._publish_new(queue)
            await worker._publish_new(queue)
            return scan_count

        assert asyncio.run(exercise()) == 1

    def test_observer_rejects_event_queue(self, monkeypatch, no_runtime_startup):
        handler = django_queue.QueueRegistry(
            {
                "events": {
                    "BACKEND": "tests.test_entry_queue.ObserverEventQueue",
                    "LOCATION": "",
                }
            }
        )
        monkeypatch.setattr(django_queue, "queues", handler)

        with pytest.raises(TypeError, match="AsyncQueue"):
            queue_observer("events", lambda entry: None)

    def test_observer_rejects_notification_queue(self, monkeypatch, no_runtime_startup):
        handler = django_queue.QueueRegistry(
            {
                "notices": {
                    "BACKEND": "django_queue.backends.MemoryNotificationQueue",
                    "LOCATION": "",
                }
            }
        )
        monkeypatch.setattr(django_queue, "queues", handler)

        with pytest.raises(TypeError, match="AsyncQueue"):
            queue_observer("notices", lambda entry: None)

    def test_observer_drops_snapshots_after_its_queue_delivery_queue_is_full(
        self, observer_queue, caplog
    ):
        observers = _observers_for(observer_queue)
        registration = _Registration(lambda entry: None, None, initialising=False)
        with observers.lock:
            observers.registrations.append(registration)
        entry = observer_queue.find(observer_queue.enqueue("work"))

        for _ in range(_EVENT_QUEUE_SIZE + 2):
            observers.publish(entry)

        assert observers.events.qsize() == _EVENT_QUEUE_SIZE
        assert caplog.messages == [
            "Queue lifecycle observer delivery queue is full; dropping snapshots"
        ]

    def test_observers_share_state_across_instances_of_one_alias(self, queue):
        """Two distinct queue objects for one alias must share observer state.

        Simulates what two threads touching the same QUEUES alias get under
        QueueRegistry's default thread-local connection_scope: two separate
        AsyncQueue instances. Registration/dispatch state must be keyed by
        alias, not by instance, or a receiver on one instance never sees a
        registration made through the other.
        """
        _discard_observers_for("requests")
        try:
            backend = type(queue)
            first = backend(queue_name="requests", clock=FixedClock())
            second = backend(queue_name="requests", clock=FixedClock())

            assert _observers_for(first) is _observers_for(second)
        finally:
            _discard_observers_for("requests")

    def test_alias_has_no_registration_before_anyone_subscribes(self, queue):
        _discard_observers_for("requests")
        assert _alias_has_observer_registration("requests") is False

    def test_alias_has_no_registration_created_as_a_side_effect(self, queue):
        """Querying registration state must not itself create a _QueueObservers."""
        _discard_observers_for("requests")
        try:
            _alias_has_observer_registration("requests")
            assert "requests" not in _observers_by_alias
        finally:
            _discard_observers_for("requests")

    def test_discard_observers_stops_a_running_dispatcher_thread(
        self, observer_queue, no_runtime_startup
    ):
        """`_discard_observers_for` must not just forget an alias's
        `_QueueObservers` -- it must stop the dispatcher thread too, or
        that thread runs forever with nothing left able to reach it.
        """
        subscription = queue_observer("requests", lambda entry: None)
        observers = _observers_for(observer_queue)
        dispatcher = observers.dispatcher

        assert dispatcher is not None
        assert dispatcher.is_alive()

        subscription.unsubscribe()
        _discard_observers_for("requests")

        dispatcher.join(timeout=1)
        assert not dispatcher.is_alive()

    def test_alias_has_a_registration_once_subscribed(
        self, observer_queue, no_runtime_startup
    ):
        subscription = queue_observer("requests", lambda entry: None)
        try:
            assert _alias_has_observer_registration("requests") is True
        finally:
            subscription.unsubscribe()

    def test_alias_has_no_registration_after_unsubscribing(
        self, observer_queue, no_runtime_startup
    ):
        subscription = queue_observer("requests", lambda entry: None)
        subscription.unsubscribe()
        assert _alias_has_observer_registration("requests") is False

    def test_decorator_form_records_without_querying_a_backend(self, monkeypatch):
        """Import-time safety: decorating must not touch any queue backend."""
        calls = []

        class TrackedRegistry(django_queue.QueueRegistry):
            def __getitem__(self, alias):
                calls.append(alias)
                return super().__getitem__(alias)

        monkeypatch.setattr(
            django_queue,
            "queues",
            TrackedRegistry({"decorated": {"BACKEND": "not-a-real-backend"}}),
        )
        try:

            @queue_observer("decorated")
            def observe(entry):
                return None

            assert calls == []
            assert _alias_has_observer_registration("decorated") is True
        finally:
            _pending_by_alias.pop("decorated", None)

    def test_decorator_form_returns_the_original_callable_unchanged(self):
        _pending_by_alias.pop("decorated", None)
        try:

            def observe(entry):
                return entry

            decorated = queue_observer("decorated")(observe)

            assert decorated is observe
        finally:
            _pending_by_alias.pop("decorated", None)

    def test_decorator_form_activates_on_runtime_start(
        self, observer_queue, no_runtime_startup
    ):
        delivered = threading.Event()

        @queue_observer("requests")
        def observe(entry):
            delivered.set()

        try:
            _activate_pending_for("requests", observer_queue)
            asyncio.run(_process_one(observer_queue))

            assert delivered.wait(1)
        finally:
            observe._queue_observer_subscription.unsubscribe()

    def test_decorator_form_dispatches_an_async_callback(
        self, observer_queue, no_runtime_startup
    ):
        delivered = threading.Event()

        @queue_observer("requests")
        async def observe(entry):
            delivered.set()

        try:
            _activate_pending_for("requests", observer_queue)
            asyncio.run(_process_one(observer_queue))

            assert delivered.wait(1)
        finally:
            observe._queue_observer_subscription.unsubscribe()

    def test_concurrent_activation_registers_a_pending_observer_once(
        self, observer_queue, no_runtime_startup, monkeypatch
    ):
        """A second activation attempt for an alias already mid-activation
        must not re-register the same pending observer -- that would
        double-register the callback and leak the loser's subscription.

        Holds the first thread inside `_register_now` (after it has already
        set `pending.activating`) via a barrier, then runs a second
        activation attempt concurrently and confirms it no-ops. This proves
        `activating`'s idempotency guard holds under real concurrent access;
        it does not reproduce the narrower unlocked check-then-set window
        the fix closes (that window is 2-3 adjacent bytecodes with no
        hookable call between them, not reliably forceable from a test
        without a test-only seam in production code).
        """
        entered_register_now = threading.Barrier(2, timeout=1)
        release_register_now = threading.Event()
        original_register_now = django_queue.observers._register_now

        def blocking_register_now(
            queue_name, callback, entry_id, configured_queue=None
        ):
            entered_register_now.wait()
            assert release_register_now.wait(timeout=1)
            return original_register_now(
                queue_name, callback, entry_id, configured_queue
            )

        monkeypatch.setattr(
            django_queue.observers, "_register_now", blocking_register_now
        )

        calls = []

        @queue_observer("requests")
        def observe(entry):
            calls.append(entry)

        results = []

        def activate():
            results.append(_activate_pending_for("requests", observer_queue))

        try:
            first = threading.Thread(target=activate)
            first.start()
            entered_register_now.wait(timeout=1)

            # The winner is now blocked inside _register_now, holding no
            # lock. A second activation attempt for the same alias must see
            # `activating` already set and do nothing further.
            second = threading.Thread(target=activate)
            second.start()
            second.join(timeout=1)
            assert not second.is_alive()

            release_register_now.set()
            first.join(timeout=1)
            assert not first.is_alive()

            observers = _observers_for(observer_queue)
            assert len(observers.registrations) == 1
        finally:
            observe._queue_observer_subscription.unsubscribe()

    def test_unsubscribing_before_activation_prevents_it(self, observer_queue):
        @queue_observer("requests")
        def observe(entry):
            raise AssertionError("must not be invoked")

        observe._queue_observer_subscription.unsubscribe()
        _activate_pending_for("requests", observer_queue)

        assert _alias_has_observer_registration("requests") is False
        asyncio.run(_process_one(observer_queue))

    def test_entry_queues_are_async_queue_variants(self, queue):
        assert isinstance(queue, AsyncQueue)

    def test_observer_bootstrap_lists_retained_entry_snapshots(self, queue):
        queued_id = queue.enqueue("queued")
        completed_id = queue.enqueue("completed")
        queue._mark_running(completed_id)
        queue._mark_succeeded(completed_id, "done")

        entries = queue.list()

        assert {entry.id for entry in entries} == {queued_id, completed_id}
        assert {entry.status for entry in entries} == {
            QueueEntryStatus.QUEUED,
            QueueEntryStatus.SUCCEEDED,
        }

    def test_observer_orders_a_terminated_snapshot_during_bootstrap(self, queue):
        entry_id = queue.enqueue("completed")
        queued = queue.find(entry_id)
        running = queue._mark_running(entry_id)
        succeeded = queue._mark_succeeded(entry_id, "done")
        terminated = replace(
            succeeded,
            status=QueueEntryStatus.TERMINATED,
        )

        assert _order_snapshots([terminated, succeeded, queued, running]) == [
            queued,
            running,
            succeeded,
            terminated,
        ]

    def test_synchronous_entry_api_uses_the_asynchronous_implementation(self, queue):
        entry_id = queue.enqueue({"request_id": 42})

        entry = queue.find(entry_id)
        dequeued = queue.dequeue()
        running = queue._mark_running(entry_id)
        completed = queue._mark_succeeded(entry_id, {"ok": True})

        assert entry.id == entry_id
        assert dequeued == entry
        assert running.status is QueueEntryStatus.RUNNING
        assert completed.status is QueueEntryStatus.SUCCEEDED

    def test_asynchronous_entry_api_matches_the_synchronous_surface(self, queue):
        async def exercise():
            entry_id = await queue.aenqueue({"request_id": 42})
            entry = await queue.afind(entry_id)
            dequeued = await queue.adequeue()
            running = await queue._amark_running(entry_id)
            completed = await queue._amark_succeeded(entry_id, {"ok": True})
            return entry_id, entry, dequeued, running, completed

        entry_id, entry, dequeued, running, completed = asyncio.run(exercise())

        assert entry.id == entry_id
        assert dequeued == entry
        assert running.status is QueueEntryStatus.RUNNING
        assert completed.status is QueueEntryStatus.SUCCEEDED

    def test_synchronous_entry_api_refuses_to_run_on_an_event_loop(self, queue):
        async def exercise():
            with pytest.raises(
                RuntimeError, match="just await the async function directly"
            ):
                queue.enqueue("work")

        asyncio.run(exercise())

    def test_enqueue_survives_a_failing_entry_observer(self, queue):
        def failing_receiver(sender, **kwargs):
            raise RuntimeError("observer failed")

        entry_enqueued.connect(failing_receiver, weak=False)
        try:
            entry_id = queue.enqueue("work")
        finally:
            entry_enqueued.disconnect(failing_receiver)

        assert queue.find(entry_id).payload == "work"

    def test_enqueue_returns_an_id_and_persists_queued_entry(self, queue):
        entry_id = queue.enqueue({"request_id": 42})

        entry = queue.find(entry_id)

        assert entry.id == entry_id
        assert entry.queue == "requests"
        assert entry.status is QueueEntryStatus.QUEUED
        assert entry.queued_at == FIXED_CLOCK_TIME
        assert entry.payload == {"request_id": 42}

    def test_dequeue_removes_the_entry_from_pending_work_but_retains_its_record(
        self, queue
    ):
        entry_id = queue.enqueue("work")

        entry = queue.dequeue()

        assert entry.id == entry_id
        assert queue.find(entry_id) == entry
        with pytest.raises(QueueEmptyException):
            queue.dequeue()

    def test_records_lifecycle_transitions(self, queue):
        entry_id = queue.enqueue("work")
        running = queue._mark_running(entry_id)
        completed = queue._mark_succeeded(entry_id, {"ok": True})

        assert running.status is QueueEntryStatus.RUNNING
        assert running.dispatched_at == FIXED_CLOCK_TIME
        assert completed.status is QueueEntryStatus.SUCCEEDED
        assert completed.result == {"ok": True}
        assert completed.finished_at == FIXED_CLOCK_TIME

    def test_records_a_safe_failure_without_a_traceback(self, queue):
        entry_id = queue.enqueue("work")
        queue._mark_running(entry_id)

        failed = queue._mark_failed(entry_id, ValueError("invalid request"))

        assert failed.status is QueueEntryStatus.FAILED
        assert failed.error == {"type": "ValueError", "message": "invalid request"}

    def test_records_a_pre_dispatch_failure_without_a_dispatch_timestamp(self, queue):
        entry_id = queue.enqueue("work", available_at=FIXED_CLOCK_TIME + 10)

        failed = queue._mark_failed(entry_id, ValueError("transport unavailable"))

        assert failed.status is QueueEntryStatus.FAILED
        assert failed.dispatched_at is None
        assert failed.finished_at == FIXED_CLOCK_TIME
        assert not queue.has_pending()
        queue.clock.timestamp = FIXED_CLOCK_TIME + 10
        with pytest.raises(QueueEmptyException):
            queue.dequeue()

    def test_find_reports_a_missing_retained_record(self, queue):
        with pytest.raises(QueueEntryNotFoundError):
            queue.find(uuid4())

    def test_prune_removes_a_terminal_entry_and_notifies_observers(
        self, observer_queue, no_runtime_startup
    ):
        terminated = threading.Event()
        snapshots = []
        subscription = queue_observer(
            "requests",
            lambda entry: (
                snapshots.append(entry),
                terminated.set()
                if entry.status is QueueEntryStatus.TERMINATED
                else None,
            ),
        )
        try:
            entry_id = observer_queue.enqueue("work")
            observer_queue.dequeue()
            observer_queue._mark_running(entry_id)
            observer_queue._mark_succeeded(entry_id, "done")

            observer_queue.prune(entry_id)

            assert terminated.wait(1)
            assert snapshots[-1].status is QueueEntryStatus.TERMINATED
            with pytest.raises(QueueEntryNotFoundError):
                observer_queue.find(entry_id)
        finally:
            subscription.unsubscribe()

    def test_prune_removes_the_durable_entry_before_publishing_termination(
        self, queue, monkeypatch
    ):
        entry_id = queue.enqueue("work")
        queue._mark_running(entry_id)
        queue._mark_succeeded(entry_id, "done")
        snapshots = []

        async def publish(entry):
            with pytest.raises(QueueEntryNotFoundError):
                await queue.afind(entry.id)
            snapshots.append(entry)

        monkeypatch.setattr(queue, "apublish", publish)

        queue.prune(entry_id)

        assert snapshots[-1].status is QueueEntryStatus.TERMINATED

    def test_prune_refuses_a_non_terminal_entry(self, queue):
        entry_id = queue.enqueue("work")

        with pytest.raises(ValueError, match="terminal"):
            queue.prune(entry_id)

        assert queue.find(entry_id).status is QueueEntryStatus.QUEUED

    def test_prune_reports_an_absent_entry(self, queue):
        with pytest.raises(QueueEntryNotFoundError):
            queue.prune(uuid4())

    def test_expired_terminal_entries_are_pruned(self):
        clock = FixedClock()
        queue = MemoryAsyncQueue(queue_name="requests", clock=clock)
        queue.retention_timeout = 10
        entry_id = queue.enqueue("work")
        queue._mark_running(entry_id)
        queue._mark_succeeded(entry_id, "done")
        clock.timestamp = FIXED_CLOCK_TIME + 10

        assert asyncio.run(queue._aprune_expired()) == 1
        with pytest.raises(QueueEntryNotFoundError):
            queue.find(entry_id)

    def test_expired_pre_dispatch_failures_are_pruned(self):
        clock = FixedClock()
        queue = MemoryAsyncQueue(queue_name="requests", clock=clock)
        queue.retention_timeout = 10
        entry_id = queue.enqueue("work")
        queue._mark_failed(entry_id, ValueError("transport unavailable"))
        clock.timestamp = FIXED_CLOCK_TIME + 10

        assert asyncio.run(queue._aprune_expired()) == 1
        with pytest.raises(QueueEntryNotFoundError):
            queue.find(entry_id)

    def test_worker_prunes_expired_terminal_entries(
        self, observer_queue, no_runtime_startup
    ):
        terminated = threading.Event()
        subscription = queue_observer(
            "requests",
            lambda entry: (
                terminated.set()
                if entry.status is QueueEntryStatus.TERMINATED
                else None
            ),
        )
        try:
            observer_queue.retention_timeout = 0
            entry_id = observer_queue.enqueue("work")
            observer_queue._mark_running(entry_id)
            observer_queue._mark_succeeded(entry_id, "done")

            async def handler(entry):
                return entry.payload

            async def exercise():
                worker = AsyncQueueWorker(
                    {"requests": observer_queue},
                    {"requests": handler},
                    idle_delay=0.001,
                )
                task = asyncio.create_task(worker.run())
                assert await asyncio.to_thread(terminated.wait, 1)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

            asyncio.run(exercise())
            with pytest.raises(QueueEntryNotFoundError):
                observer_queue.find(entry_id)
        finally:
            subscription.unsubscribe()

    def test_explicit_retention_opt_out_leaves_terminal_entries(self):
        queue = MemoryAsyncQueue(queue_name="requests", clock=FixedClock())
        queue.retention_timeout = None
        entry_id = queue.enqueue("work")
        queue._mark_running(entry_id)
        queue._mark_succeeded(entry_id, "done")

        assert asyncio.run(queue._aprune_expired()) == 0
        assert queue.find(entry_id).status is QueueEntryStatus.SUCCEEDED

    def test_only_async_queues_expose_entry_pruning(self):
        assert hasattr(AsyncQueue, "prune")
        assert not hasattr(BaseQueue, "prune")
        assert not hasattr(EventQueue, "prune")

    def test_async_queue_does_not_expose_superseded_lifecycle_names(self):
        for name in (
            "get_entry",
            "aget_entry",
            "dequeue_entry",
            "adequeue_entry",
            "has_pending_entries",
            "ahas_pending_entries",
            "prune_entry",
            "aprune_entry",
        ):
            assert not hasattr(AsyncQueue, name)

    def test_rejects_invalid_lifecycle_transitions(self, queue):
        entry_id = queue.enqueue("work")

        with pytest.raises(ValueError, match="queued.*succeeded"):
            queue._mark_succeeded(entry_id, "done")

        queue._mark_running(entry_id)
        queue._mark_succeeded(entry_id, "done")
        with pytest.raises(ValueError, match="succeeded.*failed"):
            queue._mark_failed(entry_id, ValueError("too late"))

    def test_records_cancellation_as_a_terminal_outcome(self, queue):
        entry_id = queue.enqueue("work")
        queue._mark_running(entry_id)

        cancelled = queue._mark_cancelled(entry_id)

        assert cancelled.status is QueueEntryStatus.CANCELLED
        assert cancelled.finished_at == FIXED_CLOCK_TIME

    def test_records_a_timeout_as_a_terminal_outcome(self, queue):
        """Distinct from cancellation: the handler never answered."""
        entry_id = queue.enqueue("work")
        queue._mark_running(entry_id)

        timed_out = queue._mark_timed_out(entry_id)

        assert timed_out.status is QueueEntryStatus.TIMEOUT
        assert timed_out.finished_at == FIXED_CLOCK_TIME

    def test_refuses_to_move_a_timed_out_entry_on(self, queue):
        entry_id = queue.enqueue("work")
        queue._mark_running(entry_id)
        queue._mark_timed_out(entry_id)

        with pytest.raises(ValueError, match="timeout"):
            queue._mark_succeeded(entry_id, "too late")

    def test_persists_an_execution_budget_given_at_enqueue(self, queue):
        entry_id = queue.enqueue("work", timeout_seconds=2.5)

        assert queue.find(entry_id).timeout_seconds == 2.5

    def test_carries_no_budget_when_none_was_given(self, queue):
        assert queue.find(queue.enqueue("work")).timeout_seconds is None

    @pytest.mark.parametrize("budget", [0, -1, float("nan"), float("inf")])
    def test_refuses_to_enqueue_an_invalid_budget(self, queue, budget):
        """Rejected where it is supplied, not when a worker comes to apply it."""
        with pytest.raises(ValueError, match="Execution budget"):
            queue.enqueue("work", timeout_seconds=budget)

    def test_rejects_a_budget_on_the_item_oriented_api(self, queue):
        """`add` stores raw values and dispatches nothing, so a budget is meaningless."""
        with pytest.raises(TypeError, match="timeout_seconds"):
            queue.add("work", timeout_seconds=2.5)

    def test_uses_its_configured_entry_subclass_for_lifecycle_operations(self, queue):
        queue.entry_class = CustomQueueEntry

        entry_id = queue.enqueue("work")
        queued = queue.find(entry_id)
        running = queue._mark_running(entry_id)
        completed = queue._mark_succeeded(entry_id, "done")

        assert isinstance(queued, CustomQueueEntry)
        assert isinstance(running, CustomQueueEntry)
        assert isinstance(completed, CustomQueueEntry)
        assert completed.kind == "task"


def test_memory_priority_queue_supports_identified_entries():
    queue = MemoryAsyncPriorityQueue(queue_name="priority")

    entry_id = queue.enqueue({"value": "work"})
    entry = queue.dequeue()

    assert entry.id == entry_id


def test_memory_async_queue_holds_a_future_available_entry_until_due():
    """A future `available_at` is durable pending work, not dequeueable work."""
    clock = FixedClock()
    queue = MemoryAsyncQueue(queue_name="scheduled", clock=clock)

    entry_id = queue.enqueue("later", available_at=FIXED_CLOCK_TIME + 10)

    assert queue.has_pending()
    with pytest.raises(QueueEmptyException):
        queue.dequeue()

    clock.timestamp = FIXED_CLOCK_TIME + 10
    assert queue.dequeue().id == entry_id


def test_memory_async_queue_releases_one_earliest_due_entry_per_round():
    clock = FixedClock()
    queue = MemoryAsyncQueue(queue_name="scheduled", clock=clock)

    later_id = queue.enqueue("later", available_at=FIXED_CLOCK_TIME + 20)
    earlier_id = queue.enqueue("earlier", available_at=FIXED_CLOCK_TIME + 10)
    clock.timestamp = FIXED_CLOCK_TIME + 20

    assert queue.dequeue().id == earlier_id
    assert queue.dequeue().id == later_id


def test_memory_async_queue_rejects_a_non_clocktime_available_at(queue):
    with pytest.raises(TypeError, match="available_at must be a ClockTime or None"):
        queue.enqueue("work", available_at=10)


@pytest.mark.parametrize("available_at", [FIXED_CLOCK_TIME, FIXED_CLOCK_TIME - 1])
def test_memory_async_queue_dispatches_an_already_due_available_entry(available_at):
    queue = MemoryAsyncQueue(queue_name="scheduled-now", clock=FixedClock())

    entry_id = queue.enqueue("now", available_at=available_at)

    assert queue.dequeue().id == entry_id


def test_memory_event_queue_ignores_available_at():
    queue = MemoryEventQueue(queue_name="events", clock=FixedClock())

    entry_id = queue.enqueue("event", available_at=FIXED_CLOCK_TIME + 60)

    assert queue.dequeue().id == entry_id


def test_memory_priority_queue_promotes_due_scheduled_work_by_priority():
    clock = FixedClock()
    queue = MemoryAsyncPriorityQueue(queue_name="scheduled-priority", clock=clock)

    future_high_id = queue.enqueue(
        "later", priority=10, available_at=FIXED_CLOCK_TIME + 10
    )
    immediate_low_id = queue.enqueue("now", priority=1)

    assert queue.dequeue().id == immediate_low_id

    clock.timestamp = FIXED_CLOCK_TIME + 10
    assert queue.dequeue().id == future_high_id


def test_memory_priority_queue_releases_one_earliest_due_entry_per_round():
    clock = FixedClock()
    queue = MemoryAsyncPriorityQueue(queue_name="scheduled-priority", clock=clock)

    earlier_low_id = queue.enqueue(
        "earlier", priority=1, available_at=FIXED_CLOCK_TIME + 10
    )
    later_high_id = queue.enqueue(
        "later", priority=10, available_at=FIXED_CLOCK_TIME + 20
    )
    clock.timestamp = FIXED_CLOCK_TIME + 20

    assert queue.dequeue().id == earlier_low_id
    assert queue.dequeue().id == later_high_id


def test_memory_priority_queue_orders_an_availability_group_by_priority():
    clock = FixedClock()
    queue = MemoryAsyncPriorityQueue(queue_name="scheduled-priority", clock=clock)

    low_id = queue.enqueue("low", priority=1, available_at=FIXED_CLOCK_TIME + 10)
    high_id = queue.enqueue("high", priority=10, available_at=FIXED_CLOCK_TIME + 10)
    clock.timestamp = FIXED_CLOCK_TIME + 10

    assert queue.dequeue().id == high_id
    assert queue.dequeue().id == low_id


def test_memory_priority_queue_has_pending_after_a_tracked_priority_enqueue():
    """has_pending() previously only inspected the plain FIFO pending store,
    so a priority-only queue never reported readiness -- runqueues polls
    exactly this predicate before ever constructing a worker
    (runqueues.py:104), so this bug meant a priority backend's worker was
    never started at all."""
    queue = MemoryAsyncPriorityQueue(queue_name="priority")

    assert not queue.has_pending()
    queue.enqueue("work", priority=5)
    assert queue.has_pending()
    queue.dequeue()
    assert not queue.has_pending()


def test_memory_priority_queue_dispatches_the_tracked_path_in_priority_order():
    queue = MemoryAsyncPriorityQueue(queue_name="priority")

    low_id = queue.enqueue("low", priority=1)
    high_id = queue.enqueue("high", priority=10)

    assert queue.dequeue().id == high_id
    assert queue.dequeue().id == low_id


def test_memory_priority_queue_preserves_arrival_order_within_one_priority():
    queue = MemoryAsyncPriorityQueue(queue_name="priority")

    first_id = queue.enqueue("first", priority=5)
    second_id = queue.enqueue("second", priority=5)

    assert queue.dequeue().id == first_id
    assert queue.dequeue().id == second_id


def test_memory_priority_queue_arrival_order_does_not_depend_on_uuid_ordering():
    """Push order must win the tie-break even when entry IDs sort the
    opposite way -- proves the provider's own monotonic sequence counter is
    doing the work, not an incidental property of uuid.uuid7()'s current
    (undocumented, implementation-specific) monotonicity."""
    from django_queue.backends.memory.provider import QueueProviderMemory

    provider = QueueProviderMemory()
    first_id = uuid7()
    second_id = uuid7()
    assert first_id < second_id, "test setup requires an increasing UUID pair"
    first_entry = QueueEntry.create(queue="priority", payload="first", priority=5)
    second_entry = QueueEntry.create(queue="priority", payload="second", priority=5)
    first_entry = replace(first_entry, id=second_id)
    second_entry = replace(second_entry, id=first_id)

    async def scenario():
        await provider.astore(first_entry)
        await provider.astore(second_entry)
        await provider.apush_priority(first_entry.id, first_entry.priority)
        await provider.apush_priority(second_entry.id, second_entry.priority)
        return [await provider.apop_priority(), await provider.apop_priority()]

    dispatched = asyncio.run(scenario())

    assert [entry.id for entry in dispatched] == [first_entry.id, second_entry.id]


def test_memory_provider_adelete_also_removes_a_still_pending_priority_entry():
    """adelete's contract is "remove entry_id from every store it could be
    sitting in" -- not reachable through the public AsyncQueue/EventQueue
    API today (adelete is only ever called by EventQueue.aclear(), which
    never touches the priority pending store), but a future caller must not
    be able to silently orphan an entry that's still queued in a priority
    backend's pending store."""
    from django_queue.backends.memory.provider import QueueProviderMemory

    provider = QueueProviderMemory()
    entry = QueueEntry.create(queue="priority", payload="work", priority=5)

    async def scenario():
        await provider.astore(entry)
        await provider.apush_priority(entry.id, entry.priority)
        await provider.adelete(entry.id)
        with pytest.raises(QueueEmptyException):
            await provider.apop_priority()

    asyncio.run(scenario())


def test_memory_priority_queue_dequeued_entry_is_findable_and_runs_the_lifecycle():
    queue = MemoryAsyncPriorityQueue(queue_name="priority")

    entry_id = queue.enqueue("work", priority=3)
    dequeued = queue.dequeue()

    assert dequeued.id == entry_id
    assert queue.find(entry_id).id == entry_id
    running = queue._mark_running(entry_id)
    assert running.status == QueueEntryStatus.RUNNING


def test_memory_priority_queue_defaults_priority_to_zero(queue):
    """`queue` is parametrized over fifo/priority/stack (see the fixture
    above) -- covers a zero-priority entry still dispatching on the
    priority variant, not getting stuck behind an always-nonzero
    assumption, without a separate priority-only queue construction."""
    entry_id = queue.enqueue("work")

    entry = queue.find(entry_id)

    assert entry.priority == 0
    assert queue.dequeue().id == entry_id


@pytest.mark.parametrize(
    "queue_cls", [MemoryAsyncQueue, MemoryAsyncStack], ids=["fifo", "stack"]
)
def test_non_priority_backend_ignores_priority_and_dispatches_fifo(queue_cls):
    queue = queue_cls(queue_name="non-priority")

    first_id = queue.enqueue("first", priority=1)
    second_id = queue.enqueue("second", priority=99)

    first_dequeued = queue.dequeue().id
    second_dequeued = queue.dequeue().id
    if queue_cls is MemoryAsyncStack:
        assert (first_dequeued, second_dequeued) == (second_id, first_id)
    else:
        assert (first_dequeued, second_dequeued) == (first_id, second_id)


def test_memory_priority_queue_failure_removes_entry_from_priority_pending_store():
    queue = MemoryAsyncPriorityQueue(queue_name="priority")

    entry_id = queue.enqueue("work", priority=5)
    queue._mark_failed(entry_id, ValueError("boom"))

    with pytest.raises(QueueEmptyException):
        queue.dequeue()


def test_memory_priority_queue_discard_preserves_remaining_pop_order():
    """Removing the highest-priority (heap-root) entry from the priority
    pending store must not corrupt the heap ordering of the entries left
    behind -- a plain filter of PriorityQueue's internal list without
    re-heapifying can silently break future pop order without raising
    anything. A small number of entries can survive a naive filter by luck
    (the root's removal happens to leave a still-valid heap); this uses
    enough entries, discarding the actual root, to force a real
    parent-child violation if the fix regresses."""
    queue = MemoryAsyncPriorityQueue(queue_name="priority")

    ids = {
        name: queue.enqueue(name, priority=priority)
        for name, priority in [
            ("a", 1),
            ("b", 2),
            ("c", 3),
            ("d", 4),
            ("e", 5),
            ("f", 6),
            ("g", 7),
        ]
    }

    queue._mark_failed(ids["g"], ValueError("boom"))

    dispatch_order = [queue.dequeue().id for _ in range(6)]
    assert dispatch_order == [ids[name] for name in ["f", "e", "d", "c", "b", "a"]]


def test_memory_stack_dequeues_newest_entry_first():
    queue = MemoryAsyncStack(queue_name="stack")

    first_id = queue.enqueue("first")
    latest_id = queue.enqueue("latest")

    assert queue.dequeue().id == latest_id
    assert queue.dequeue().id == first_id
