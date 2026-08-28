"""One process-local asyncio runtime for configured event, notification, and async queues."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Iterator
from typing import Protocol

from django_queue.backends.base import (
    AsyncQueue,
    BaseQueue,
    EventQueue,
    NotificationQueue,
)
from django_queue.observers import (
    _activate_pending_for,
    _alias_has_observer_registration,
    _observers_for,
)

logger = logging.getLogger(__name__)


class QueueLookup(Protocol):
    """Configured queue aliases and their lazily-built queue instances."""

    def __iter__(self) -> Iterator[str]: ...

    def __getitem__(self, alias: str) -> object: ...


class QueueRuntime:
    """Own one background loop hosting one task per configured queue alias.

    An alias is an EventQueue (a worker task: claim, dispatch to listeners,
    settle), a NotificationQueue (a receiver task: see the payload, dispatch
    to all eligible local listeners, expire stored entries), or an AsyncQueue
    with observers registered (a receiver task: relay lifecycle snapshots to
    `queue_observer` callbacks) -- never more than one of these -- so one
    alias-keyed task dict serves all three.
    """

    restart_initial_delay = 1.0
    restart_max_delay = 30.0

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._queues: dict[str, EventQueue | AsyncQueue | NotificationQueue] = {}
        self._closed = False

    def start_thread(self) -> None:
        """Start the shared background thread and loop, once, for this process.

        The sole entry point that brings the thread up. Called once from
        `DjangoQueueConfig.ready()` when `QUEUES` is non-empty. Every other
        method that schedules a task (`start`, `start_one`) assumes the
        thread is already running rather than starting it themselves --
        there is exactly one code path that kickstarts the runtime, not
        several conditional ones.

        Always waits for `_ready` before returning, including when another
        caller already started the thread -- a caller that returned early
        without waiting could see `_loop` still `None` and have its own
        `start`/`start_one` call silently no-op.
        """
        with self._lock:
            if self._closed:
                return
            if self._thread is None:
                self._ready.clear()
                self._thread = threading.Thread(
                    target=self._run_loop,
                    name="django-queues-runtime",
                    daemon=True,
                )
                self._thread.start()
        self._ready.wait()

    def start(self, queues: QueueLookup) -> None:
        """Schedule tasks for every configured event, notification, and async queue.

        Walks every alias in `queues`, resolving each via `queues[alias]`.
        Callers whose own resolution of `alias` is not yet complete (e.g.
        `QueueRegistry.create_connection`, invoked from inside
        `BaseConnectionHandler.__getitem__` before that alias is cached)
        must use `start_one` instead, or this recurses back into that
        caller for the same alias.
        """
        with self._lock:
            if self._closed:
                return
        event_queues = []
        notification_queues = []
        receiver_queues = []
        for alias in queues:
            self._classify(
                alias, queues[alias], event_queues, notification_queues, receiver_queues
            )
        self._start_classified(event_queues, notification_queues, receiver_queues)

    def start_one(self, alias: str, queue: BaseQueue) -> None:
        """Schedule the task for one already-resolved alias.

        Safe to call while that alias's own connection resolution is still
        in progress, unlike `start`, since it never re-resolves `alias`
        through `queues[alias]`.
        """
        with self._lock:
            if self._closed:
                return
        event_queues = []
        notification_queues = []
        receiver_queues = []
        self._classify(alias, queue, event_queues, notification_queues, receiver_queues)
        self._start_classified(event_queues, notification_queues, receiver_queues)

    def _classify(
        self,
        alias: str,
        queue: object,
        event_queues: list[tuple[str, EventQueue]],
        notification_queues: list[tuple[str, NotificationQueue]],
        receiver_queues: list[tuple[str, AsyncQueue]],
    ) -> None:
        if isinstance(queue, EventQueue):
            event_queues.append((alias, queue))
        elif isinstance(queue, NotificationQueue):
            notification_queues.append((alias, queue))
        elif isinstance(queue, AsyncQueue) and _alias_has_observer_registration(alias):
            # Activation replays synchronous, blocking work (a snapshot
            # fetch via configured_queue.list()), so it must happen here,
            # on the calling thread, before scheduling anything onto the
            # asyncio loop -- never inside a loop-scheduled method. Passes
            # `queue` through rather than letting activation re-resolve it,
            # since this can run while the caller's own registry lookup for
            # `alias` is still on the stack (see _activate_pending_for).
            _activate_pending_for(alias, queue)
            receiver_queues.append((alias, queue))

    def _start_classified(
        self,
        event_queues: list[tuple[str, EventQueue]],
        notification_queues: list[tuple[str, NotificationQueue]],
        receiver_queues: list[tuple[str, AsyncQueue]],
    ) -> None:
        if not event_queues and not notification_queues and not receiver_queues:
            return
        with self._lock:
            if self._closed or self._loop is None:
                return
            loop = self._loop
            for alias, queue in event_queues:
                loop.call_soon_threadsafe(self._start_worker, alias, queue)
            for alias, queue in notification_queues:
                loop.call_soon_threadsafe(
                    self._start_notification_receiver, alias, queue
                )
            for alias, queue in receiver_queues:
                loop.call_soon_threadsafe(self._start_receiver, alias, queue)

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with self._lock:
            self._loop = loop
            self._ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            results = loop.run_until_complete(
                asyncio.gather(
                    *(queue.aclose() for queue in self._queues.values()),
                    return_exceptions=True,
                )
            )
            for result in results:
                if isinstance(result, Exception):
                    logger.error("Unable to close queue", exc_info=result)
            loop.close()
            with self._lock:
                self._tasks.clear()
                self._queues.clear()
                self._loop = None
                self._thread = None

    def _start_worker(self, alias: str, queue: EventQueue) -> None:
        with self._lock:
            if self._closed or alias in self._tasks:
                return
            self._queues[alias] = queue
            task = asyncio.create_task(
                self._run_worker(alias, queue), name=f"event:{alias}"
            )
            self._tasks[alias] = task
        task.add_done_callback(lambda task: self._worker_done(alias, task))

    async def _run_worker(self, alias: str, queue: EventQueue) -> None:
        """Keep one queue dispatcher available across transient failures."""
        delay = self.restart_initial_delay
        while True:
            try:
                await queue.create_worker(alias).run()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Event worker stopped unexpectedly", extra={"queue": alias}
                )
            else:
                logger.error(
                    "Event worker stopped unexpectedly", extra={"queue": alias}
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.restart_max_delay)

    def _worker_done(self, alias: str, task: asyncio.Task[None]) -> None:
        with self._lock:
            self._tasks.pop(alias, None)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            logger.exception("Event worker supervisor failed", extra={"queue": alias})

    def _start_notification_receiver(
        self, alias: str, queue: NotificationQueue
    ) -> None:
        with self._lock:
            if self._closed or alias in self._tasks:
                return
            self._queues[alias] = queue
            task = asyncio.create_task(
                self._run_notification_receiver(alias, queue),
                name=f"notification:{alias}",
            )
            self._tasks[alias] = task
        task.add_done_callback(
            lambda task: self._notification_receiver_done(alias, task)
        )

    async def _run_notification_receiver(
        self, alias: str, queue: NotificationQueue
    ) -> None:
        """Keep one notification receiver available across transient failures."""
        delay = self.restart_initial_delay
        while True:
            try:
                await queue.create_worker(alias).run()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Notification receiver stopped unexpectedly", extra={"queue": alias}
                )
            else:
                logger.error(
                    "Notification receiver stopped unexpectedly", extra={"queue": alias}
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.restart_max_delay)

    def _notification_receiver_done(self, alias: str, task: asyncio.Task[None]) -> None:
        with self._lock:
            self._tasks.pop(alias, None)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            logger.exception(
                "Notification receiver supervisor failed", extra={"queue": alias}
            )

    def _start_receiver(self, alias: str, queue: AsyncQueue) -> None:
        with self._lock:
            if self._closed or alias in self._tasks:
                return
            receiver = queue._observer_receiver(_observers_for(queue).publish)
            if receiver is None:
                return
            self._queues[alias] = queue
            task = asyncio.create_task(receiver(), name=f"observer:{alias}")
            self._tasks[alias] = task
        task.add_done_callback(lambda task: self._receiver_done(alias, task))

    def _receiver_done(self, alias: str, task: asyncio.Task[None]) -> None:
        with self._lock:
            self._tasks.pop(alias, None)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            # Never re-raise into the shared loop: a receiver failure must not
            # destabilise the event workers sharing this loop. No automatic
            # retry -- a later registration for this alias starts a fresh one.
            logger.exception("Queue lifecycle receiver stopped", extra={"queue": alias})

    def stop_one(self, alias: str, timeout: float = 5.0) -> None:
        """Cancel and await one alias's task, leaving the runtime otherwise live.

        Unlike `shutdown()`, this does not close the shared thread/loop or
        affect any other alias -- other tests or callers relying on the
        singleton continue to work normally afterward. Blocks until the
        task (and whatever cleanup its cancellation triggers, e.g. an
        observer receiver's `aobserve()` closing its Redis connections) has
        actually finished, not just been asked to stop -- primarily useful
        to controlled test hosts that need a live runtime for one assertion
        and a guaranteed-clean stop before a resource such as a test
        container goes away.

        Must not be called from a coroutine running on this runtime's own
        loop (an event worker or observer receiver task calling this for its
        own or another alias): `future.result(timeout=...)` blocks the
        calling thread, and if that thread is the loop thread, the scheduled
        cancellation coroutine can never run until that same thread frees
        up -- a deadlock until `timeout` elapses, not resolution.
        """
        with self._lock:
            loop = self._loop
            task = self._tasks.get(alias)
        if loop is None or task is None:
            return

        async def _cancel_and_wait() -> None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        future = asyncio.run_coroutine_threadsafe(_cancel_and_wait(), loop)
        future.result(timeout=timeout)

    def shutdown(self) -> None:
        """Stop all queue tasks; primarily useful to controlled test hosts."""
        with self._lock:
            self._closed = True
            loop = self._loop
            thread = self._thread
        if loop is not None:
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join()


queue_runtime = QueueRuntime()
