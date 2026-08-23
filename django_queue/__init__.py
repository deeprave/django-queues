import asyncio
import logging
import math
import threading
from collections.abc import Mapping

from django.conf import settings as django_settings
from django.utils.connection import BaseConnectionHandler, ConnectionProxy
from django.utils.module_loading import import_string

from .aliases import validate_queue_alias
from .backends import InvalidQueueBackendError
from .backends.base import AsyncQueue, EventQueue
from .clock import ClockTime
from .entries import QueueEntry, QueueEntryStatus, validate_budget
from .event_worker import EventQueueWorker
from .listeners import queue_listener
from .observers import QueueSubscription, queue_observer
from .providers import QueueProvider
from .signals import queue_created
from .worker import AsyncQueueWorker, BaseQueueWorker, WorkerSnapshot, heartbeat

logger = logging.getLogger(__name__)

__all__ = (
    "AsyncQueueWorker",
    "BaseQueueWorker",
    "ClockTime",
    "EventQueueWorker",
    "QueueEntry",
    "QueueEntryStatus",
    "QueueProvider",
    "QueueRegistry",
    "QueueSubscription",
    "WorkerSnapshot",
    "aclose_queues",
    "close_queues",
    "heartbeat",
    "initialise_queues",
    "queue",
    "queue_listener",
    "queue_observer",
    "queues",
)


DEFAULT_QUEUE_ALIAS = "default"


class QueueRegistry(BaseConnectionHandler):
    settings_name = "QUEUES"
    exception_class = InvalidQueueBackendError

    def __init__(self, settings=None) -> None:
        super().__init__(settings)
        self._process_queues: dict[str, AsyncQueue | EventQueue] = {}
        self._process_queues_lock = threading.RLock()

    def configure_settings(self, settings):
        if settings is None:
            settings = getattr(django_settings, self.settings_name, {})
        if not isinstance(settings, Mapping):
            raise InvalidQueueBackendError(
                f"{self.settings_name} must be a mapping of queue aliases to configurations"
            )

        configured_queues = {}
        for alias, options in settings.items():
            validate_queue_alias(alias)
            if not isinstance(options, Mapping):
                raise InvalidQueueBackendError(
                    f"Queue alias '{alias}' must use a mapping configuration"
                )
            backend = options.get("BACKEND")
            if not isinstance(backend, str) or not backend:
                raise InvalidQueueBackendError(
                    f"Queue alias '{alias}' must define a non-empty BACKEND string"
                )
            configured_options = dict(options)
            # Validate now so a bad entry class fails before any backend is built,
            # but leave the configured value in place so settings stay faithful.
            _resolve_extension_class(
                alias, "ENTRY_CLASS", configured_options.get("ENTRY_CLASS"), QueueEntry
            )
            if "TIMEOUT" in configured_options:
                _resolve_timeout(alias, configured_options["TIMEOUT"])
            if "RETENTION_TIMEOUT" in configured_options:
                _resolve_retention_timeout(
                    alias, configured_options["RETENTION_TIMEOUT"]
                )
            configured_queues[alias] = configured_options
        return configured_queues

    def create_connection(self, alias: str):
        params: dict = self.settings[alias].copy()
        # A configured queue is identified by its ``QUEUES`` alias.  Do not
        # allow a backend option to give it a second, divergent identity.
        params.pop("queue_name", None)
        params["queue_name"] = alias
        backend = params.pop("BACKEND")
        location = params.pop("LOCATION", "")
        params.pop("HANDLER", None)
        params.pop("WORKER", None)
        params.pop("TIMEOUT", None)
        params.pop("RETENTION_TIMEOUT", None)
        entry_class = _resolve_extension_class(
            alias, "ENTRY_CLASS", params.pop("ENTRY_CLASS", None), QueueEntry
        )
        try:
            backend_cls = import_string(backend)
        except ImportError as e:
            raise InvalidQueueBackendError(
                f"Queue alias '{alias}' could not find backend '{backend}': {e}"
            ) from e
        if getattr(backend_cls, "requires_entry_class_at_construction", False):
            params["entry_class"] = entry_class
        if getattr(backend_cls, "connection_scope", "thread") == "process":
            with self._process_queues_lock:
                if (queue := self._process_queues.get(alias)) is None:
                    queue = self._build_queue(
                        alias, backend_cls, location, params, entry_class
                    )
                    self._process_queues[alias] = queue
        else:
            queue = self._build_queue(alias, backend_cls, location, params, entry_class)
        # Schedules this alias's task on the runtime's shared thread, started
        # once by DjangoQueueConfig.ready(). Uses start_one, not start(queues),
        # because this method runs inside BaseConnectionHandler.__getitem__
        # before it caches this alias -- start(self) would re-resolve `alias`
        # through queues[alias] here and recurse back into this method for
        # the same, still-uncached alias. start_one is idempotent per alias,
        # so a later resolution of the same alias is a no-op.
        from django_queue.queue_runtime import queue_runtime

        queue_runtime.start_one(alias, queue)
        return queue

    def _build_queue(
        self, alias: str, backend_cls, location: str, params: dict, entry_class
    ):
        try:
            queue = backend_cls(location, params)
        except (AttributeError, TypeError, ValueError) as exc:
            raise InvalidQueueBackendError(
                f"Queue alias '{alias}' has invalid backend options: {exc}"
            ) from exc
        handler = self.settings[alias].get("HANDLER")
        worker_class = self.settings[alias].get("WORKER")
        timeout_seconds = (
            _resolve_timeout(alias, self.settings[alias]["TIMEOUT"])
            if "TIMEOUT" in self.settings[alias]
            else None
        )
        retention_timeout = _resolve_retention_timeout(
            alias, self.settings[alias].get("RETENTION_TIMEOUT", 600)
        )
        if isinstance(queue, EventQueue) and handler is not None:
            raise InvalidQueueBackendError(
                f"Queue alias '{alias}' event queues use registered listeners and "
                "do not accept HANDLER metadata"
            )
        queue.entry_class = entry_class
        queue.timeout_seconds = timeout_seconds
        if isinstance(queue, AsyncQueue):
            queue.retention_timeout = retention_timeout
        if worker_class is not None:
            queue.worker_class = worker_class
        if isinstance(queue, AsyncQueue | EventQueue):
            queue.resolve_worker(alias)
        queue_created.send(self, name=alias, instance=queue)
        return queue


queues = QueueRegistry()

queue = ConnectionProxy(queues, DEFAULT_QUEUE_ALIAS)


def initialise_queues(registry: QueueRegistry | None = None) -> QueueRegistry:
    """Validate and construct every queue configured for this Django process."""
    registry = queues if registry is None else registry
    for alias in registry:
        registry[alias]
    return registry


def close_queues(registry: QueueRegistry | None = None) -> None:
    """Synchronously release resources created through synchronous wrappers."""
    (queues if registry is None else registry).close_all()


async def aclose_queues(registry: QueueRegistry | None = None) -> None:
    """Dispose configured queues from the loop that acquired their resources."""
    registry = queues if registry is None else registry
    results = await asyncio.gather(
        *(
            configured_queue.aclose()
            for configured_queue in registry.all(initialized_only=True)
        ),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, Exception):
            logger.error("Unable to dispose queue resources", exc_info=result)


def _resolve_timeout(alias: str, value: object) -> float:
    """Validate an alias's execution budget, in seconds.

    Defers to the shared budget rule and re-raises as a configuration error:
    a settings fault is an `ImproperlyConfigured` whichever way the value is
    wrong, and an operator reading it wants the alias named, not the exception
    class distinguished.
    """
    try:
        return validate_budget(value)
    except (TypeError, ValueError) as exc:
        raise InvalidQueueBackendError(
            f"Queue alias '{alias}' TIMEOUT is invalid: {exc}"
        ) from exc


def _resolve_retention_timeout(alias: str, value: object) -> float | None:
    """Validate terminal-entry retention, allowing explicit opt-out."""
    if value is None:
        return None
    try:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise ValueError("must be a non-negative number of seconds or None")
        return float(value)
    except ValueError as exc:
        raise InvalidQueueBackendError(
            f"Queue alias '{alias}' RETENTION_TIMEOUT is invalid: {exc}"
        ) from exc


def _resolve_extension_class(
    alias: str, name: str, value: object, base_class: type
) -> type:
    if value is None:
        return base_class
    if isinstance(value, str):
        if not value:
            raise InvalidQueueBackendError(
                f"Queue alias '{alias}' {name} must be a class or non-empty dotted path"
            )
        try:
            value = import_string(value)
        except ImportError as exc:
            raise InvalidQueueBackendError(
                f"Queue alias '{alias}' {name} could not be imported: {exc}"
            ) from exc
    if not isinstance(value, type) or not issubclass(value, base_class):
        raise InvalidQueueBackendError(
            f"Queue alias '{alias}' {name} must be a {base_class.__name__} subclass"
        )
    return value
