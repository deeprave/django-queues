"""Local worker for owner-less notification queues."""

from __future__ import annotations

import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from typing import Any

from asgiref.sync import sync_to_async

from django_queue.backends.base import NotificationQueue
from django_queue.entries import QueueEntry
from django_queue.listeners import listeners_for
from django_queue.worker import BaseQueueWorker

logger = logging.getLogger(__name__)


class NotificationQueueWorker(BaseQueueWorker, ABC):
    """Provider-agnostic listener orchestration for notification workers."""

    def __init__(
        self,
        queue: NotificationQueue,
        *,
        alias: str | None = None,
        idle_delay: float = 0.1,
    ) -> None:
        if not isinstance(queue, NotificationQueue):
            raise TypeError("NotificationQueueWorker requires a NotificationQueue")
        if self.provider_kind != queue.worker_provider_kind:
            raise TypeError(
                f"{type(queue).__name__} requires a {queue.worker_provider_kind} worker"
            )
        if not queue._worker_class_is_compatible(type(self)):
            raise TypeError(
                f"{type(self).__name__} is not compatible with {type(queue).__name__}"
            )
        super().__init__(idle_delay=idle_delay)
        self._queue = queue
        self._alias = queue.queue_name if alias is None else alias

    async def run(self) -> None:
        self._running = True
        try:
            while True:
                if not await self.adispatch_once():
                    await asyncio.sleep(self._idle_delay)
        finally:
            self._running = False

    async def adispatch_once(self) -> bool:
        """Expire at most one due stored payload, then dispatch one seen notification."""
        await self._expire_due()
        entry = await self._next()
        if entry is None:
            return False
        await self._dispatch(entry)
        return True

    @abstractmethod
    async def _next(self) -> QueueEntry | None:
        """Return one seen payload using this worker's provider-specific delivery."""
        raise NotImplementedError

    @abstractmethod
    async def _expire_due(self) -> None:
        """Expire at most one due stored copy whose sender-set lifetime has elapsed."""
        raise NotImplementedError

    async def _dispatch(self, entry: QueueEntry) -> None:
        for registration in listeners_for(self._alias):
            try:
                if registration.filter is not None and not await self._invoke(
                    registration.filter, entry
                ):
                    continue
                await self._invoke(registration.callback, entry)
            except Exception:
                logger.exception(
                    "Notification listener failed",
                    extra={
                        "queue": self._queue.queue_name,
                        "entry_id": str(entry.id),
                    },
                )

    async def _invoke(self, callback: Any, entry: QueueEntry) -> Any:
        if inspect.iscoroutinefunction(callback):
            return await callback(entry)
        result = await sync_to_async(callback)(entry)
        if inspect.isawaitable(result):
            return await result
        return result
