"""Redis-backed owner-less notification queue."""

from __future__ import annotations

from ..base import NotificationQueue
from .provider import QueueProviderRedis


class RedisNotificationQueue(NotificationQueue):
    """Owner-less notifications published through Redis Pub/Sub.

    Every process with an active receiver at publish time can see the payload;
    none owns it. A process without an active receiver at publish is not
    required to catch up. This is not a rewindable stream: storage exists
    for ``afind`` until worker expiry, not for replay.
    """

    requires_entry_class_at_construction = True
    worker_provider_kind = "redis"
    worker_provider_type = "redis"
    redis_topology = "standalone"
    provider_class = QueueProviderRedis
    worker_class = "django_queue.backends.redis.RedisNotificationQueueWorker"
    compatible_worker_class = "django_queue.backends.redis.RedisNotificationQueueWorker"

    def __init__(self, redis_url: str, options: dict | None = None, **kwargs) -> None:
        options = {} if options is None else options
        options |= kwargs
        self.entry_class = options.pop("entry_class", self.entry_class)
        self._provider = type(self).provider_class(
            redis_url, options, entry_class=self.entry_class
        )
        self._queue_name = self._provider.queue_name
        self._clock = self._provider.clock
