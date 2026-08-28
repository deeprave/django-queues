"""Redis implementation of the queue-provider protocol.

This module deliberately contains the Redis connection, key, script, and
claim machinery.  Queue classes build task or event lifecycle semantics on
top of this provider; they do not need to know how Redis represents them.
"""

from __future__ import annotations

import asyncio
import codecs
import inspect
import json
import logging
import uuid
from typing import Any

import redis
import redis.asyncio as async_redis
from asgiref.sync import async_to_sync
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

from django_queue.aliases import validate_queue_alias
from django_queue.backends.exceptions import (
    InvalidQueueBackendError,
    QueueClaimConflictError,
    QueueEmptyException,
    QueueEncodingException,
    QueueEntryExpiredError,
    QueueEntryMissingError,
    QueueEntryNotFoundError,
    QueueFullException,
    QueueValueError,
)
from django_queue.backends.redis.functions import FUNCTION_API_VERSION
from django_queue.backends.redis.transport import (
    redis_client_kwargs,
    redis_from_url_location,
    redis_tls_failure,
)
from django_queue.clock import (
    MICROSECONDS_PER_SECOND,
    QueueClock,
    QueueClockError,
    RedisQueueClock,
)
from django_queue.entries import QueueEntry, QueueEntryStatus, validate_budget

logger = logging.getLogger(__name__)

_PRIORITY_SEQUENCE_SPACE = 2**32
_NOTIFICATION_REMOVAL_LEASE_MS = 50

# This queue packs priority and an arrival-order sequence number into one
# ZSET score: `priority * _PRIORITY_SEQUENCE_SPACE - sequence`. A double's
# exact-integer range is 2**53, so `priority` must stay within `2**53 //
# 2**32 == 2**21` in magnitude or the packing loses precision -- silently,
# not by raising, since the corrupted value is still a valid float. This
# bound is intentionally far below that edge: no realistic dispatch-priority
# use needs six-figure values, and staying well clear of the edge leaves
# headroom without requiring callers to reason about IEEE 754 exactness.
#
# Enforced here, at the Redis priority provider's own write points
# (apush_priority, astore_and_push_priority), not in QueueEntry itself --
# a plain (non-priority) AsyncQueue/EventQueue MUST ignore `priority`
# entirely per spec, and QueueEntry has no way to know which backend an
# entry is destined for, so it must never reject a value on this backend's
# behalf.
MAX_PRIORITY_MAGNITUDE = 100_000


def validate_redis_priority_magnitude(priority: int) -> int:
    """Return *priority*, or raise if it would break this queue's ZSET
    score-packing exactness (see `MAX_PRIORITY_MAGNITUDE` above)."""
    if abs(priority) > MAX_PRIORITY_MAGNITUDE:
        raise ValueError(
            f"Queue entry priority must be within +/-{MAX_PRIORITY_MAGNITUDE} "
            f"for a priority-variant Redis queue, not {priority!r}"
        )
    return priority


class _RedisClockFacade:
    def __init__(self, provider: QueueProviderRedis) -> None:
        self._provider = provider

    def now(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None and (clock := self._provider._clocks_by_loop.get(loop)):
            return clock.now()
        if loop is not None:
            raise QueueClockError(
                "Redis queue clock is not calibrated; await queue.clock.anow() first"
            )
        return async_to_sync(self._anow_and_close)()

    async def anow(self):
        try:
            return await self._provider._async_clock().anow()
        except Exception as exc:
            if tls_error := redis_tls_failure(exc, self._provider._redis_url):
                raise tls_error from exc
            raise

    async def _anow_and_close(self):
        try:
            return await self.anow()
        finally:
            await self._provider.aclose()


class QueueProviderRedis:
    """Store entry records and ownership state in Redis.

    The provider accepts the same connection and queue identity options as the
    Redis queues, but exposes only record/claim operations.  It is deliberately
    independent of async-queue lifecycle transitions.
    """

    @staticmethod
    def encode(value: str, encoding: str) -> bytes:
        """Encode a Redis value with a queue-specific error."""
        try:
            return value.encode(encoding)
        except UnicodeEncodeError as exc:
            raise QueueEncodingException from exc

    @staticmethod
    def decode(value: object, encoding: str) -> str:
        """Decode a Redis value with a queue-specific error."""
        if isinstance(value, str):
            return value
        if isinstance(value, bytes | bytearray | memoryview):
            try:
                return bytes(value).decode(encoding)
            except UnicodeDecodeError as exc:
                raise QueueEncodingException from exc
        raise QueueEncodingException(
            f"Queue value must be text or bytes, not {type(value).__name__}"
        )

    def __init__(
        self,
        redis_url: str,
        options: dict | None = None,
        *,
        entry_class: type[QueueEntry],
        **kwargs,
    ) -> None:
        if not isinstance(redis_url, str):
            raise InvalidQueueBackendError("Redis queues require a Redis URL")
        options = {} if options is None else options
        options = options | kwargs
        try:
            connection_kwargs = redis.connection.parse_url(redis_url)
        except (AttributeError, ValueError) as exc:
            raise InvalidQueueBackendError(f"Redis URL is invalid: {exc}") from exc
        try:
            self._encoding = codecs.lookup(options.get("encoding", "utf-8")).name
        except (LookupError, TypeError) as exc:
            raise InvalidQueueBackendError("Queue encoding is invalid") from exc
        if (
            connection_kwargs.get("decode_responses", False)
            and self._encoding != "utf-8"
        ):
            raise InvalidQueueBackendError(
                "A Redis client with decode_responses cannot use a non-UTF-8 queue encoding"
            )
        self._redis_url = redis_url
        self._client_kwargs = redis_client_kwargs(redis_url, options)
        self.entry_class = entry_class
        self._queue_alias = validate_queue_alias(
            options.get("queue_name", f"queue_{uuid.uuid4().hex}")
        )
        self._queue_name = f"{{{self._queue_alias}}}"
        self._stack = bool(options.get("stack", False))
        self._maxsize = options.get("maxsize", 0)
        self._connection_encoding = connection_kwargs.get("encoding", "utf-8")
        self._entry_pending_name = f"{self._queue_name}:entries:pending"
        self._entry_pending_priority_name = (
            f"{self._queue_name}:entries:pending:priority"
        )
        self._entry_pending_priority_sequence_name = (
            f"{self._queue_name}:entries:pending:priority:sequence"
        )
        self._entry_delayed_name = f"{self._queue_name}:entries:delayed"
        self._entry_scheduled_name = f"{self._queue_name}:entries:scheduled"
        self._entry_claim_prefix = f"{self._queue_name}:entries:claims:"
        self._entry_claim_deadlines_name = f"{self._queue_name}:entries:claim-leases"
        self._entry_unclaimed_deadlines_name = (
            f"{self._queue_name}:entries:unclaimed-leases"
        )
        self._async_redis_by_loop: dict[asyncio.AbstractEventLoop, Any] = {}
        self._function_compatibility_by_loop: set[asyncio.AbstractEventLoop] = set()
        self._clocks_by_loop: dict[asyncio.AbstractEventLoop, RedisQueueClock] = {}
        self._clock: QueueClock = _RedisClockFacade(self)

    def _create_async_client(self) -> Any:
        return async_redis.from_url(
            redis_from_url_location(self._redis_url), **self._client_kwargs
        )

    def _function_info_keys(self) -> tuple[str, ...]:
        return ()

    async def _aclose_client(self, client: Any) -> None:
        try:
            await client.aclose(close_connection_pool=True)
        except TypeError:
            await client.aclose()

    async def _prepare_async_client(self, client: Any) -> Any:
        initialize = getattr(client, "initialize", None)
        if callable(initialize):
            result = initialize()
            if inspect.isawaitable(result):
                await result
        return client

    @property
    def clock(self) -> QueueClock:
        return self._clock

    @property
    def queue_name(self) -> str:
        return self._queue_alias

    @property
    def lifecycle_channel(self) -> str:
        """Return the backend-owned channel for async-queue lifecycle snapshots."""
        return f"{self._queue_name}:entries:lifecycle"

    @property
    def notification_channel(self) -> str:
        """Return the backend-owned channel for owner-less notification payloads."""
        return f"{self._queue_name}:notifications"

    def _notification_entry_key(self, entry_id: uuid.UUID) -> str:
        return f"{self._queue_name}:notifications:{entry_id}"

    def _notification_lease_key(self, entry_id: uuid.UUID) -> str:
        return f"{self._queue_name}:notifications:leases:{entry_id}"

    @property
    def _notification_deadlines_name(self) -> str:
        return f"{self._queue_name}:notifications:deadlines"

    @property
    def _notification_lease_prefix(self) -> str:
        return f"{self._queue_name}:notifications:leases:"

    @property
    def _notification_entry_prefix(self) -> str:
        return f"{self._queue_name}:notifications:"

    @property
    def capacity(self) -> int:
        return self._maxsize

    @property
    def stack(self) -> bool:
        return self._stack

    async def aadd(self, *items: str) -> None:
        if not items:
            return
        client = self._async_redis()
        current_size = await client.llen(self._queue_name)
        if self._maxsize and current_size + len(items) > self._maxsize:
            raise QueueFullException
        await client.rpush(
            self._queue_name,
            *(self.encode(item, self._encoding) for item in items if item is not None),
        )

    async def aget(self) -> str:
        client = self._async_redis()
        item = (
            await client.rpop(self._queue_name)
            if self._stack
            else await client.lpop(self._queue_name)
        )
        if item is None:
            raise QueueEmptyException
        return self.decode(item, self._encoding)

    async def apoll(self) -> str:
        client = self._async_redis()
        item = (
            await client.brpop([self._queue_name], 0)
            if self._stack
            else await client.blpop([self._queue_name], 0)
        )
        if not item:
            raise QueueEmptyException
        return self.decode(item[1], self._encoding)

    async def apeek(self) -> str:
        client = self._async_redis()
        values = (
            await client.lrange(self._queue_name, -1, -1)
            if self._stack
            else await client.lrange(self._queue_name, 0, 0)
        )
        if not values:
            raise QueueEmptyException
        return self.decode(values[0], self._encoding)

    async def asize(self) -> int:
        return await self._async_redis().llen(self._queue_name)

    async def aclear(self) -> None:
        await self._async_redis().delete(self._queue_name)

    async def aclear_records(self) -> None:
        """Remove all Redis state owned by this queue's retained entries.

        This is deliberately provider-local: it supports the bundled demo's
        fixture reset without exposing Redis keys through a queue facade.
        """
        client = self._async_redis()
        keys = [self._queue_name]
        async for key in client.scan_iter(match=f"{self._queue_name}:entries:*"):
            keys.append(key)
        await client.delete(*keys)

    async def aadd_priority(self, *items) -> None:
        for priority, value in items:
            if self._maxsize and await self.asize_priority() >= self._maxsize:
                raise QueueFullException
            await self._async_redis().zadd(
                self._queue_name,
                {self.encode(value, self._encoding): priority},
                nx=True,
            )

    async def aget_priority(self) -> str:
        if item := await self._async_redis().zrevrange(
            self._queue_name, 0, 0, withscores=False
        ):
            member = item[0]
            if not isinstance(member, bytes | bytearray | memoryview | str):
                raise QueueEncodingException(
                    f"Queue value must be text or bytes, not {type(member).__name__}"
                )
            await self._async_redis().zrem(self._queue_name, member)
            return self.decode(member, self._encoding)
        raise QueueEmptyException

    async def apoll_priority(self, timeout: int = 0, retries: int = 10) -> str:
        """Remove the highest-priority item, waiting for each attempt."""
        attempt = retries
        while retries == 0 or attempt > 0:
            attempt -= 1
            try:
                return await self.aget_priority()
            except QueueEmptyException:
                if timeout <= 0:
                    raise
                if item := await self._async_redis().bzpopmax(
                    [self._queue_name], timeout=timeout
                ):
                    return self.decode(item[1], self._encoding)
        raise QueueEmptyException

    async def apeek_priority(self) -> str:
        if item := await self._async_redis().zrevrange(
            self._queue_name, 0, 0, withscores=False
        ):
            return self.decode(item[0], self._encoding)
        raise QueueEmptyException

    async def asize_priority(self) -> int:
        return await self._async_redis().zcard(self._queue_name)

    async def aclear_priority(self) -> None:
        await self._async_redis().delete(self._queue_name)

    def _entry_key(self, entry_id: uuid.UUID) -> str:
        return f"{self._queue_name}:entries:{entry_id}"

    def _claim_key(self, entry_id: uuid.UUID) -> bytes:
        return self.encode(
            self._entry_claim_prefix, self._connection_encoding
        ) + self.encode(str(entry_id), "ascii")

    def _async_redis(self) -> Any:
        loop = asyncio.get_running_loop()
        if client := self._async_redis_by_loop.get(loop):
            return client
        client = self._create_async_client()
        self._async_redis_by_loop[loop] = client
        return client

    async def _ensure_function_compatibility(self) -> None:
        loop = asyncio.get_running_loop()
        if loop in self._function_compatibility_by_loop:
            return
        try:
            keys = self._function_info_keys()
            result = await self._async_redis().fcall(
                "django_queue_info", len(keys), *keys
            )
        except redis.RedisError as exc:
            if tls_error := redis_tls_failure(exc, self._redis_url):
                raise tls_error from exc
            raise InvalidQueueBackendError(
                "Redis Function library is unavailable or FCALL is denied; run "
                "redis_lua_compat with the application credentials. If the library "
                "is absent, deploy it with redis_lua_lib --deploy."
            ) from exc
        if not isinstance(result, list) or len(result) != 2:
            raise InvalidQueueBackendError(
                "Redis Function library returned invalid introspection data; run "
                "redis_lua_lib --deploy."
            )
        library_version, api_version = result
        if (
            not isinstance(library_version, str | bytes)
            or isinstance(api_version, bool)
            or not isinstance(api_version, int)
        ):
            raise InvalidQueueBackendError(
                "Redis Function library returned invalid introspection data; run "
                "redis_lua_lib --deploy."
            )
        if api_version < FUNCTION_API_VERSION:
            raise InvalidQueueBackendError(
                "Redis Function library api_version is below the required "
                f"{FUNCTION_API_VERSION}; run redis_lua_lib --deploy."
            )
        self._function_compatibility_by_loop.add(loop)

    async def _fcall(self, function: str, numkeys: int, *args: object) -> Any:
        await self._ensure_function_compatibility()
        try:
            return await self._async_redis().fcall(function, numkeys, *args)
        except redis.RedisError as exc:
            if tls_error := redis_tls_failure(exc, self._redis_url):
                raise tls_error from exc
            raise InvalidQueueBackendError(
                f"Redis Function {function} failed: {exc}"
            ) from exc

    async def aobserve(self, on_snapshot) -> None:
        """Receive and decode lifecycle snapshots through provider-owned Pub/Sub."""
        client = await self._prepare_async_client(self._create_async_client())
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        try:
            try:
                await pubsub.subscribe(self.lifecycle_channel)
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    try:
                        entry = self.entry_class.from_dict(json.loads(message["data"]))
                    except Exception:
                        logger.exception(
                            "Ignoring invalid queue lifecycle snapshot",
                            extra={"queue": self._queue_alias},
                        )
                        continue
                    on_snapshot(entry)
            except Exception as exc:
                if tls_error := redis_tls_failure(exc, self._redis_url):
                    raise tls_error from exc
                raise
        finally:
            await pubsub.aclose()
            await self._aclose_client(client)

    async def apublish(self, entry: QueueEntry) -> None:
        await self._async_redis().publish(
            self.lifecycle_channel, json.dumps(entry.to_dict())
        )

    def _async_clock(self) -> RedisQueueClock:
        loop = asyncio.get_running_loop()
        if clock := self._clocks_by_loop.get(loop):
            return clock
        clock = RedisQueueClock(self._async_redis(), asynchronous=True)
        self._clocks_by_loop[loop] = clock
        return clock

    async def astore(self, entry: QueueEntry) -> None:
        if entry.status is QueueEntryStatus.TERMINATED:
            raise TypeError("Terminated queue entry snapshots cannot be stored")
        await self._async_redis().set(
            self._entry_key(entry.id),
            self.encode(json.dumps(entry.to_dict()), "ascii"),
        )

    async def astore_event(self, entry: QueueEntry) -> None:
        if entry.timeout_seconds is None:
            raise ValueError("Event entries require a resolved lifetime")
        client = self._async_redis()
        await client.set(
            self._entry_key(entry.id),
            self.encode(json.dumps(entry.to_dict()), "ascii"),
        )
        await client.zadd(
            self._entry_unclaimed_deadlines_name,
            {
                str(entry.id): round(
                    (entry.queued_at + entry.timeout_seconds).to_timestamp()
                    * MICROSECONDS_PER_SECOND
                )
            },
        )

    async def astore_and_push(self, entry: QueueEntry) -> None:
        """Atomically store a new entry and add it to the plain pending list.

        `astore()` followed by a separate `apush()` leaves a window where a
        crash or connection loss
        between the two calls durably stores the entry with no pending-store
        index pointing to it -- a silent, permanent orphan.
        """
        if entry.status is QueueEntryStatus.TERMINATED:
            raise TypeError("Terminated queue entry snapshots cannot be stored")
        self._async_redis()
        await self._fcall(
            "django_queue_store_and_push",
            2,
            self._entry_key(entry.id),
            self._entry_pending_name,
            self.encode(json.dumps(entry.to_dict()), "ascii"),
            b"1" if self._stack else b"0",
            self.encode(str(entry.id), "ascii"),
        )

    async def astore_and_push_priority(self, entry: QueueEntry) -> None:
        """Like `astore_and_push`, but for a priority-variant queue's own
        pending store -- see `apush_priority` for the score encoding."""
        if entry.status is QueueEntryStatus.TERMINATED:
            raise TypeError("Terminated queue entry snapshots cannot be stored")
        validate_redis_priority_magnitude(entry.priority)
        self._async_redis()
        await self._fcall(
            "django_queue_store_and_push_priority",
            3,
            self._entry_key(entry.id),
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self.encode(json.dumps(entry.to_dict()), "ascii"),
            self.encode(str(entry.id), "ascii"),
            self.encode(str(entry.priority * _PRIORITY_SEQUENCE_SPACE), "ascii"),
        )

    async def astore_available(
        self, entry: QueueEntry, available_at, *, priority: bool
    ) -> None:
        """Atomically store an entry and choose scheduled or immediate membership."""
        if entry.status is QueueEntryStatus.TERMINATED:
            raise TypeError("Terminated queue entry snapshots cannot be stored")
        if priority:
            validate_redis_priority_magnitude(entry.priority)
        self._async_redis()
        await self._fcall(
            "django_queue_store_available",
            5,
            self._entry_key(entry.id),
            self._entry_pending_name,
            self._entry_scheduled_name,
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self.encode(json.dumps(entry.to_dict()), "ascii"),
            self.encode(str(entry.id), "ascii"),
            self.encode(
                str(
                    available_at.seconds * MICROSECONDS_PER_SECOND
                    + available_at.microseconds
                ),
                "ascii",
            ),
            self.encode(str(entry.priority * _PRIORITY_SEQUENCE_SPACE), "ascii"),
            b"1" if priority else b"0",
            b"1" if self._stack else b"0",
        )

    async def astore_and_discard(self, entry: QueueEntry) -> None:
        self._async_redis()
        await self._fcall(
            "django_queue_store_and_discard",
            5,
            self._entry_key(entry.id),
            self._entry_pending_name,
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self._entry_scheduled_name,
            self.encode(json.dumps(entry.to_dict()), "ascii"),
            self.encode(str(entry.id), "ascii"),
        )

    async def astore_event_and_push(self, entry: QueueEntry) -> None:
        """Atomically store a new event, index its unclaimed deadline, and
        add it to the plain pending list -- the event-queue equivalent of
        `astore_and_push`, closing the same class of window across all
        three of `astore_event`'s writes plus the outer `apush`."""
        if entry.timeout_seconds is None:
            raise ValueError("Event entries require a resolved lifetime")
        self._async_redis()
        await self._fcall(
            "django_queue_store_event_and_push",
            3,
            self._entry_key(entry.id),
            self._entry_unclaimed_deadlines_name,
            self._entry_pending_name,
            self.encode(json.dumps(entry.to_dict()), "ascii"),
            b"1" if self._stack else b"0",
            self.encode(str(entry.id), "ascii"),
            self.encode(
                str(
                    round(
                        (entry.queued_at + entry.timeout_seconds).to_timestamp()
                        * MICROSECONDS_PER_SECOND
                    )
                ),
                "ascii",
            ),
        )

    async def afind(self, entry_id: uuid.UUID) -> QueueEntry:
        raw = await self._async_redis().get(self._entry_key(entry_id))
        if raw is None:
            raise QueueEntryNotFoundError(entry_id)
        return self.entry_class.from_dict(json.loads(raw))

    async def adelete(self, entry_id: uuid.UUID) -> None:
        """Atomically remove entry_id from every store it could be sitting
        in -- the durable record, the plain pending list, the delayed set,
        both claim-lease ZSETs, the claim key, and the priority pending
        ZSET (with its sequence counter reset if that ZSET just emptied).

        No caller relies on the priority-store cleanup today -- adelete is
        only reached via EventQueue.aclear(), which never touches the
        priority pending store -- but leaving it out would silently orphan
        an entry if a future caller (e.g. AsyncQueue gaining its own
        adelete/aremove) ever reached here while the entry was still queued
        in a priority backend.
        """
        entry_id_value = self.encode(str(entry_id), "ascii")
        self._async_redis()
        await self._fcall(
            "django_queue_delete",
            9,
            self._entry_key(entry_id),
            self._entry_pending_name,
            self._entry_delayed_name,
            self._entry_claim_deadlines_name,
            self._entry_unclaimed_deadlines_name,
            self._claim_key(entry_id),
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self._entry_scheduled_name,
            entry_id_value,
        )

    async def aexpire(self, entry_id: uuid.UUID) -> bool:
        """Atomically delete an event only when it has no active claim."""
        self._async_redis()
        return bool(
            await self._fcall(
                "django_queue_expire",
                6,
                self._claim_key(entry_id),
                self._entry_key(entry_id),
                self._entry_pending_name,
                self._entry_delayed_name,
                self._entry_claim_deadlines_name,
                self._entry_unclaimed_deadlines_name,
                self.encode(str(entry_id), "ascii"),
            )
        )

    async def aexpire_due(self) -> list[uuid.UUID]:
        now = await self.clock.anow()
        raw_ids = await self._async_redis().zrangebyscore(
            self._entry_unclaimed_deadlines_name,
            "-inf",
            round(now.to_timestamp() * MICROSECONDS_PER_SECOND),
        )
        expired = []
        for raw_entry_id in raw_ids:
            entry_id = uuid.UUID(self.decode(raw_entry_id, "ascii"))
            if await self.aexpire(entry_id):
                expired.append(entry_id)
        return expired

    async def alist(self) -> list[QueueEntry]:
        client = self._async_redis()
        keys = []
        match = f"{self._queue_name}:entries:????????-????-????-????-????????????"
        async for key in client.scan_iter(match=match):
            keys.append(key)
        if not keys:
            return []
        return [
            self.entry_class.from_dict(json.loads(raw))
            for raw in await client.mget(keys)
            if raw is not None
        ]

    async def apush(self, entry_id: uuid.UUID) -> None:
        await self._async_redis().rpush(
            self._entry_pending_name, self.encode(str(entry_id), "ascii")
        )

    async def apromote_scheduled(self) -> None:
        """Move due scheduled IDs into FIFO membership before direct dequeue."""
        await self._fcall(
            "django_queue_promote_scheduled",
            3,
            self._entry_scheduled_name,
            f"{self._queue_name}:entries:",
            self._entry_pending_name,
            b"1" if self._stack else b"0",
        )

    async def apromote_scheduled_priority(self) -> None:
        """Move due scheduled IDs into priority membership before direct dequeue."""
        await self._fcall(
            "django_queue_promote_scheduled_priority",
            4,
            self._entry_scheduled_name,
            f"{self._queue_name}:entries:",
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self.encode(str(_PRIORITY_SEQUENCE_SPACE), "ascii"),
        )

    async def apop(self) -> QueueEntry:
        raw_entry_id = (
            await self._async_redis().rpop(self._entry_pending_name)
            if self._stack
            else await self._async_redis().lpop(self._entry_pending_name)
        )
        if raw_entry_id is None:
            raise QueueEmptyException
        return await self.afind(uuid.UUID(self.decode(raw_entry_id, "ascii")))

    async def apop_scheduled(self) -> QueueEntry:
        """Atomically promote one due entry and pop the FIFO or stack head."""
        raw_entry_id = await self._fcall(
            "django_queue_dequeue",
            3,
            self._entry_scheduled_name,
            f"{self._queue_name}:entries:",
            self._entry_pending_name,
            b"1" if self._stack else b"0",
        )
        if raw_entry_id is None:
            raise QueueEmptyException
        return await self.afind(uuid.UUID(self.decode(raw_entry_id, "ascii")))

    async def apop_scheduled_priority(self) -> QueueEntry:
        """Atomically promote one due entry and pop the priority head."""
        raw_entry_id = await self._fcall(
            "django_queue_dequeue_priority",
            4,
            self._entry_scheduled_name,
            f"{self._queue_name}:entries:",
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self.encode(str(_PRIORITY_SEQUENCE_SPACE), "ascii"),
        )
        if raw_entry_id is None:
            raise QueueEmptyException
        return await self.afind(uuid.UUID(self.decode(raw_entry_id, "ascii")))

    async def adiscard(self, entry_id: uuid.UUID) -> None:
        await self._async_redis().lrem(
            self._entry_pending_name, 0, self.encode(str(entry_id), "ascii")
        )

    async def adiscard_scheduled(self, entry_id: uuid.UUID) -> None:
        await self._async_redis().zrem(
            self._entry_scheduled_name, self.encode(str(entry_id), "ascii")
        )

    async def apush_priority(self, entry_id: uuid.UUID, priority: int) -> None:
        # A ZSET score alone cannot express "arrival order within equal
        # priority": ties break on member value (the encoded entry ID), not
        # insertion order. Folding this queue's own monotonic sequence into
        # the low bits of the score (subtracted, so an earlier sequence number
        # yields a higher score) makes zrevrange return equal-priority
        # entries oldest-first, matching apop's plain-FIFO tie-break and the
        # memory backend's PriorityQueue (a stable, insertion-ordered heap
        # for equal keys).
        #
        # INCR and ZADD run inside one Lua script rather than as two
        # separate round-trips: `sequence` resets to 0 whenever the priority
        # ZSET drains empty (see apop_priority/adiscard_priority), so that
        # -- unlike an ever-growing counter -- it never runs out of the
        # headroom a double's 53-bit exact-integer range gives it. If the
        # INCR and the ZADD were separate calls, a push that reserved a
        # sequence number just before the queue drained (and reset) could
        # still land its ZADD afterwards, using a now-stale, oversized
        # sequence value that sorts after every entry pushed since the
        # reset -- silently breaking arrival order for exactly the entries
        # this mechanism exists to protect. One atomic script closes that
        # window entirely.
        validate_redis_priority_magnitude(priority)
        await self._fcall(
            "django_queue_push_priority",
            2,
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self.encode(str(priority * _PRIORITY_SEQUENCE_SPACE), "ascii"),
            self.encode(str(entry_id), "ascii"),
        )

    async def apop_priority(self) -> QueueEntry:
        member = await self._fcall(
            "django_queue_pop_priority",
            2,
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
        )
        if not member:
            raise QueueEmptyException
        return await self.afind(uuid.UUID(self.decode(member, "ascii")))

    async def adiscard_priority(self, entry_id: uuid.UUID) -> None:
        await self._fcall(
            "django_queue_discard_priority",
            2,
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self.encode(str(entry_id), "ascii"),
        )

    async def ahas_pending(self) -> bool:
        client = self._async_redis()
        return bool(
            await client.llen(self._entry_pending_name)
            or await client.zcard(self._entry_delayed_name)
            or await client.zcard(self._entry_scheduled_name)
            or await client.zcard(self._entry_pending_priority_name)
        )

    async def aclaim(
        self, worker_id: uuid.UUID, lease_seconds: float | None = None
    ) -> QueueEntry:
        return await self._aclaim(worker_id, lease_seconds, expire_unclaimed=False)

    async def aclaim_unexpired(
        self, worker_id: uuid.UUID, lease_seconds: float | None = None
    ) -> QueueEntry:
        return await self._aclaim(worker_id, lease_seconds, expire_unclaimed=True)

    async def aclaim_priority(
        self, worker_id: uuid.UUID, lease_seconds: float | None = None
    ) -> QueueEntry:
        """Like `aclaim`, but for a priority-variant queue's own claim path.

        The FIFO Function only looks at the plain pending list, so it cannot
        see an entry pushed into the priority ZSET. The priority Function tries
        the plain list first (delayed-retry recoveries still land there) and
        falls back to the priority ZSET only when it is empty.
        """
        return await self._aclaim_priority(
            worker_id, lease_seconds, expire_unclaimed=False
        )

    async def aclaim_priority_unexpired(
        self, worker_id: uuid.UUID, lease_seconds: float | None = None
    ) -> QueueEntry:
        return await self._aclaim_priority(
            worker_id, lease_seconds, expire_unclaimed=True
        )

    async def adequeue(self) -> QueueEntry:
        """Atomically remove and return the next unclaimed live event."""
        self._async_redis()
        outcome, raw_entry = await self._fcall(
            "django_queue_dequeue_event",
            6,
            self._entry_pending_name,
            self._entry_delayed_name,
            self._entry_claim_prefix,
            self._entry_claim_deadlines_name,
            f"{self._queue_name}:entries:",
            self._entry_unclaimed_deadlines_name,
            b"1" if self._stack else b"0",
        )
        outcome = self.decode(outcome, "ascii")
        if outcome == "empty":
            raise QueueEmptyException
        if outcome != "dequeued":
            raise QueueValueError(f"Unknown Redis event dequeue outcome: {outcome!r}")
        return self.entry_class.from_dict(json.loads(raw_entry))

    async def _aclaim(
        self,
        worker_id: uuid.UUID,
        lease_seconds: float | None,
        *,
        expire_unclaimed: bool,
    ) -> QueueEntry:
        lease_seconds = (
            600.0 if lease_seconds is None else validate_budget(lease_seconds)
        )
        outcome, raw_entry_id = await self._fcall(
            "django_queue_claim",
            7,
            self._entry_pending_name,
            self._entry_delayed_name,
            self._entry_claim_prefix,
            self._entry_claim_deadlines_name,
            f"{self._queue_name}:entries:",
            self._entry_unclaimed_deadlines_name,
            self._entry_scheduled_name,
            self.encode(str(worker_id), "ascii"),
            self.encode(str(round(lease_seconds * MICROSECONDS_PER_SECOND)), "ascii"),
            b"1" if self._stack else b"0",
            b"1" if expire_unclaimed else b"0",
        )
        outcome = self.decode(outcome, "ascii")
        if outcome == "empty":
            raise QueueEmptyException
        entry_id = uuid.UUID(self.decode(raw_entry_id, "ascii"))
        if outcome == "conflict":
            raise QueueClaimConflictError(entry_id)
        if outcome == "expired":
            raise QueueEntryExpiredError(entry_id)
        if outcome != "claimed":
            raise QueueValueError(f"Unknown Redis claim outcome: {outcome!r}")
        try:
            return await self.afind(entry_id)
        except QueueEntryNotFoundError as exc:
            raise QueueEntryMissingError(entry_id) from exc

    async def _aclaim_priority(
        self,
        worker_id: uuid.UUID,
        lease_seconds: float | None,
        *,
        expire_unclaimed: bool,
    ) -> QueueEntry:
        lease_seconds = (
            600.0 if lease_seconds is None else validate_budget(lease_seconds)
        )
        outcome, raw_entry_id = await self._fcall(
            "django_queue_claim_priority",
            9,
            self._entry_pending_name,
            self._entry_delayed_name,
            self._entry_claim_prefix,
            self._entry_claim_deadlines_name,
            f"{self._queue_name}:entries:",
            self._entry_unclaimed_deadlines_name,
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            self._entry_scheduled_name,
            self.encode(str(worker_id), "ascii"),
            self.encode(str(round(lease_seconds * MICROSECONDS_PER_SECOND)), "ascii"),
            b"1" if self._stack else b"0",
            b"1" if expire_unclaimed else b"0",
            self.encode(str(_PRIORITY_SEQUENCE_SPACE), "ascii"),
        )
        outcome = self.decode(outcome, "ascii")
        if outcome == "empty":
            raise QueueEmptyException
        entry_id = uuid.UUID(self.decode(raw_entry_id, "ascii"))
        if outcome == "conflict":
            raise QueueClaimConflictError(entry_id)
        if outcome == "expired":
            raise QueueEntryExpiredError(entry_id)
        if outcome != "claimed":
            raise QueueValueError(f"Unknown Redis claim outcome: {outcome!r}")
        try:
            return await self.afind(entry_id)
        except QueueEntryNotFoundError as exc:
            raise QueueEntryMissingError(entry_id) from exc

    async def arenew(
        self, entry_id: uuid.UUID, worker_id: uuid.UUID, lease_seconds: float
    ) -> bool:
        validate_budget(lease_seconds)
        self._async_redis()
        return bool(
            await self._fcall(
                "django_queue_renew",
                2,
                self._claim_key(entry_id),
                self._entry_claim_deadlines_name,
                self.encode(str(worker_id), "ascii"),
                self.encode(
                    str(round(lease_seconds * MICROSECONDS_PER_SECOND)), "ascii"
                ),
                self.encode(str(entry_id), "ascii"),
            )
        )

    async def arelease(
        self, entry_id: uuid.UUID, worker_id: uuid.UUID, delay_seconds: float
    ) -> bool:
        validate_budget(delay_seconds)
        self._async_redis()
        return bool(
            await self._fcall(
                "django_queue_release",
                4,
                self._claim_key(entry_id),
                self._entry_claim_deadlines_name,
                self._entry_delayed_name,
                self._entry_unclaimed_deadlines_name,
                self.encode(str(worker_id), "ascii"),
                self.encode(str(entry_id), "ascii"),
                self.encode(
                    str(round(delay_seconds * MICROSECONDS_PER_SECOND)), "ascii"
                ),
            )
        )

    async def arelease_priority(
        self, entry_id: uuid.UUID, worker_id: uuid.UUID, delay_seconds: float
    ) -> bool:
        """Like `arelease`, but redelivers via the entry's priority score
        instead of the plain delayed set.

        `delay_seconds` is accepted for signature compatibility with
        `arelease` but not honoured: the priority ZSET has no "not yet due"
        concept the way the delayed set does.
        """
        validate_budget(delay_seconds)
        self._async_redis()
        return bool(
            await self._fcall(
                "django_queue_release_priority",
                7,
                self._claim_key(entry_id),
                self._entry_claim_deadlines_name,
                self._entry_delayed_name,
                self._entry_unclaimed_deadlines_name,
                self._entry_pending_priority_name,
                self._entry_key(entry_id),
                self._entry_pending_priority_sequence_name,
                self.encode(str(worker_id), "ascii"),
                self.encode(str(entry_id), "ascii"),
                self.encode(str(_PRIORITY_SEQUENCE_SPACE), "ascii"),
            )
        )

    async def aremove(self, entry_id: uuid.UUID, worker_id: uuid.UUID) -> bool:
        self._async_redis()
        return bool(
            await self._fcall(
                "django_queue_remove",
                6,
                self._claim_key(entry_id),
                self._entry_claim_deadlines_name,
                self._entry_delayed_name,
                self._entry_pending_name,
                self._entry_key(entry_id),
                self._entry_unclaimed_deadlines_name,
                self.encode(str(worker_id), "ascii"),
                self.encode(str(entry_id), "ascii"),
            )
        )

    async def aack(self, entry_id: uuid.UUID, worker_id: uuid.UUID) -> bool:
        self._async_redis()
        return bool(
            await self._fcall(
                "django_queue_ack",
                2,
                self._claim_key(entry_id),
                self._entry_claim_deadlines_name,
                self.encode(str(worker_id), "ascii"),
                self.encode(str(entry_id), "ascii"),
            )
        )

    async def amark_running(self, worker_id: uuid.UUID, entry: QueueEntry) -> bool:
        self._async_redis()
        return bool(
            await self._fcall(
                "django_queue_mark_running",
                2,
                self._claim_key(entry.id),
                self._entry_key(entry.id),
                self.encode(str(worker_id), "ascii"),
                self.encode(json.dumps(entry.to_dict()), "ascii"),
            )
        )

    async def asettle(self, worker_id: uuid.UUID, entry: QueueEntry) -> bool:
        self._async_redis()
        return bool(
            await self._fcall(
                "django_queue_settle",
                3,
                self._claim_key(entry.id),
                self._entry_claim_deadlines_name,
                self._entry_key(entry.id),
                self.encode(str(worker_id), "ascii"),
                self.encode(str(entry.id), "ascii"),
                self.encode(json.dumps(entry.to_dict()), "ascii"),
            )
        )

    async def arecover(self, batch_size: int) -> tuple[int, int]:
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("Recovery batch size must be a positive integer")
        recovered, discarded = await self._fcall(
            "django_queue_recover",
            5,
            self._entry_claim_deadlines_name,
            self._entry_claim_prefix,
            self._entry_pending_name,
            f"{self._queue_name}:entries:",
            self._entry_unclaimed_deadlines_name,
            b"1" if self._stack else b"0",
            self.encode(str(batch_size), "ascii"),
        )
        return int(recovered), int(discarded)

    async def arecover_priority(self, batch_size: int) -> tuple[int, int]:
        """Like `arecover`, but redelivers a recovered entry via its
        priority score instead of the plain pending list."""
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("Recovery batch size must be a positive integer")
        recovered, discarded = await self._fcall(
            "django_queue_recover_priority",
            7,
            self._entry_claim_deadlines_name,
            self._entry_claim_prefix,
            self._entry_pending_name,
            f"{self._queue_name}:entries:",
            self._entry_unclaimed_deadlines_name,
            self._entry_pending_priority_name,
            self._entry_pending_priority_sequence_name,
            b"1" if self._stack else b"0",
            self.encode(str(batch_size), "ascii"),
            self.encode(str(_PRIORITY_SEQUENCE_SPACE), "ascii"),
        )
        return int(recovered), int(discarded)

    async def aprune(self, entry_id: uuid.UUID) -> QueueEntry:
        entry = await self.afind(entry_id)
        if QueueEntryStatus.TERMINATED not in entry.status.next_state():
            raise ValueError("Only terminal queue entries can be pruned")
        self._async_redis()
        outcome = await self._fcall(
            "django_queue_prune",
            2,
            self._entry_key(entry_id),
            self._entry_pending_name,
            self.encode(str(entry_id), "ascii"),
        )
        if outcome == 0:
            raise QueueEntryNotFoundError(entry_id)
        if outcome == -1:
            raise ValueError("Only terminal queue entries can be pruned")
        return self.entry_class.from_dict(json.loads(outcome))

    def decode_notification(self, raw: object) -> QueueEntry:
        """Decode a published notification payload into an entry record."""
        if isinstance(raw, bytes | bytearray | memoryview):
            raw = bytes(raw).decode("ascii")
        if not isinstance(raw, str):
            raise QueueEncodingException(
                f"Notification payload must be text or bytes, not {type(raw).__name__}"
            )
        return self.entry_class.from_dict(json.loads(raw))

    async def astore_notification(self, entry: QueueEntry) -> None:
        """Store a notification, index its deadline, and publish it.

        One Function writes the payload, ZADDs the sender-set deadline, then
        PUBLISHes so every connected receiver can see it. There is no pending
        claim list.
        """
        if entry.timeout_seconds is None:
            raise ValueError("Notification entries require a resolved lifetime")
        self._async_redis()
        await self._fcall(
            "django_queue_notification_store",
            2,
            self._notification_entry_key(entry.id),
            self._notification_deadlines_name,
            self.encode(json.dumps(entry.to_dict()), "ascii"),
            self.encode(str(entry.id), "ascii"),
            self.encode(
                str(round(entry.timeout_seconds * MICROSECONDS_PER_SECOND)),
                "ascii",
            ),
            self.notification_channel,
        )

    async def aget_notification(self, entry_id: uuid.UUID) -> QueueEntry:
        raw = await self._fcall(
            "django_queue_notification_get",
            2,
            self._notification_lease_key(entry_id),
            self._notification_entry_key(entry_id),
        )
        if raw is None:
            raise QueueEntryNotFoundError(entry_id)
        return self.decode_notification(raw)

    async def ahas_notification(self) -> bool:
        return bool(await self._async_redis().zcard(self._notification_deadlines_name))

    async def aexpire_due_notifications(self) -> list[uuid.UUID]:
        raw_entry_id = await self._fcall(
            "django_queue_notification_expire",
            3,
            self._notification_deadlines_name,
            self._notification_lease_prefix,
            self._notification_entry_prefix,
            self.encode(str(_NOTIFICATION_REMOVAL_LEASE_MS), "ascii"),
        )
        if not raw_entry_id:
            return []
        return [uuid.UUID(self.decode(raw_entry_id, "ascii"))]

    async def aclear_notifications(self) -> None:
        await self._fcall(
            "django_queue_notification_clear",
            3,
            self._notification_deadlines_name,
            self._notification_lease_prefix,
            self._notification_entry_prefix,
        )

    async def aclose(self) -> None:
        loop = asyncio.get_running_loop()
        self._function_compatibility_by_loop.discard(loop)
        if clock := self._clocks_by_loop.pop(loop, None):
            await clock.aclose()
        if client := self._async_redis_by_loop.pop(loop, None):
            await self._aclose_client(client)


class QueueProviderRedisCluster(QueueProviderRedis):
    """Redis Cluster provider using redis-py's asyncio Cluster client.

    Queue operations stay on :class:`QueueProviderRedis`. This subclass only
    constructs and closes a Cluster client, rejects a non-zero database, and
    routes Function introspection to the queue's hash slot.
    """

    def __init__(
        self,
        redis_url: str,
        options: dict | None = None,
        *,
        entry_class: type[QueueEntry],
        **kwargs,
    ) -> None:
        options = {} if options is None else options
        options = dict(options) | kwargs
        address_remap = options.pop("address_remap", None)
        super().__init__(redis_url, options, entry_class=entry_class)
        self._address_remap = address_remap
        try:
            connection_kwargs = redis.connection.parse_url(self._redis_url)
        except (AttributeError, ValueError) as exc:
            raise InvalidQueueBackendError(f"Redis URL is invalid: {exc}") from exc
        database = int(connection_kwargs.get("db") or 0)
        if database != 0:
            raise InvalidQueueBackendError(
                "Redis Cluster queues require database 0; "
                f"the configured seed URL selects database {database}"
            )

    def _create_async_client(self) -> Any:
        kwargs = dict(self._client_kwargs)
        if self._address_remap is not None:
            kwargs["address_remap"] = self._address_remap
        return AsyncRedisCluster.from_url(
            redis_from_url_location(self._redis_url), **kwargs
        )

    def _function_info_keys(self) -> tuple[str, ...]:
        return (self._queue_name,)
