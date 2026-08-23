import asyncio

import pytest
import redis

from django_queue.backends.exceptions import InvalidQueueBackendError
from django_queue.backends.redis import functions
from django_queue.backends.redis.functions import (
    FUNCTION_API_VERSION,
    FUNCTION_LIBRARY_NAME,
    FUNCTION_LIBRARY_VERSION,
    load_function_library,
)
from django_queue.backends.redis.provider import QueueProviderRedis
from django_queue.entries import QueueEntry

_FUNCTION_NAMES = (
    "django_queue_info",
    "django_queue_store_and_push",
    "django_queue_push_priority",
    "django_queue_pop_priority",
    "django_queue_discard_priority",
    "django_queue_promote_scheduled",
    "django_queue_promote_scheduled_priority",
    "django_queue_dequeue",
    "django_queue_dequeue_priority",
    "django_queue_store_and_push_priority",
    "django_queue_store_available",
    "django_queue_store_and_discard",
    "django_queue_store_event_and_push",
    "django_queue_dequeue_event",
    "django_queue_renew",
    "django_queue_release",
    "django_queue_release_priority",
    "django_queue_remove",
    "django_queue_ack",
    "django_queue_mark_running",
    "django_queue_settle",
    "django_queue_expire",
    "django_queue_prune",
    "django_queue_delete",
    "django_queue_recover",
    "django_queue_recover_priority",
    "django_queue_claim",
    "django_queue_claim_priority",
)


def test_loads_the_function_library_from_its_package_resource():
    library = load_function_library()

    assert library.name == FUNCTION_LIBRARY_NAME == "django_queues"
    assert library.library_version == FUNCTION_LIBRARY_VERSION
    assert library.api_version == FUNCTION_API_VERSION == 1
    assert library.source.startswith(b"#!lua name=django_queues\n")
    assert b"register_function('django_queue_info', 'Keys: none." in library.source


def test_rejects_a_bundled_library_resource_with_invalid_metadata(monkeypatch):
    class Resource:
        def read_bytes(self):
            return b"#!lua name=django_queues\nredis.register_function('test', function() end)"

    class PackageFiles:
        def joinpath(self, name):
            assert name == "library.lua"
            return Resource()

    monkeypatch.setattr(functions.resources, "files", lambda package: PackageFiles())
    functions.load_function_library.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="invalid metadata"):
            functions.load_function_library()
    finally:
        functions.load_function_library.cache_clear()


def test_redis_function_library_fixture_loads_stable_introspection(
    redis_raw_client, redis_function_library
):
    assert redis_raw_client.fcall("django_queue_info", 0) == [
        FUNCTION_LIBRARY_VERSION.encode("ascii"),
        FUNCTION_API_VERSION,
    ]


def test_library_documents_its_recovery_entry_points():
    source = load_function_library().source

    assert b"function_name = 'django_queue_recover'" in source
    assert b"function_name = 'django_queue_recover_priority'" in source
    assert b"Keys: claim-deadline ZSET" in source
    assert b"Args: stack flag, recovery batch size" in source


def test_library_documents_its_fifo_claim_entry_point():
    source = load_function_library().source

    assert b"function_name = 'django_queue_claim'" in source
    assert (
        b"Returns: claimed, conflict, expired, or empty outcome and entry ID" in source
    )


def test_library_documents_its_priority_claim_entry_point():
    source = load_function_library().source

    assert b"function_name = 'django_queue_claim_priority'" in source
    assert b"priority ZSET and sequence" in source


def test_library_documents_its_fifo_scheduled_promotion_entry_point():
    source = load_function_library().source

    assert b"function_name = 'django_queue_promote_scheduled'" in source
    assert (
        b"Returns: no value after promoting at most one due availability group"
        in source
    )


def test_library_documents_its_priority_scheduled_promotion_entry_point():
    source = load_function_library().source

    assert b"function_name = 'django_queue_promote_scheduled_priority'" in source
    assert b"priority ZSET, priority sequence" in source


def test_library_has_one_priority_score_insertion_helper():
    source = load_function_library().source

    assert (
        b"local function push_priority(priority_key, sequence_key, base_score, entry_id)"
        in source
    )


def test_library_documents_every_public_function_registration():
    source = load_function_library().source.decode("utf-8")

    assert "redis.register_function('" not in source
    for name in _FUNCTION_NAMES:
        assert (
            f"function_name = '{name}'" in source
            or f"register_function('{name}', 'Keys:" in source
        )
    assert source.count("description = 'Keys:") + source.count("', 'Keys:") == len(
        _FUNCTION_NAMES
    )
    assert source.count(". Args:") == len(_FUNCTION_NAMES)
    assert source.count(". Returns:") == len(_FUNCTION_NAMES)


def test_provider_does_not_create_an_evalsha_script_cache():
    provider = QueueProviderRedis("redis://localhost:6379/0", entry_class=QueueEntry)

    assert not hasattr(provider, "_async_scripts_by_loop")


def test_provider_checks_function_compatibility_once_per_loop_before_fcall():
    class Client:
        def __init__(self):
            self.calls = []

        async def fcall(self, function, numkeys, *args):
            self.calls.append((function, numkeys, args))
            if function == "django_queue_info":
                return [b"260822_160000", 1]
            return "result"

    async def exercise():
        provider = QueueProviderRedis(
            "redis://localhost:6379/0", entry_class=QueueEntry
        )
        client = Client()
        provider._async_redis_by_loop[asyncio.get_running_loop()] = client

        assert await provider._fcall("first_operation", 0) == "result"
        assert await provider._fcall("second_operation", 0) == "result"
        return client.calls

    assert asyncio.run(exercise()) == [
        ("django_queue_info", 0, ()),
        ("first_operation", 0, ()),
        ("second_operation", 0, ()),
    ]


def test_provider_reports_unavailable_function_library_with_next_steps():
    class Client:
        async def fcall(self, function, numkeys, *args):
            raise redis.ResponseError("ERR Function not found")

    async def exercise():
        provider = QueueProviderRedis(
            "redis://localhost:6379/0", entry_class=QueueEntry
        )
        provider._async_redis_by_loop[asyncio.get_running_loop()] = Client()
        with pytest.raises(InvalidQueueBackendError) as error:
            await provider._fcall("django_queue_claim", 0)
        return str(error.value)

    message = asyncio.run(exercise())

    assert "redis_lua_compat" in message
    assert "redis_lua_lib --deploy" in message


def test_provider_rejects_malformed_function_library_introspection():
    class Client:
        async def fcall(self, function, numkeys, *args):
            return [42, 1]

    async def exercise():
        provider = QueueProviderRedis(
            "redis://localhost:6379/0", entry_class=QueueEntry
        )
        provider._async_redis_by_loop[asyncio.get_running_loop()] = Client()
        with pytest.raises(InvalidQueueBackendError) as error:
            await provider._fcall("django_queue_claim", 0)
        return str(error.value)

    assert "invalid introspection data" in asyncio.run(exercise())


def test_provider_reports_the_function_when_an_operation_is_denied():
    class Client:
        async def fcall(self, function, numkeys, *args):
            if function == "django_queue_info":
                return [b"260822_160000", 1]
            raise redis.ResponseError("NOPERM this user has no permissions")

    async def exercise():
        provider = QueueProviderRedis(
            "redis://localhost:6379/0", entry_class=QueueEntry
        )
        provider._async_redis_by_loop[asyncio.get_running_loop()] = Client()
        with pytest.raises(InvalidQueueBackendError) as error:
            await provider._fcall("django_queue_claim", 0)
        return str(error.value)

    assert asyncio.run(exercise()) == (
        "Redis Function django_queue_claim failed: NOPERM this user has no permissions"
    )
