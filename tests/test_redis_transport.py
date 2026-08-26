import asyncio
import ssl
from types import SimpleNamespace

import pytest
import redis
import redis.asyncio as async_redis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

from django_queue.backends.exceptions import InvalidQueueBackendError
from django_queue.backends.redis import RedisAsyncQueue, RedisClusterAsyncQueue
from django_queue.backends.redis.provider import QueueProviderRedis
from django_queue.backends.redis.transport import (
    redact_redis_url,
    redis_client_kwargs,
    redis_tls_failure,
)
from django_queue.clock import QueueClockError
from django_queue.entries import QueueEntry


def test_rediss_url_enables_tls_without_options():
    kwargs = redis_client_kwargs("rediss://localhost:6379/0")

    assert "ssl" not in kwargs
    assert redis_client_kwargs("redis://localhost:6379/0") == {}


def test_rediss_ssl_true_option_is_not_passed_to_from_url():
    kwargs = redis_client_kwargs(
        "rediss://localhost:6379/0",
        {"ssl": True, "ssl_ca_certs": "/tmp/ca.pem"},
    )

    assert "ssl" not in kwargs
    assert kwargs["ssl_ca_certs"] == "/tmp/ca.pem"


def test_rediss_ssl_false_option_is_a_configuration_error():
    with pytest.raises(InvalidQueueBackendError, match="ssl=False"):
        redis_client_kwargs("rediss://localhost:6379/0", {"ssl": False})


def test_rediss_ssl_false_url_query_is_a_configuration_error():
    with pytest.raises(InvalidQueueBackendError, match="URL query"):
        redis_client_kwargs("rediss://localhost:6379/0?ssl=false")


def test_rediss_options_override_url_query_ssl_keys():
    kwargs = redis_client_kwargs(
        "rediss://localhost:6379/0?ssl_cert_reqs=none&ssl_ca_certs=/url/ca.pem",
        {
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": "/options/ca.pem",
            "ssl_check_hostname": True,
        },
    )

    assert kwargs["ssl_cert_reqs"] == "required"
    assert kwargs["ssl_ca_certs"] == "/options/ca.pem"
    assert kwargs["ssl_check_hostname"] is True


def test_redis_url_with_tls_options_is_a_configuration_error():
    with pytest.raises(InvalidQueueBackendError, match="rediss://"):
        redis_client_kwargs(
            "redis://localhost:6379/0",
            {"ssl_ca_certs": "/tmp/ca.pem"},
        )


def test_redis_url_with_tls_query_is_a_configuration_error():
    with pytest.raises(InvalidQueueBackendError, match="rediss://"):
        redis_client_kwargs("redis://localhost:6379/0?ssl_ca_certs=/tmp/ca.pem")


def test_plaintext_redis_url_keeps_non_tls_options_out_of_client_kwargs():
    kwargs = redis_client_kwargs(
        "redis://localhost:6379/12",
        {"maxsize": 8, "encoding": "utf-8", "address_remap": lambda address: address},
    )

    assert kwargs == {}


def test_standalone_rediss_queue_constructs_a_tls_client():
    queue = RedisAsyncQueue(
        "rediss://localhost:6379/0",
        queue_name="tls-standalone",
        ssl_ca_certs="/tmp/ca.pem",
        ssl_check_hostname=True,
    )
    client = queue._provider._create_async_client()
    try:
        pool_kwargs = client.connection_pool.connection_kwargs
        assert pool_kwargs.get("ssl_ca_certs") == "/tmp/ca.pem"
        assert "SSL" in client.connection_pool.connection_class.__name__
    finally:
        asyncio.run(queue._provider._aclose_client(client))


def test_cluster_rediss_seed_constructs_a_tls_client():
    queue = RedisClusterAsyncQueue(
        "rediss://localhost:6379/0",
        queue_name="tls-cluster",
        ssl_ca_certs="/tmp/ca.pem",
    )
    client = queue._provider._create_async_client()
    try:
        assert isinstance(client, AsyncRedisCluster)
        kwargs = client.connection_kwargs
        assert kwargs.get("ssl_ca_certs") == "/tmp/ca.pem"
        assert kwargs.get("ssl") or "SSL" in getattr(
            kwargs.get("connection_class"), "__name__", ""
        )
    finally:
        asyncio.run(queue._provider._aclose_client(client))


def test_plaintext_redis_url_still_constructs_a_non_tls_client():
    provider = QueueProviderRedis(
        "redis://localhost:6379/12", queue_name="plain", entry_class=QueueEntry
    )
    client = provider._create_async_client()
    try:
        assert type(client) is type(async_redis.from_url("redis://localhost:6379/12"))
        assert "SSL" not in client.connection_pool.connection_class.__name__
    finally:
        asyncio.run(provider._aclose_client(client))


def test_aobserve_opens_a_client_with_the_same_tls_settings_as_the_provider():
    provider = QueueProviderRedis(
        "rediss://localhost:6379/0",
        {"ssl_ca_certs": "/tmp/observer-ca.pem", "queue_name": "obs"},
        entry_class=QueueEntry,
    )
    created = []

    class FakePubSub:
        async def subscribe(self, channel):
            return None

        async def listen(self):
            if False:
                yield None

        async def aclose(self):
            return None

    class FakeClient:
        def __init__(self, kwargs):
            self.connection_pool = SimpleNamespace(connection_kwargs=kwargs)

        def pubsub(self, ignore_subscribe_messages=True):
            return FakePubSub()

        async def aclose(self, close_connection_pool=True):
            return None

    def create():
        kwargs = dict(provider._client_kwargs)
        created.append(kwargs)
        return FakeClient(kwargs)

    provider._create_async_client = create
    asyncio.run(provider.aobserve(lambda entry: None))

    assert created[0]["ssl_ca_certs"] == "/tmp/observer-ca.pem"


def test_aobserve_remaps_tls_handshake_failures():
    provider = QueueProviderRedis(
        "rediss://localhost:6379/0",
        {"ssl_ca_certs": "/tmp/observer-ca.pem", "queue_name": "obs"},
        entry_class=QueueEntry,
    )

    class FakePubSub:
        async def subscribe(self, channel):
            raise ssl.SSLError("certificate verify failed")

        async def aclose(self):
            return None

    class FakeClient:
        def pubsub(self, ignore_subscribe_messages=True):
            return FakePubSub()

        async def aclose(self, close_connection_pool=True):
            return None

    provider._create_async_client = lambda: FakeClient()

    with pytest.raises(InvalidQueueBackendError, match="TLS handshake failed"):
        asyncio.run(provider.aobserve(lambda entry: None))


def test_tls_handshake_error_is_actionable():
    error = redis_tls_failure(
        ssl.SSLError("certificate verify failed"),
        "rediss://localhost:6379/0",
    )

    assert error is not None
    assert "TLS handshake failed" in str(error)
    assert "rediss://" in str(error)


def test_tls_error_redacts_userinfo_and_query_string():
    error = redis_tls_failure(
        ssl.SSLError("certificate verify failed"),
        "rediss://alice:example-userinfo@host:6379/0?ssl_ca_certs=/tmp/ca.pem",
    )

    assert error is not None
    message = str(error)
    assert "example-userinfo" not in message
    assert "ssl_ca_certs" not in message
    assert "ca.pem" not in message
    assert "rediss://host:6379/0" in message


def test_redact_redis_url_strips_userinfo_and_query_string():
    assert (
        redact_redis_url(
            "rediss://alice:example-userinfo@host:6379/0?ssl_ca_certs=/tmp/ca.pem"
        )
        == "rediss://host:6379/0"
    )
    assert redact_redis_url("redis://localhost:6379/0") == "redis://localhost:6379/0"


def test_rediss_connect_timeout_is_a_tls_failure():
    error = redis_tls_failure(
        redis.TimeoutError("Timeout connecting to server"),
        "rediss://localhost:6379/0",
    )

    assert error is not None
    assert "Could not establish a TLS connection" in str(error)
    assert "handshake" not in str(error).lower()


def test_plaintext_connect_timeout_is_not_a_tls_failure():
    assert (
        redis_tls_failure(
            redis.TimeoutError("Timeout connecting to server"),
            "redis://localhost:6379/0",
        )
        is None
    )


def test_rediss_clock_timeout_is_a_tls_failure():
    try:
        raise QueueClockError("Redis TIME is unavailable") from redis.TimeoutError(
            "Timeout connecting to server"
        )
    except QueueClockError as exc:
        error = redis_tls_failure(exc, "rediss://localhost:6379/0")

    assert error is not None
    assert "Could not establish a TLS connection" in str(error)


def test_rediss_authentication_error_is_not_a_tls_failure():
    assert (
        redis_tls_failure(
            redis.AuthenticationError("invalid username-password pair"),
            "rediss://localhost:6379/0",
        )
        is None
    )


def test_rediss_busy_loading_error_is_not_a_tls_failure():
    assert (
        redis_tls_failure(
            redis.BusyLoadingError("Redis is loading the dataset in memory"),
            "rediss://localhost:6379/0",
        )
        is None
    )


def test_rediss_read_timeout_is_not_a_tls_failure():
    assert (
        redis_tls_failure(
            redis.TimeoutError("Timeout reading from socket"),
            "rediss://localhost:6379/0",
        )
        is None
    )
