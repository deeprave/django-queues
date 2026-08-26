import asyncio
from io import StringIO
from types import SimpleNamespace
from typing import ClassVar
from uuid import uuid4

import pytest
import redis
from django.core.management.base import CommandError
from django.utils.module_loading import import_string
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

import django_queue
from django_queue.backends.exceptions import (
    InvalidQueueBackendError,
    QueueEmptyException,
    QueueEntryNotFoundError,
)
from django_queue.backends.redis import (
    RedisAsyncQueue,
    RedisClusterAsyncPriorityQueue,
    RedisClusterAsyncPriorityQueueJson,
    RedisClusterAsyncQueue,
    RedisClusterAsyncQueueJson,
    RedisClusterAsyncStack,
    RedisClusterAsyncStackJson,
    RedisClusterEventQueue,
)
from django_queue.backends.redis.provider import (
    QueueProviderRedis,
    QueueProviderRedisCluster,
)
from django_queue.entries import QueueEntry, QueueEntryStatus
from django_queue.management.commands.redis_lua_compat import Command as CompatCommand
from django_queue.management.commands.redis_lua_lib import Command as LibCommand
from django_queue.management.redis_functions import (
    REDIS_TOPOLOGY_CLUSTER,
    REDIS_TOPOLOGY_STANDALONE,
    RedisFunctionTarget,
    _lock_key_for_slot,
    iter_cluster_primary_clients,
    parse_cluster_node_ids,
    resolve_redis_targets,
    warn_duplicate_cluster_seeds,
)

_CLUSTER_BACKENDS = (
    "django_queue.backends.redis.RedisClusterAsyncQueue",
    "django_queue.backends.redis.RedisClusterAsyncQueueJson",
    "django_queue.backends.redis.RedisClusterAsyncStack",
    "django_queue.backends.redis.RedisClusterAsyncStackJson",
    "django_queue.backends.redis.RedisClusterAsyncPriorityQueue",
    "django_queue.backends.redis.RedisClusterAsyncPriorityQueueJson",
    "django_queue.backends.redis.RedisClusterEventQueue",
)


def _cluster_queue(cls, url, remap, **kwargs):
    name = kwargs.pop("queue_name", f"q{uuid4().hex}")
    return cls(url, queue_name=name, address_remap=remap, **kwargs)


@pytest.mark.parametrize("backend", _CLUSTER_BACKENDS)
def test_cluster_backends_resolve_from_queue_configuration(backend, no_runtime_startup):
    cls = import_string(backend)
    registry = django_queue.QueueRegistry(
        {
            "jobs": {
                "BACKEND": backend,
                "LOCATION": "redis://localhost:6379/0",
            }
        }
    )

    queue = registry["jobs"]

    assert isinstance(queue, cls)
    assert isinstance(queue._provider, QueueProviderRedisCluster)
    assert queue.redis_topology == "cluster"
    assert queue.worker_provider_kind == "redis"


def test_standalone_backends_do_not_use_the_cluster_provider(no_runtime_startup):
    registry = django_queue.QueueRegistry(
        {
            "jobs": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": "redis://localhost:6379/12",
            }
        }
    )

    queue = registry["jobs"]

    assert isinstance(queue, RedisAsyncQueue)
    assert type(queue._provider) is QueueProviderRedis
    assert queue.redis_topology == "standalone"


def test_cluster_provider_constructs_a_cluster_client_and_closes_it():
    async def exercise():
        provider = QueueProviderRedisCluster(
            "redis://localhost:6379/0",
            queue_name="cluster-close",
            entry_class=QueueEntry,
        )
        client = provider._create_async_client()
        assert isinstance(client, AsyncRedisCluster)
        assert not isinstance(client, AsyncRedis)
        closed = []

        class Observer:
            async def aclose(self, *args, **kwargs):
                closed.append(True)

        provider._async_redis_by_loop[asyncio.get_running_loop()] = Observer()
        await provider.aclose()
        await provider._aclose_client(client)
        return closed

    assert asyncio.run(exercise()) == [True]


def test_standalone_provider_still_constructs_a_standalone_client():
    provider = QueueProviderRedis(
        "redis://localhost:6379/12", queue_name="standalone", entry_class=QueueEntry
    )
    client = provider._create_async_client()
    try:
        assert type(client).__name__ == "Redis"
        assert not isinstance(client, AsyncRedisCluster)
    finally:
        asyncio.run(provider._aclose_client(client))


def test_cluster_backend_rejects_a_non_zero_database():
    with pytest.raises(InvalidQueueBackendError, match="database 0"):
        RedisClusterAsyncQueue("redis://localhost:6379/1", queue_name="jobs")


def test_cluster_backend_accepts_database_zero():
    queue = RedisClusterAsyncQueue("redis://localhost:6379/0", queue_name="jobs")

    assert isinstance(queue._provider, QueueProviderRedisCluster)


def test_cluster_backend_treats_a_url_without_a_database_as_zero():
    queue = RedisClusterAsyncQueue("redis://localhost:6379", queue_name="jobs")

    assert isinstance(queue._provider, QueueProviderRedisCluster)


def test_configured_cluster_alias_rejects_a_non_zero_database(no_runtime_startup):
    registry = django_queue.QueueRegistry(
        {
            "jobs": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                "LOCATION": "redis://localhost:6379/1",
            }
        }
    )

    with pytest.raises(InvalidQueueBackendError, match="database 0"):
        registry["jobs"]


def test_resolve_redis_targets_separates_standalone_and_cluster_locations():
    targets = resolve_redis_targets(
        {
            "first": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": "redis://standalone/0",
            },
            "clustered": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                "LOCATION": "redis://cluster/0",
            },
            "same-cluster": {
                "BACKEND": "django_queue.backends.redis.RedisClusterEventQueue",
                "LOCATION": "redis://cluster/0",
            },
            "other-cluster": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncStack",
                "LOCATION": "redis://cluster-b/0",
            },
        }
    )

    assert targets == (
        RedisFunctionTarget(
            "redis://standalone/0", REDIS_TOPOLOGY_STANDALONE, ("first",)
        ),
        RedisFunctionTarget(
            "redis://cluster/0",
            REDIS_TOPOLOGY_CLUSTER,
            ("clustered", "same-cluster"),
        ),
        RedisFunctionTarget(
            "redis://cluster-b/0", REDIS_TOPOLOGY_CLUSTER, ("other-cluster",)
        ),
    )


def test_redis_url_override_ignores_configured_cluster_aliases():
    targets = resolve_redis_targets(
        {
            "clustered": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                "LOCATION": "redis://cluster/0",
            },
            "standalone": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": "redis://standalone/0",
            },
        },
        redis_url="redis://override/0",
    )

    assert targets == (
        RedisFunctionTarget("redis://override/0", REDIS_TOPOLOGY_STANDALONE),
    )


def test_redis_cluster_url_override_ignores_configured_standalone_aliases():
    targets = resolve_redis_targets(
        {
            "clustered": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                "LOCATION": "redis://cluster/0",
            },
            "standalone": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": "redis://standalone/0",
            },
        },
        redis_cluster_url="redis://cluster-override/0",
    )

    assert targets == (
        RedisFunctionTarget("redis://cluster-override/0", REDIS_TOPOLOGY_CLUSTER),
    )


def test_redis_url_and_cluster_url_overrides_are_mutually_exclusive():
    with pytest.raises(CommandError, match="mutually exclusive"):
        resolve_redis_targets(
            {}, redis_url="redis://a/0", redis_cluster_url="redis://b/0"
        )


def test_resolve_redis_targets_carries_configured_address_remap():
    def remap(address):
        return address

    targets = resolve_redis_targets(
        {
            "jobs": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                "LOCATION": "redis://cluster/0",
                "address_remap": remap,
            }
        }
    )

    assert targets == (
        RedisFunctionTarget(
            "redis://cluster/0",
            REDIS_TOPOLOGY_CLUSTER,
            ("jobs",),
            remap,
        ),
    )


def test_parse_cluster_node_ids_from_cluster_nodes_text():
    nodes = (
        "07c37dfeb235213a872192d90877d0cd35411309 127.0.0.1:7000@17000 master - 0 0 1 connected 0-5460\n"
        "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:7001@17001 master - 0 0 2 connected 5461-10922\n"
    )

    assert parse_cluster_node_ids(nodes) == {
        "07c37dfeb235213a872192d90877d0cd35411309",
        "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca",
    }


@pytest.mark.parametrize("slot", [0, 5460, 5461, 10922, 10923, 16383])
def test_cluster_deployment_lock_key_hashes_to_the_target_slot(slot):
    from redis.crc import key_slot

    assert key_slot(_lock_key_for_slot(slot).encode()) == slot


class _Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port


class _FakeCluster:
    def __init__(self, primaries):
        self._primaries = primaries
        self.closed = False

    def get_primaries(self):
        return list(self._primaries)

    def close(self):
        self.closed = True

    def execute_command(self, *args):
        return ""


class _NodeRedis:
    clients: ClassVar[list] = []

    def __init__(self, *, host, port, **kwargs):
        self.host = host
        self.port = port
        self.kwargs = kwargs
        self.loads = 0
        self.closed = False
        type(self).clients.append(self)

    def set(self, *args, **kwargs):
        return True

    def function_list(self, *, library, withcode=False):
        return []

    def function_load(self, source, *, replace):
        self.loads += 1
        self.source = source
        self.replace = replace

    def eval(self, *args):
        pass

    def close(self):
        self.closed = True

    def fcall(self, function, numkeys, *args):
        self.function = function
        self.numkeys = numkeys
        self.args = args
        return [b"260822_160000", 1]


def test_libcheck_deploys_the_library_to_every_cluster_primary(monkeypatch):
    _NodeRedis.clients = []
    primaries = [
        _Node("10.0.0.1", 7000),
        _Node("10.0.0.2", 7001),
        _Node("10.0.0.3", 7002),
    ]
    cluster = _FakeCluster(primaries)
    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_lib.RedisCluster.from_url",
        lambda url, **kwargs: cluster,
    )
    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", _NodeRedis
    )

    LibCommand(stdout=StringIO()).handle(
        deploy=True, redis_url=None, redis_cluster_url="redis://cluster/0"
    )

    assert [
        (client.host, client.port, client.loads) for client in _NodeRedis.clients
    ] == [
        ("10.0.0.1", 7000, 1),
        ("10.0.0.2", 7001, 1),
        ("10.0.0.3", 7002, 1),
    ]
    assert all(client.closed for client in _NodeRedis.clients)
    assert cluster.closed is True


def test_libcheck_redeploy_loads_the_library_on_a_new_primary(monkeypatch):
    _NodeRedis.clients = []
    first = [_Node("10.0.0.1", 7000), _Node("10.0.0.2", 7001)]
    second = first + [_Node("10.0.0.3", 7002)]
    clusters = [_FakeCluster(first), _FakeCluster(second)]
    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_lib.RedisCluster.from_url",
        lambda url, **kwargs: clusters.pop(0),
    )
    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", _NodeRedis
    )

    LibCommand(stdout=StringIO()).handle(
        deploy=True, redis_url=None, redis_cluster_url="redis://cluster/0"
    )
    LibCommand(stdout=StringIO()).handle(
        deploy=True, redis_url=None, redis_cluster_url="redis://cluster/0"
    )

    loaded = [(client.host, client.port) for client in _NodeRedis.clients]
    assert loaded.count(("10.0.0.3", 7002)) == 1
    assert loaded.count(("10.0.0.1", 7000)) == 2


def test_compat_fcalls_info_on_every_cluster_primary(monkeypatch):
    _NodeRedis.clients = []
    primaries = [
        _Node("10.0.0.1", 7000),
        _Node("10.0.0.2", 7001),
        _Node("10.0.0.3", 7002),
    ]
    cluster = _FakeCluster(primaries)
    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_compat.RedisCluster.from_url",
        lambda url, **kwargs: cluster,
    )
    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", _NodeRedis
    )
    configured = django_queue.QueueRegistry(
        {
            "jobs": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                "LOCATION": "redis://cluster/0",
            }
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)

    CompatCommand(stdout=StringIO()).handle(redis_url=None, redis_cluster_url=None)

    assert [
        (client.host, client.port, client.function, client.numkeys, client.args)
        for client in _NodeRedis.clients
    ] == [
        ("10.0.0.1", 7000, "django_queue_info", 0, ()),
        ("10.0.0.2", 7001, "django_queue_info", 0, ()),
        ("10.0.0.3", 7002, "django_queue_info", 0, ()),
    ]
    assert all(client.closed for client in _NodeRedis.clients)
    assert cluster.closed is True


def test_cluster_provider_does_not_create_an_evalsha_script_cache():
    provider = QueueProviderRedisCluster(
        "redis://localhost:6379/0", queue_name="jobs", entry_class=QueueEntry
    )

    assert not hasattr(provider, "_async_scripts_by_loop")


def test_cluster_provider_checks_function_compatibility_with_the_queue_key():
    class Client:
        def __init__(self):
            self.calls = []

        async def fcall(self, function, numkeys, *args):
            self.calls.append((function, numkeys, args))
            if function == "django_queue_info":
                return [b"260822_160000", 1]
            return "result"

    async def exercise():
        provider = QueueProviderRedisCluster(
            "redis://localhost:6379/0",
            queue_name="email-outbound",
            entry_class=QueueEntry,
        )
        client = Client()
        provider._async_redis_by_loop[asyncio.get_running_loop()] = client
        await provider._fcall("django_queue_store_and_push", 2, "k1", "k2")
        return client.calls

    assert asyncio.run(exercise()) == [
        ("django_queue_info", 1, ("{email-outbound}",)),
        ("django_queue_store_and_push", 2, ("k1", "k2")),
    ]


def test_libcheck_warns_when_two_seeds_share_cluster_node_ids(monkeypatch):
    _NodeRedis.clients = []
    node_ids = (
        "07c37dfeb235213a872192d90877d0cd35411309 127.0.0.1:7000@17000 master - 0 0 1 connected 0-5460\n"
        "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:7001@17001 master - 0 0 2 connected 5461-10922\n"
    )

    class SharedCluster(_FakeCluster):
        def execute_command(self, *args):
            return node_ids

        def get_primaries(self):
            return [_Node("10.0.0.1", 7000)]

    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_lib.RedisCluster.from_url",
        lambda url, **kwargs: SharedCluster([]),
    )
    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", _NodeRedis
    )
    stderr = StringIO()
    command = LibCommand(stdout=StringIO(), stderr=stderr)
    configured = django_queue.QueueRegistry(
        {
            "a": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                "LOCATION": "redis://:deploy-secret@seed-a/0",
            },
            "b": {
                "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                "LOCATION": "redis://:deploy-secret@seed-b/0",
            },
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)

    command.handle(deploy=True, redis_url=None, redis_cluster_url=None)

    assert "same Redis Cluster node ids" in stderr.getvalue()
    assert "redis://seed-a/0" in stderr.getvalue()
    assert "redis://seed-b/0" in stderr.getvalue()
    assert "deploy-secret" not in stderr.getvalue()


def test_warn_duplicate_cluster_seeds_redacts_userinfo():
    node_ids = frozenset(
        {
            "07c37dfeb235213a872192d90877d0cd35411309",
            "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca",
        }
    )
    messages: list[str] = []

    warn_duplicate_cluster_seeds(
        [
            ("redis://:deploy-secret@seed-a/0", node_ids),
            ("redis://:deploy-secret@seed-b/0", node_ids),
        ],
        messages.append,
    )

    assert len(messages) == 1
    assert "same Redis Cluster node ids" in messages[0]
    assert "redis://seed-a/0" in messages[0]
    assert "redis://seed-b/0" in messages[0]
    assert "deploy-secret" not in messages[0]


@pytest.mark.slow
def test_cluster_seed_discovers_topology_and_routes_to_the_slot_owner(
    redis_cluster_url, redis_cluster_remap
):
    cluster = redis.cluster.RedisCluster.from_url(
        redis_cluster_url, address_remap=redis_cluster_remap
    )
    try:
        if not cluster.nodes_manager.slots_cache:
            cluster.nodes_manager.initialize()
        seed_host, seed_port = redis_cluster_remap(("127.0.0.1", 7000))
        owner = None
        alias = None
        for candidate in (f"slot{n}" for n in range(64)):
            slot = cluster.cluster_keyslot(f"{{{candidate}}}")
            node = cluster.nodes_manager.get_node_from_slot(slot)
            mapped_host, mapped_port = cluster.nodes_manager.remap_host_port(
                node.host, int(node.port)
            )
            if (mapped_host, mapped_port) != (seed_host, int(seed_port)):
                owner = node
                alias = candidate
                break
        assert owner is not None
        assert alias is not None

        async def exercise():
            queue = _cluster_queue(
                RedisClusterAsyncQueue,
                redis_cluster_url,
                redis_cluster_remap,
                queue_name=alias,
            )
            try:
                await queue.aclear()
                await queue.aadd("routed")
                assert await queue.aget() == "routed"
            finally:
                await queue.aclose()

        asyncio.run(exercise())
    finally:
        cluster.close()


@pytest.mark.slow
def test_cluster_fifo_stack_priority_json_and_events_match_standalone_semantics(
    redis_cluster_url, redis_cluster_remap
):
    async def exercise():
        fifo = _cluster_queue(
            RedisClusterAsyncQueue, redis_cluster_url, redis_cluster_remap
        )
        stack = _cluster_queue(
            RedisClusterAsyncStack, redis_cluster_url, redis_cluster_remap
        )
        priority = _cluster_queue(
            RedisClusterAsyncPriorityQueue, redis_cluster_url, redis_cluster_remap
        )
        json_queue = _cluster_queue(
            RedisClusterAsyncQueueJson, redis_cluster_url, redis_cluster_remap
        )
        json_stack = _cluster_queue(
            RedisClusterAsyncStackJson, redis_cluster_url, redis_cluster_remap
        )
        events = _cluster_queue(
            RedisClusterEventQueue, redis_cluster_url, redis_cluster_remap
        )
        try:
            await fifo.aadd("a", "b")
            assert await fifo.aget() == "a"
            assert await fifo.aget() == "b"
            with pytest.raises(QueueEmptyException):
                await fifo.aget()

            await stack.aadd("a", "b")
            assert await stack.aget() == "b"
            assert await stack.aget() == "a"

            await priority.aadd((1, "low"), (9, "high"))
            assert await priority.aget() == "high"
            assert await priority.aget() == "low"

            await json_queue.aadd({"n": 1})
            assert await json_queue.aget() == {"n": 1}

            await json_stack.aadd({"n": 1}, {"n": 2})
            assert await json_stack.aget() == {"n": 2}
            assert await json_stack.aget() == {"n": 1}

            event_id = await events.aenqueue("notice")
            delivered = await events.adequeue()
            assert delivered.payload == "notice"
            assert delivered.id == event_id
            with pytest.raises(QueueEntryNotFoundError):
                await events.afind(event_id)
        finally:
            await fifo.aclose()
            await stack.aclose()
            await priority.aclose()
            await json_queue.aclose()
            await json_stack.aclose()
            await events.aclose()

    asyncio.run(exercise())


@pytest.mark.slow
def test_cluster_priority_json_round_trips_payloads(
    redis_cluster_url, redis_cluster_remap
):
    async def exercise():
        queue = _cluster_queue(
            RedisClusterAsyncPriorityQueueJson,
            redis_cluster_url,
            redis_cluster_remap,
        )
        try:
            await queue.aadd((2, {"body": "later"}), (8, {"body": "first"}))
            assert await queue.aget() == {"body": "first"}
            assert await queue.aget() == {"body": "later"}
        finally:
            await queue.aclose()

    asyncio.run(exercise())


def test_cluster_scheduling_lifecycle_and_retained_lookup(
    redis_cluster_url, redis_cluster_remap
):
    async def exercise():
        queue = _cluster_queue(
            RedisClusterAsyncQueue, redis_cluster_url, redis_cluster_remap
        )
        try:
            now = await queue.clock.anow()
            later_id = await queue.aenqueue("later", available_at=now + 60)
            assert await queue.afind(later_id)
            worker_id = uuid4()
            with pytest.raises(QueueEmptyException):
                await queue.aclaim(worker_id)

            client = queue._provider._async_redis()
            await client.zadd(queue._provider._entry_scheduled_name, {str(later_id): 0})
            claimed = await queue.aclaim(worker_id)
            assert claimed.id == later_id
            assert claimed.payload == "later"

            immediate_id = await queue.aenqueue("now")

            async def handle(entry):
                return "done"

            from django_queue.backends.redis import RedisAsyncQueueWorker

            worker = RedisAsyncQueueWorker(
                {"jobs": queue}, {"jobs": handle}, idle_delay=0.001
            )
            task = asyncio.create_task(worker.run())
            try:
                while (await queue.afind(immediate_id)).status not in {
                    QueueEntryStatus.SUCCEEDED,
                    QueueEntryStatus.FAILED,
                }:
                    await asyncio.sleep(0.001)
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

            finished = await queue.afind(immediate_id)
            assert finished.status is QueueEntryStatus.SUCCEEDED
            assert finished.result == "done"
        finally:
            await queue.aclose()

    asyncio.run(exercise())


def test_cluster_lifecycle_pubsub_uses_ordinary_publish_subscribe(
    redis_cluster_url, redis_cluster_remap
):
    async def exercise():
        queue = _cluster_queue(
            RedisClusterAsyncQueue, redis_cluster_url, redis_cluster_remap
        )
        received = asyncio.Event()
        snapshots = []

        def on_snapshot(entry):
            snapshots.append(entry)
            received.set()

        observer = asyncio.create_task(queue._provider.aobserve(on_snapshot))
        try:
            entry = QueueEntry.create(queue=queue.queue_name, payload="snap")
            for _ in range(50):
                await queue._provider.apublish(entry)
                try:
                    await asyncio.wait_for(received.wait(), timeout=0.1)
                    break
                except TimeoutError:
                    received.clear()
            else:
                raise AssertionError(
                    "Cluster Pub/Sub did not deliver a lifecycle snapshot"
                )
            assert snapshots[-1].payload == "snap"
        finally:
            observer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await observer
            await queue.aclose()

    asyncio.run(exercise())


def test_cluster_queue_keys_share_one_hash_slot(redis_cluster_url, redis_cluster_remap):
    queue = _cluster_queue(
        RedisClusterAsyncQueue, redis_cluster_url, redis_cluster_remap
    )
    cluster = redis.cluster.RedisCluster.from_url(
        redis_cluster_url, address_remap=redis_cluster_remap
    )
    try:
        provider = queue._provider
        keys = (
            provider._queue_name,
            provider._entry_pending_name,
            provider._entry_scheduled_name,
            provider.lifecycle_channel,
            provider._entry_key(uuid4()),
        )
        slots = {cluster.cluster_keyslot(key) for key in keys}
        assert len(slots) == 1
    finally:
        cluster.close()
        asyncio.run(queue.aclose())


def test_cluster_provider_fails_clearly_when_the_slot_owner_lacks_the_library(
    redis_cluster_url, redis_cluster_remap
):
    queue = _cluster_queue(
        RedisClusterAsyncQueue, redis_cluster_url, redis_cluster_remap
    )
    cluster = redis.cluster.RedisCluster.from_url(
        redis_cluster_url, address_remap=redis_cluster_remap
    )
    slot_client = None
    try:
        if not cluster.nodes_manager.slots_cache:
            cluster.nodes_manager.initialize()
        slot = cluster.cluster_keyslot(queue._provider._queue_name)
        node = cluster.nodes_manager.get_node_from_slot(slot)
        host, port = cluster.nodes_manager.remap_host_port(node.host, int(node.port))
        slot_client = redis.Redis(host=host, port=port)
        slot_client.function_flush()

        loads_before = slot_client.function_list(library="django_queues")

        async def exercise():
            try:
                with pytest.raises(InvalidQueueBackendError, match="redis_lua_compat"):
                    await queue.aenqueue("work")
            finally:
                await queue.aclose()

        asyncio.run(exercise())
        assert slot_client.function_list(library="django_queues") == loads_before
        assert not hasattr(queue._provider, "_async_scripts_by_loop")
    finally:
        from django_queue.backends.redis.functions import load_function_library

        library = load_function_library()
        if not cluster.nodes_manager.slots_cache:
            cluster.nodes_manager.initialize()
        for node in cluster.get_primaries():
            host, port = cluster.nodes_manager.remap_host_port(
                node.host, int(node.port)
            )
            node_client = redis.Redis(host=host, port=port)
            try:
                node_client.function_load(library.source, replace=True)
            finally:
                node_client.close()
        if slot_client is not None:
            slot_client.close()
        cluster.close()


def test_cluster_deploy_restores_the_library_on_a_primary_that_lost_it(
    redis_cluster_url, redis_cluster_remap, monkeypatch
):
    original_from_url = redis.cluster.RedisCluster.from_url

    def from_url(url, **kwargs):
        kwargs.setdefault("address_remap", redis_cluster_remap)
        return original_from_url(url, **kwargs)

    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_lib.RedisCluster.from_url",
        from_url,
    )
    cluster = original_from_url(redis_cluster_url, address_remap=redis_cluster_remap)
    try:
        if not cluster.nodes_manager.slots_cache:
            cluster.nodes_manager.initialize()
        node = cluster.get_primaries()[0]
        host, port = cluster.nodes_manager.remap_host_port(node.host, int(node.port))
        node_client = redis.Redis(host=host, port=port)
        try:
            node_client.function_flush()
            assert node_client.function_list(library="django_queues") == []
        finally:
            node_client.close()
    finally:
        cluster.close()

    LibCommand(stdout=StringIO()).handle(
        deploy=True, redis_url=None, redis_cluster_url=redis_cluster_url
    )

    restored = original_from_url(redis_cluster_url, address_remap=redis_cluster_remap)
    try:
        if not restored.nodes_manager.slots_cache:
            restored.nodes_manager.initialize()
        for node in restored.get_primaries():
            host, port = restored.nodes_manager.remap_host_port(
                node.host, int(node.port)
            )
            node_client = redis.Redis(host=host, port=port)
            try:
                installed = node_client.function_list(
                    library="django_queues", withcode=True
                )
                assert installed
            finally:
                node_client.close()
    finally:
        restored.close()


def test_cluster_primary_clients_forward_seed_connection_settings(monkeypatch):
    captured = []

    class RecordingRedis:
        def __init__(self, **kwargs):
            captured.append(kwargs)

    cluster = SimpleNamespace(
        nodes_manager=SimpleNamespace(
            connection_kwargs={
                "username": "deploy",
                "password": "secret",
                "ssl": True,
                "db": 0,
                "host": "seed.example",
                "port": 6379,
                "redis_connect_func": object(),
            },
            slots_cache={0: True},
        ),
        get_primaries=lambda: [_Node("10.0.0.1", 7000)],
    )

    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", RecordingRedis
    )

    list(iter_cluster_primary_clients(cluster))

    assert captured == [
        {
            "host": "10.0.0.1",
            "port": 7000,
            "username": "deploy",
            "password": "secret",
            "ssl": True,
            "db": 0,
        }
    ]


def test_cluster_primary_clients_forward_rediss_url_settings(monkeypatch):
    captured = []
    parsed = redis.connection.parse_url("rediss://deploy:s3cret@seed.example:6379/0")

    class RecordingRedis:
        def __init__(self, **kwargs):
            captured.append(kwargs)

    cluster = SimpleNamespace(
        nodes_manager=SimpleNamespace(
            connection_kwargs=parsed,
            slots_cache={0: True},
        ),
        get_primaries=lambda: [_Node("10.0.0.1", 7000)],
    )

    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", RecordingRedis
    )

    list(iter_cluster_primary_clients(cluster))

    assert captured[0]["username"] == "deploy"
    assert captured[0]["password"] == "s3cret"
    assert captured[0]["host"] == "10.0.0.1"
    assert captured[0]["port"] == 7000
    assert (
        captured[0].get("ssl") is True
        or captured[0].get("connection_class") is redis.connection.SSLConnection
    )


def test_libcheck_passes_configured_address_remap_to_cluster_client(monkeypatch):
    def remap(address):
        return address

    captured = {}

    def from_url(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return _FakeCluster([_Node("10.0.0.1", 7000)])

    _NodeRedis.clients = []
    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_lib.RedisCluster.from_url",
        from_url,
    )
    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", _NodeRedis
    )
    monkeypatch.setattr(
        django_queue,
        "queues",
        django_queue.QueueRegistry(
            {
                "jobs": {
                    "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                    "LOCATION": "redis://cluster/0",
                    "address_remap": remap,
                }
            }
        ),
    )

    LibCommand(stdout=StringIO()).handle(
        deploy=True, redis_url=None, redis_cluster_url=None
    )

    assert captured["url"] == "redis://cluster/0"
    assert captured["kwargs"]["address_remap"] is remap


def test_libcheck_closes_primary_clients_when_a_primary_fails(monkeypatch):
    class FailingRedis(_NodeRedis):
        def function_list(self, *, library, withcode=False):
            raise redis.ResponseError("boom")

    _NodeRedis.clients = []
    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_lib.RedisCluster.from_url",
        lambda url, **kwargs: _FakeCluster(
            [_Node("10.0.0.1", 7000), _Node("10.0.0.2", 7001)]
        ),
    )
    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", FailingRedis
    )

    with pytest.raises(CommandError, match="boom"):
        LibCommand(stdout=StringIO()).handle(
            deploy=True, redis_url=None, redis_cluster_url="redis://cluster/0"
        )

    assert _NodeRedis.clients
    assert all(client.closed for client in _NodeRedis.clients)


def test_compat_reports_a_denied_cluster_function_call(monkeypatch):
    class FailingRedis(_NodeRedis):
        def fcall(self, function, numkeys, *args):
            raise redis.ResponseError("NOPERM this user has no permissions")

    _NodeRedis.clients = []
    cluster = _FakeCluster([_Node("10.0.0.1", 7000)])
    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_compat.RedisCluster.from_url",
        lambda url, **kwargs: cluster,
    )
    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", FailingRedis
    )

    with pytest.raises(
        CommandError, match="compatibility check failed on 10.0.0.1:7000: NOPERM"
    ):
        CompatCommand(stdout=StringIO()).handle(
            redis_url=None, redis_cluster_url="redis://cluster/0"
        )

    assert _NodeRedis.clients
    assert all(client.closed for client in _NodeRedis.clients)
    assert cluster.closed is True


def test_compat_reports_when_cluster_has_no_primaries(monkeypatch):
    cluster = _FakeCluster([])
    monkeypatch.setattr(
        "django_queue.management.commands.redis_lua_compat.RedisCluster.from_url",
        lambda url, **kwargs: cluster,
    )
    monkeypatch.setattr(
        "django_queue.management.redis_functions.redis.Redis", _NodeRedis
    )

    with pytest.raises(CommandError, match="did not discover any primary nodes"):
        CompatCommand(stdout=StringIO()).handle(
            redis_url=None, redis_cluster_url="redis://cluster/0"
        )

    assert cluster.closed is True


def test_cluster_provider_reports_the_function_when_an_operation_is_denied():
    class Client:
        async def fcall(self, function, numkeys, *args):
            if function == "django_queue_info":
                return [b"260822_160000", 1]
            raise redis.ResponseError("NOPERM this user has no permissions")

    async def exercise():
        provider = QueueProviderRedisCluster(
            "redis://localhost:6379/0",
            queue_name="jobs",
            entry_class=QueueEntry,
        )
        provider._async_redis_by_loop[asyncio.get_running_loop()] = Client()
        with pytest.raises(InvalidQueueBackendError) as error:
            await provider._fcall("django_queue_claim", 2, "k1", "k2")
        return str(error.value)

    assert asyncio.run(exercise()) == (
        "Redis Function django_queue_claim failed: NOPERM this user has no permissions"
    )


def test_cluster_alist_returns_retained_entries(redis_cluster_url, redis_cluster_remap):
    async def exercise():
        queue = _cluster_queue(
            RedisClusterAsyncQueue, redis_cluster_url, redis_cluster_remap
        )
        try:
            first = await queue.aenqueue("one")
            second = await queue.aenqueue("two")
            entries = await queue.alist()
            return (
                {first, second},
                {entry.id for entry in entries},
                {entry.payload for entry in entries},
            )
        finally:
            await queue.aclose()

    expected_ids, listed_ids, payloads = asyncio.run(exercise())

    assert payloads == {"one", "two"}
    assert listed_ids == expected_ids


def test_cluster_recovers_an_expired_claim(
    redis_cluster_url, redis_cluster_remap, eventually
):
    async def exercise():
        queue = _cluster_queue(
            RedisClusterAsyncQueue, redis_cluster_url, redis_cluster_remap
        )
        try:
            entry_id = await queue.aenqueue("work")
            claimed = await queue.aclaim(uuid4(), lease_seconds=0.001)
            assert claimed.id == entry_id

            recovery = None

            async def assert_recovered():
                nonlocal recovery
                recovery = await queue.arecover(1)
                assert recovery == (1, 0)

            await eventually(1, assert_recovered)
            assert recovery is not None
            redelivered = await queue.adequeue()
            return recovery, redelivered.id, entry_id
        finally:
            await queue.aclose()

    recovery, redelivered_id, entry_id = asyncio.run(exercise())

    assert recovery == (1, 0)
    assert redelivered_id == entry_id


def test_cluster_recovers_an_expired_priority_claim(
    redis_cluster_url, redis_cluster_remap, eventually
):
    async def exercise():
        queue = _cluster_queue(
            RedisClusterAsyncPriorityQueue, redis_cluster_url, redis_cluster_remap
        )
        try:
            low_id = await queue.aenqueue("low", priority=1)
            high_id = await queue.aenqueue("high", priority=10)
            claimed = await queue.aclaim(uuid4(), lease_seconds=0.001)
            assert claimed.id == high_id

            recovery = None

            async def assert_recovered():
                nonlocal recovery
                recovery = await queue.arecover(1)
                assert recovery == (1, 0)

            await eventually(1, assert_recovered)
            assert recovery is not None
            with pytest.raises(QueueEmptyException):
                await queue._provider.aclaim(uuid4())
            redelivered = await queue.adequeue()
            remaining = await queue.adequeue()
            return recovery, redelivered.id, remaining.id, high_id, low_id
        finally:
            await queue.aclose()

    recovery, redelivered_id, remaining_id, high_id, low_id = asyncio.run(exercise())

    assert recovery == (1, 0)
    assert redelivered_id == high_id
    assert remaining_id == low_id


def test_libcheck_deploys_using_configured_address_remap(
    redis_cluster_url, redis_cluster_remap, monkeypatch
):
    cluster = redis.cluster.RedisCluster.from_url(
        redis_cluster_url, address_remap=redis_cluster_remap
    )
    try:
        if not cluster.nodes_manager.slots_cache:
            cluster.nodes_manager.initialize()
        node = cluster.get_primaries()[0]
        host, port = cluster.nodes_manager.remap_host_port(node.host, int(node.port))
        node_client = redis.Redis(host=host, port=port)
        try:
            node_client.function_flush()
            assert node_client.function_list(library="django_queues") == []
        finally:
            node_client.close()
    finally:
        cluster.close()

    monkeypatch.setattr(
        django_queue,
        "queues",
        django_queue.QueueRegistry(
            {
                "jobs": {
                    "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                    "LOCATION": redis_cluster_url,
                    "address_remap": redis_cluster_remap,
                }
            }
        ),
    )
    LibCommand(stdout=StringIO()).handle(
        deploy=True, redis_url=None, redis_cluster_url=None
    )

    restored = redis.cluster.RedisCluster.from_url(
        redis_cluster_url, address_remap=redis_cluster_remap
    )
    try:
        if not restored.nodes_manager.slots_cache:
            restored.nodes_manager.initialize()
        for node in restored.get_primaries():
            host, port = restored.nodes_manager.remap_host_port(
                node.host, int(node.port)
            )
            node_client = redis.Redis(host=host, port=port)
            try:
                assert node_client.function_list(library="django_queues", withcode=True)
            finally:
                node_client.close()
    finally:
        restored.close()
