import asyncio
from io import StringIO
from uuid import uuid4

import pytest
import redis

import django_queue
from django_queue.backends.exceptions import InvalidQueueBackendError
from django_queue.backends.redis import RedisAsyncQueue, RedisClusterAsyncQueue
from django_queue.entries import QueueEntry
from django_queue.management.commands.redis_lua_compat import Command as CompatCommand
from django_queue.management.commands.redis_lua_lib import Command as LibCommand


def test_standalone_tls_queue_operations_and_recovery(
    redis_tls_url, redis_tls_certs, eventually
):
    async def exercise():
        queue = RedisAsyncQueue(
            redis_tls_url,
            queue_name="tls-standalone",
            ssl_ca_certs=str(redis_tls_certs.ca_cert),
        )
        try:
            await queue.aclear()
            entry_id = await queue.aenqueue("work")
            claimed = await queue.aclaim(uuid4(), lease_seconds=0.001)
            assert claimed.id == entry_id

            recovery = None

            async def assert_recovered():
                nonlocal recovery
                recovery = await queue.arecover(1)
                assert recovery == (1, 0)

            await eventually(1, assert_recovered)
            redelivered = await queue.adequeue()
            assert redelivered.id == entry_id
        finally:
            await queue.aclose()

    asyncio.run(exercise())


def test_standalone_tls_rejects_an_untrusted_ca(redis_tls_url, redis_tls_certs):
    queue = RedisAsyncQueue(
        redis_tls_url,
        queue_name="tls-untrusted",
        ssl_ca_certs=str(redis_tls_certs.untrusted_ca_cert),
    )

    async def exercise():
        try:
            with pytest.raises(InvalidQueueBackendError, match="TLS handshake failed"):
                await queue.aenqueue("work")
        finally:
            await queue.aclose()

    asyncio.run(exercise())


def test_standalone_tls_observer_uses_encrypted_transport(
    redis_tls_url, redis_tls_certs
):
    async def exercise():
        queue = RedisAsyncQueue(
            redis_tls_url,
            queue_name="tls-observer",
            ssl_ca_certs=str(redis_tls_certs.ca_cert),
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
                raise AssertionError("TLS Pub/Sub did not deliver a lifecycle snapshot")
            assert snapshots[-1].payload == "snap"
        finally:
            observer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await observer
            await queue.aclose()

    asyncio.run(exercise())


def test_standalone_tls_function_deploy_and_compat(
    redis_tls_url, redis_tls_certs, monkeypatch, no_runtime_startup
):
    monkeypatch.setattr(
        django_queue,
        "queues",
        django_queue.QueueRegistry(
            {
                "jobs": {
                    "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                    "LOCATION": redis_tls_url,
                    "ssl_ca_certs": str(redis_tls_certs.ca_cert),
                }
            }
        ),
    )

    LibCommand(stdout=StringIO()).handle(
        deploy=True, redis_url=None, redis_cluster_url=None
    )
    CompatCommand(stdout=StringIO()).handle(redis_url=None, redis_cluster_url=None)


def test_cluster_tls_routes_to_a_non_seed_slot(
    redis_cluster_tls_url, redis_cluster_tls_remap, redis_tls_certs
):
    cluster = redis.cluster.RedisCluster.from_url(
        redis_cluster_tls_url,
        address_remap=redis_cluster_tls_remap,
        ssl_ca_certs=str(redis_tls_certs.ca_cert),
    )
    try:
        if not cluster.nodes_manager.slots_cache:
            cluster.nodes_manager.initialize()
        seed_host, seed_port = redis_cluster_tls_remap(("127.0.0.1", 7000))
        alias = None
        for candidate in (f"tls{n}" for n in range(64)):
            slot = cluster.cluster_keyslot(f"{{{candidate}}}")
            node = cluster.nodes_manager.get_node_from_slot(slot)
            mapped_host, mapped_port = cluster.nodes_manager.remap_host_port(
                node.host, int(node.port)
            )
            if (mapped_host, mapped_port) != (seed_host, int(seed_port)):
                alias = candidate
                break
        assert alias is not None

        async def exercise():
            queue = RedisClusterAsyncQueue(
                redis_cluster_tls_url,
                queue_name=alias,
                address_remap=redis_cluster_tls_remap,
                ssl_ca_certs=str(redis_tls_certs.ca_cert),
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


def test_cluster_tls_function_deploy(
    redis_cluster_tls_url,
    redis_cluster_tls_remap,
    redis_tls_certs,
    monkeypatch,
    no_runtime_startup,
):
    monkeypatch.setattr(
        django_queue,
        "queues",
        django_queue.QueueRegistry(
            {
                "jobs": {
                    "BACKEND": "django_queue.backends.redis.RedisClusterAsyncQueue",
                    "LOCATION": redis_cluster_tls_url,
                    "address_remap": redis_cluster_tls_remap,
                    "ssl_ca_certs": str(redis_tls_certs.ca_cert),
                }
            }
        ),
    )

    LibCommand(stdout=StringIO()).handle(
        deploy=True, redis_url=None, redis_cluster_url=None
    )
