import asyncio
import inspect
import os
from pathlib import Path

import pytest

os.environ.setdefault("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")

_CLUSTER_IMAGE_DIR = Path(__file__).resolve().parent / "redis-cluster"
_CLUSTER_PORTS = (7000, 7001, 7002)


def redis_cluster_address_remap(container):
    """Map advertised Cluster node addresses to published host ports."""
    host = container.get_container_host_ip()
    published = {port: int(container.get_exposed_port(port)) for port in _CLUSTER_PORTS}

    def remap(address):
        _advertised_host, advertised_port = address
        mapped = published.get(int(advertised_port))
        if mapped is None:
            return address
        return host, mapped

    return remap


try:
    import redis
    from docker.errors import DockerException
    from testcontainers.community.redis import RedisContainer
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.image import DockerImage
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    @pytest.fixture(scope="module")
    def redis_container():
        try:
            with RedisContainer("redis:alpine") as container:
                yield container
        except DockerException as exc:
            pytest.skip(f"Docker is unavailable for Redis integration tests: {exc}")

    @pytest.fixture(scope="module")
    def redis_client(redis_container, redis_function_library):
        return (
            f"redis://{redis_container.get_container_host_ip()}:"
            f"{redis_container.get_exposed_port(6379)}/0"
        )

    @pytest.fixture(scope="module")
    def redis_raw_client(redis_container):
        return redis.Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
        )

    @pytest.fixture(scope="module")
    def redis_function_library(redis_raw_client):
        from django_queue.backends.redis.functions import load_function_library

        redis_raw_client.function_load(load_function_library().source, replace=True)

    @pytest.fixture(scope="module")
    def redis_url(redis_client):
        return redis_client

    @pytest.fixture(scope="module")
    def redis_cluster_container():
        try:
            with DockerImage(
                path=str(_CLUSTER_IMAGE_DIR),
                tag="django-queues-redis-cluster:test",
            ) as image:
                container = (
                    DockerContainer(str(image))
                    .with_exposed_ports(*_CLUSTER_PORTS)
                    .waiting_for(LogMessageWaitStrategy("cluster-ready"))
                )
                with container:
                    yield container
        except DockerException as exc:
            pytest.skip(f"Docker is unavailable for Redis Cluster tests: {exc}")

    @pytest.fixture(scope="module")
    def redis_cluster_url(redis_cluster_container):
        from django_queue.backends.redis.functions import load_function_library

        remap = redis_cluster_address_remap(redis_cluster_container)
        host = redis_cluster_container.get_container_host_ip()
        port = redis_cluster_container.get_exposed_port(7000)
        url = f"redis://{host}:{port}/0"
        cluster = redis.cluster.RedisCluster.from_url(url, address_remap=remap)
        try:
            library = load_function_library()
            manager = cluster.nodes_manager
            if not manager.slots_cache:
                manager.initialize()
            for node in cluster.get_primaries():
                mapped_host, mapped_port = manager.remap_host_port(
                    node.host, int(node.port)
                )
                node_client = redis.Redis(host=mapped_host, port=mapped_port)
                try:
                    node_client.function_load(library.source, replace=True)
                finally:
                    node_client.close()
        finally:
            cluster.close()
        return url

    @pytest.fixture(scope="module")
    def redis_cluster_remap(redis_cluster_container):
        return redis_cluster_address_remap(redis_cluster_container)

except ImportError:
    pass


@pytest.fixture
def eventually():
    async def assert_eventually(timeout, assertion):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        async def invoke_assertion():
            result = assertion()
            if inspect.isawaitable(result):
                await result

        while True:
            try:
                await invoke_assertion()
                return
            except AssertionError:
                if loop.time() >= deadline:
                    break
                await asyncio.sleep(min(0.001, deadline - loop.time()))

        await invoke_assertion()

    return assert_eventually


@pytest.fixture
def no_runtime_startup(monkeypatch):
    """Suppress queue_runtime auto-start via ready()/QueueRegistry.create_connection.
    Prevents thread leaks during tests."""
    from django_queue.queue_runtime import queue_runtime

    monkeypatch.setattr(queue_runtime, "start_thread", lambda: None)
    monkeypatch.setattr(queue_runtime, "start", lambda queues: None)
    monkeypatch.setattr(queue_runtime, "start_one", lambda alias, queue: None)


def _slow_tests_enabled():
    return os.getenv("SLOW_TESTS") in ("true", "1", "enabled")


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "slow: mark test as slow (skipped unless SLOW_TESTS=true/1/enabled)"
    )
    # Register the slow marker
    pytest.mark.slow = pytest.mark.skipif(
        not _slow_tests_enabled(),
        reason="Test skipped because SLOW_TESTS environment variable not set to true, 1 or enabled",
    )
