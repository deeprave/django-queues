import asyncio
import inspect
import os
import shutil
import tempfile
import warnings
from pathlib import Path
from subprocess import CalledProcessError

import pytest

from tests.redis_tls import generate_redis_tls_material, openssl_available

os.environ.setdefault("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")

_CLUSTER_IMAGE_DIR = Path(__file__).resolve().parent / "redis-cluster"
_CLUSTER_PORTS = (7000, 7001, 7002)
_TLS_REDIS_CMD = [
    "redis-server",
    "--tls-port",
    "6379",
    "--port",
    "0",
    "--tls-cert-file",
    "/tls/server.crt",
    "--tls-key-file",
    "/tls/server.key",
    "--tls-ca-cert-file",
    "/tls/ca.crt",
    "--tls-auth-clients",
    "no",
    "--protected-mode",
    "no",
    "--bind",
    "0.0.0.0",
]


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

    def _container_log_detail(container) -> str:
        try:
            stdout, stderr = container.get_logs()
        except OSError, AttributeError, DockerException:
            return ""

        def decode(value):
            if isinstance(value, bytes):
                return value.decode("utf-8", "replace")
            return str(value)

        return f" stdout={decode(stdout)!r} stderr={decode(stderr)!r}"

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
        client = redis.Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
        )
        try:
            yield client
        finally:
            client.close()

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

    @pytest.fixture(scope="module")
    def redis_tls_certs():
        if not openssl_available():
            pytest.skip("openssl is unavailable for Redis TLS integration tests")
        # pytest tmp dirs are 0700. Linux Docker preserves that, and redis:8
        # drops to uid 999, so bind-mounted certs must live under a world-
        # traversable path (typically /tmp at 1777).
        directory = Path(tempfile.mkdtemp(prefix="django-queues-redis-tls-"))
        directory.chmod(0o755)
        try:
            try:
                yield generate_redis_tls_material(directory)
            except (CalledProcessError, OSError) as exc:
                pytest.skip(f"openssl could not generate Redis TLS material: {exc}")
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    @pytest.fixture(scope="module")
    def redis_tls_container(redis_tls_certs):
        try:
            container = (
                DockerContainer("redis:8")
                .with_exposed_ports(6379)
                .with_volume_mapping(str(redis_tls_certs.directory), "/tls", "ro")
                .with_command(_TLS_REDIS_CMD)
                .waiting_for(LogMessageWaitStrategy("Ready to accept connections"))
            )
            try:
                with container:
                    yield container
            except DockerException:
                raise
            except (RuntimeError, TimeoutError) as exc:
                detail = _container_log_detail(container)
                raise RuntimeError(
                    f"Redis TLS container failed to start: {exc}.{detail}"
                ) from exc
        except DockerException as exc:
            pytest.skip(f"Docker is unavailable for Redis TLS tests: {exc}")

    @pytest.fixture(scope="module")
    def redis_tls_url(redis_tls_container, redis_tls_certs):
        from django_queue.backends.redis.functions import load_function_library

        host = redis_tls_container.get_container_host_ip()
        port = redis_tls_container.get_exposed_port(6379)
        url = f"rediss://{host}:{port}/0"
        client = redis.Redis.from_url(url, ssl_ca_certs=str(redis_tls_certs.ca_cert))
        try:
            client.function_load(load_function_library().source, replace=True)
        finally:
            client.close()
        return url

    @pytest.fixture(scope="module")
    def redis_cluster_tls_container(redis_tls_certs):
        try:
            with DockerImage(
                path=str(_CLUSTER_IMAGE_DIR),
                tag="django-queues-redis-cluster:test-tls",
            ) as image:
                container = (
                    DockerContainer(str(image))
                    .with_exposed_ports(*_CLUSTER_PORTS)
                    .with_volume_mapping(str(redis_tls_certs.directory), "/tls", "ro")
                    .waiting_for(LogMessageWaitStrategy("cluster-ready"))
                )
                with container:
                    yield container
        except DockerException as exc:
            pytest.skip(f"Docker is unavailable for Redis Cluster TLS tests: {exc}")

    @pytest.fixture(scope="module")
    def redis_cluster_tls_url(redis_cluster_tls_container, redis_tls_certs):
        from django_queue.backends.redis.functions import load_function_library
        from django_queue.management.redis_functions import (
            iter_cluster_primary_clients,
        )

        remap = redis_cluster_address_remap(redis_cluster_tls_container)
        host = redis_cluster_tls_container.get_container_host_ip()
        port = redis_cluster_tls_container.get_exposed_port(7000)
        url = f"rediss://{host}:{port}/0"
        cluster = redis.cluster.RedisCluster.from_url(
            url,
            address_remap=remap,
            ssl_ca_certs=str(redis_tls_certs.ca_cert),
        )
        try:
            library = load_function_library()
            for client, _advertised, _node in iter_cluster_primary_clients(cluster):
                try:
                    client.function_load(library.source, replace=True)
                finally:
                    client.close()
        finally:
            cluster.close()
        return url

    @pytest.fixture(scope="module")
    def redis_cluster_tls_remap(redis_cluster_tls_container):
        return redis_cluster_address_remap(redis_cluster_tls_container)

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


def _ignore_docker_unix_socket_warnings() -> None:
    """Ignore leftover Docker daemon AF_UNIX sockets under ``-Werror``.

    Command-line ``-Werror`` is applied after ini filters and wins, so this
    must be re-installed inside pytest's per-test ``catch_warnings`` as well
    as at session boundaries. Family 1 is AF_UNIX; leaked Redis TCP sockets
    stay errors.
    """
    warnings.filterwarnings(
        "ignore",
        message=r"unclosed <socket\.socket fd=\d+, family=1, type=1, proto=0>",
        category=ResourceWarning,
    )
    warnings.filterwarnings(
        "ignore",
        message=r"Exception ignored while finalizing socket.*family=1, type=1, proto=0",
        category=pytest.PytestUnraisableExceptionWarning,
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "slow: mark test as slow (skipped unless SLOW_TESTS=true/1/enabled)"
    )
    # Register the slow marker
    pytest.mark.slow = pytest.mark.skipif(
        not _slow_tests_enabled(),
        reason="Test skipped because SLOW_TESTS environment variable not set to true, 1 or enabled",
    )
    _ignore_docker_unix_socket_warnings()


@pytest.hookimpl(wrapper=True)
def pytest_runtest_protocol(item):
    _ignore_docker_unix_socket_warnings()
    return (yield)


@pytest.hookimpl(tryfirst=True)
def pytest_unconfigure(config):
    _ignore_docker_unix_socket_warnings()


def pytest_sessionfinish(session, exitstatus):
    """Close testcontainers' Ryuk Docker client at session end."""
    _ignore_docker_unix_socket_warnings()
    try:
        from testcontainers.core.container import Reaper
    except ImportError:
        return
    Reaper.delete_instance()
