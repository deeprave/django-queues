import os

import pytest

os.environ.setdefault("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")

try:
    import redis
    from docker.errors import DockerException
    from testcontainers.community.redis import RedisContainer

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

except ImportError:
    pass


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
