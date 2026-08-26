from io import StringIO

import pytest
import redis
from django.core.management.base import CommandError

import django_queue
from django_queue.backends.redis.functions import FUNCTION_LIBRARY_VERSION
from django_queue.management.commands.redis_lua_compat import Command as CompatCommand
from django_queue.management.commands.redis_lua_lib import Command
from django_queue.management.redis_functions import (
    read_installed_library_info,
    redact_redis_url,
    resolve_redis_urls,
)


def _installed_library(
    library_version: str | None = None, api_version: int = 1
) -> list[dict[str, str]]:
    if library_version is None:
        library_version = FUNCTION_LIBRARY_VERSION
    return [
        {
            "library_name": "django_queues",
            "library_code": (
                "#!lua name=django_queues\n"
                f"-- django-queues-library-version: {library_version}\n"
                f"-- django-queues-api-version: {api_version}\n"
            ),
        }
    ]


def test_resolves_and_deduplicates_redis_urls_from_queue_configuration():
    urls = resolve_redis_urls(
        {
            "first": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": "redis://first/0",
            },
            "duplicate": {
                "BACKEND": "django_queue.backends.redis.RedisEventQueue",
                "LOCATION": "redis://first/0",
            },
            "second": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncPriorityQueue",
                "LOCATION": "redis://second/1",
            },
            "memory": {
                "BACKEND": "django_queue.backends.MemoryAsyncQueue",
                "LOCATION": "",
            },
        }
    )

    assert urls == ("redis://first/0", "redis://second/1")


def test_redis_url_override_replaces_configured_targets():
    urls = resolve_redis_urls(
        {
            "configured": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": "redis://configured/0",
            },
        },
        redis_url="redis://override/3",
    )

    assert urls == ("redis://override/3",)


def test_reads_installed_library_info_from_resp2_function_list_pairs():
    source = (
        "#!lua name=django_queues\n"
        "-- django-queues-library-version: 260822_160000\n"
        "-- django-queues-api-version: 1\n"
    )
    libraries = [
        [
            b"library_name",
            b"django_queues",
            b"library_code",
            source.encode("utf-8"),
        ]
    ]

    assert read_installed_library_info(libraries) == ("260822_160000", 1)


def test_libcheck_deploys_the_bundled_library_to_each_configured_target(
    monkeypatch,
):
    configured = django_queue.QueueRegistry(
        {
            "first": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": "redis://first/0",
            },
            "duplicate": {
                "BACKEND": "django_queue.backends.redis.RedisEventQueue",
                "LOCATION": "redis://first/0",
            },
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)
    clients = []

    class RedisClient:
        def set(self, *args, **kwargs):
            self.lock_args = args
            self.lock_kwargs = kwargs
            return True

        def function_list(self, *, library, withcode=False):
            assert withcode is True
            return []

        def function_load(self, source, *, replace):
            self.source = source
            self.replace = replace

        def close(self):
            self.closed = True

        def eval(self, script, numkeys, key, token):
            self.released_lock = (script, numkeys, key, token)

    def from_url(url):
        client = RedisClient()
        client.url = url
        clients.append(client)
        return client

    monkeypatch.setattr("redis.Redis.from_url", from_url)

    Command(stdout=StringIO()).handle(deploy=True, redis_url=None)

    assert [client.url for client in clients] == ["redis://first/0"]
    assert clients[0].replace is True
    assert clients[0].source.startswith("#!lua name=django_queues\n")
    assert clients[0].closed is True
    assert clients[0].released_lock[1:3] == (1, "django_queues:function-deploy-lock")


def test_libcheck_deploy_skips_an_installed_matching_library(monkeypatch):
    class RedisClient:
        def set(self, *args, **kwargs):
            return True

        def function_list(self, *, library, withcode=False):
            assert library == "django_queues"
            assert withcode is True
            return _installed_library()

        def fcall(self, *args):
            raise AssertionError("deployment checks must not require FCALL")

        def function_load(self, *args, **kwargs):
            raise AssertionError("matching library must not be replaced")

        def eval(self, *args):
            pass

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)
    output = StringIO()

    Command(stdout=output).handle(deploy=True, redis_url="redis://override/0")

    assert output.getvalue() == "Redis Function library django_queues is current.\n"
    assert client.closed is True


def test_libcheck_deploy_replaces_a_different_installed_library(monkeypatch):
    class RedisClient:
        def set(self, *args, **kwargs):
            return True

        def function_list(self, *, library, withcode=False):
            assert withcode is True
            return _installed_library("260822_150000")

        def function_load(self, source, *, replace):
            self.load = (source, replace)

        def eval(self, *args):
            pass

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    Command(stdout=StringIO()).handle(deploy=True, redis_url="redis://override/0")

    assert client.load[1] is True
    assert client.closed is True


def test_libcheck_rejects_lowering_the_installed_api_without_rollback(monkeypatch):
    class RedisClient:
        def set(self, *args, **kwargs):
            return True

        def function_list(self, *, library, withcode=False):
            assert withcode is True
            return _installed_library("260823_120000", 2)

        def function_load(self, *args, **kwargs):
            raise AssertionError("API rollback must require explicit approval")

        def eval(self, *args):
            pass

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    with pytest.raises(CommandError, match="use --rollback"):
        Command(stdout=StringIO()).handle(deploy=True, redis_url="redis://override/0")

    assert client.closed is True


def test_libcheck_allows_lowering_the_installed_api_with_rollback(monkeypatch):
    class RedisClient:
        def set(self, *args, **kwargs):
            return True

        def function_list(self, *, library, withcode=False):
            assert withcode is True
            return _installed_library("260823_120000", 2)

        def function_load(self, source, *, replace):
            self.load = (source, replace)

        def eval(self, *args):
            pass

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    Command(stdout=StringIO()).handle(
        deploy=True, rollback=True, redis_url="redis://override/0"
    )

    assert client.load[1] is True
    assert client.closed is True


def test_libcheck_reports_installed_versions_and_required_deployment(monkeypatch):
    class RedisClient:
        def function_list(self, *, library, withcode=False):
            assert library == "django_queues"
            assert withcode is True
            return _installed_library("260822_150000")

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    with pytest.raises(CommandError) as error:
        Command(stdout=StringIO()).handle(deploy=False, redis_url="redis://override/0")

    assert str(error.value) == (
        "Redis Function library is installed:\n\n"
        "- django_queues version 260822_150000\n"
        "- api_version 1 (compatible)\n\n"
        "Bundled library_version: "
        f"{FUNCTION_LIBRARY_VERSION}\n"
        "Deployment required."
    )
    assert client.closed is True


def test_libcheck_formats_a_current_library_status(monkeypatch):
    class RedisClient:
        def function_list(self, *, library, withcode=False):
            assert library == "django_queues"
            assert withcode is True
            return _installed_library()

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)
    output = StringIO()

    Command(stdout=output).handle(deploy=False, redis_url="redis://override/0")

    assert output.getvalue() == (
        "Redis Function library is installed:\n\n"
        f"- django_queues version {FUNCTION_LIBRARY_VERSION}\n"
        "- api_version 1 (compatible)\n\n"
        f"Bundled library_version: {FUNCTION_LIBRARY_VERSION}\n"
    )
    assert client.closed is True


def test_libcheck_reports_when_the_library_is_not_installed(monkeypatch):
    class RedisClient:
        def function_list(self, *, library, withcode=False):
            assert library == "django_queues"
            assert withcode is True
            return []

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    with pytest.raises(CommandError, match="is not installed. Deployment required"):
        Command(stdout=StringIO()).handle(deploy=False, redis_url="redis://override/0")

    assert client.closed is True


def test_compat_checks_the_installed_function_api(monkeypatch):
    class RedisClient:
        def fcall(self, function, numkeys):
            self.function = function
            self.numkeys = numkeys
            return [b"260822_160000", 1]

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)
    configured = django_queue.QueueRegistry(
        {
            "configured": {
                "BACKEND": "django_queue.backends.redis.RedisAsyncQueue",
                "LOCATION": "redis://configured/0",
            },
        }
    )
    monkeypatch.setattr(django_queue, "queues", configured)

    CompatCommand(stdout=StringIO()).handle(redis_url=None)

    assert client.function == "django_queue_info"
    assert client.numkeys == 0
    assert client.closed is True


def test_libcheck_reports_a_concurrent_deployment(monkeypatch):
    class RedisClient:
        def set(self, *args, **kwargs):
            return False

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    with pytest.raises(CommandError, match="deployment is in progress"):
        Command(stdout=StringIO()).handle(deploy=True, redis_url="redis://override/0")

    assert client.closed is True


def test_compat_rejects_an_incompatible_function_api(monkeypatch):
    class RedisClient:
        def fcall(self, function, numkeys):
            return [b"260822_160000", 0]

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    with pytest.raises(CommandError, match="below the required"):
        CompatCommand(stdout=StringIO()).handle(redis_url="redis://override/0")

    assert client.closed is True


@pytest.mark.parametrize(
    "result",
    [
        [b"260822_160000", True],
        [b"\xff", 1],
    ],
)
def test_compat_rejects_invalid_function_introspection_data(monkeypatch, result):
    """Reject malformed Function metadata instead of reporting compatibility."""

    class RedisClient:
        def fcall(self, function, numkeys):
            return result

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    with pytest.raises(CommandError, match="invalid introspection data"):
        CompatCommand(stdout=StringIO()).handle(redis_url="redis://override/0")

    assert client.closed is True


def test_compat_reports_a_denied_function_call(monkeypatch):
    class RedisClient:
        def fcall(self, function, numkeys):
            raise redis.ResponseError("NOPERM this user has no permissions")

        def close(self):
            self.closed = True

    client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: client)

    with pytest.raises(CommandError, match="compatibility check failed: NOPERM"):
        CompatCommand(stdout=StringIO()).handle(redis_url="redis://override/0")

    assert client.closed is True


def test_libcheck_wraps_an_invalid_redis_url(monkeypatch):
    """Catch a client-construction error at the command boundary."""

    def reject_url(url):
        raise ValueError("Invalid Redis URL")

    monkeypatch.setattr("redis.Redis.from_url", reject_url)

    with pytest.raises(
        CommandError, match="Redis Function check failed: Invalid Redis URL"
    ) as error:
        Command(stdout=StringIO()).handle(deploy=False, redis_url="not-a-redis-url")

    assert isinstance(error.value.__cause__, ValueError)


def test_compat_wraps_an_invalid_redis_url(monkeypatch):
    """Catch a client-construction error at the command boundary."""

    def reject_url(url):
        raise ValueError("Invalid Redis URL")

    monkeypatch.setattr("redis.Redis.from_url", reject_url)

    with pytest.raises(
        CommandError,
        match="Redis Function compatibility check failed: Invalid Redis URL",
    ) as error:
        CompatCommand(stdout=StringIO()).handle(redis_url="not-a-redis-url")

    assert isinstance(error.value.__cause__, ValueError)


def test_redact_redis_url_strips_userinfo():
    assert (
        redact_redis_url("redis://:deploy-secret@10.0.0.1:6379/0")
        == "redis://10.0.0.1:6379/0"
    )
    assert (
        redact_redis_url("rediss://deploy:s3cret@seed.example:6379/0")
        == "rediss://seed.example:6379/0"
    )
    assert redact_redis_url("redis://localhost:6379/0") == "redis://localhost:6379/0"
