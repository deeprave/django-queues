"""Check and explicitly deploy the bundled Redis Function library."""

from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import redis
from django.core.management.base import BaseCommand, CommandError

import django_queue
from django_queue.backends.redis.functions import (
    FUNCTION_LIBRARY_NAME,
    load_function_library,
)
from django_queue.management.redis_functions import (
    read_installed_library_info,
    resolve_redis_urls,
)

_DEPLOYMENT_LOCK_KEY = "django_queues:function-deploy-lock"
_DEPLOYMENT_LOCK_SECONDS = 30
_RELEASE_DEPLOYMENT_LOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class Command(BaseCommand):
    """Check Redis Function support and optionally deploy the bundled library."""

    help = "Check Redis Function-library support; use --deploy to load the bundled library."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--deploy",
            action="store_true",
            help="Load or replace the bundled Redis Function library.",
        )
        parser.add_argument(
            "--rollback",
            action="store_true",
            help="Allow --deploy to lower the installed Function API version.",
        )
        parser.add_argument(
            "--redis-url",
            help="Exceptional single-target override; may expose credentials in shell history.",
        )

    def handle(
        self,
        *args,
        deploy: bool,
        redis_url: str | None,
        rollback: bool = False,
        **options,
    ) -> None:
        if rollback and not deploy:
            raise CommandError("--rollback requires --deploy.")
        queue_settings = {} if redis_url is not None else django_queue.queues.settings
        urls = resolve_redis_urls(queue_settings, redis_url=redis_url)
        if not urls:
            raise CommandError("No Redis queue URLs are configured.")
        library = load_function_library()
        for url in urls:
            try:
                client = redis.Redis.from_url(url)
            except (redis.RedisError, ValueError) as exc:
                raise CommandError(f"Redis Function check failed: {exc}") from exc
            try:
                if deploy:
                    with _deployment_lease(client):
                        installed = client.function_list(
                            library=FUNCTION_LIBRARY_NAME, withcode=True
                        )
                        if installed:
                            library_version, api_version = read_installed_library_info(
                                installed
                            )
                            if api_version > library.api_version and not rollback:
                                raise CommandError(
                                    "Installed Redis Function library api_version "
                                    f"{api_version} is newer than bundled "
                                    f"api_version {library.api_version}; use --rollback "
                                    "to deploy it explicitly."
                                )
                            if (
                                library_version == library.library_version
                                and api_version == library.api_version
                            ):
                                self.stdout.write(
                                    f"Redis Function library {FUNCTION_LIBRARY_NAME} is current."
                                )
                                continue
                        client.function_load(
                            library.source.decode("utf-8"), replace=True
                        )
                    self.stdout.write(
                        f"Loaded {FUNCTION_LIBRARY_NAME} {library.library_version}."
                    )
                else:
                    installed = client.function_list(
                        library=FUNCTION_LIBRARY_NAME, withcode=True
                    )
                    if not installed:
                        raise CommandError(
                            f"Redis Function library {FUNCTION_LIBRARY_NAME} is not "
                            "installed. Deployment required."
                        )
                    library_version, api_version = read_installed_library_info(
                        installed
                    )
                    api_status = (
                        "compatible"
                        if api_version >= library.api_version
                        else "incompatible"
                    )
                    message = "\n".join(
                        (
                            "Redis Function library is installed:",
                            "",
                            f"- {FUNCTION_LIBRARY_NAME} version {library_version}",
                            f"- api_version {api_version} ({api_status})",
                            "",
                            f"Bundled library_version: {library.library_version}",
                        )
                    )
                    if (
                        library_version != library.library_version
                        or api_version < library.api_version
                    ):
                        raise CommandError(f"{message}\nDeployment required.")
                    self.stdout.write(message)
            except redis.RedisError as exc:
                raise CommandError(f"Redis Function check failed: {exc}") from exc
            finally:
                client.close()


@contextmanager
def _deployment_lease(client):
    token = uuid4().hex
    if not client.set(
        _DEPLOYMENT_LOCK_KEY,
        token,
        nx=True,
        ex=_DEPLOYMENT_LOCK_SECONDS,
    ):
        raise CommandError("Another Redis Function-library deployment is in progress.")
    try:
        yield
    finally:
        client.eval(_RELEASE_DEPLOYMENT_LOCK, 1, _DEPLOYMENT_LOCK_KEY, token)
