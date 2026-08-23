"""Check application-role compatibility with the Redis Function library."""

from __future__ import annotations

import redis
from django.core.management.base import BaseCommand, CommandError

import django_queue
from django_queue.backends.redis.functions import FUNCTION_API_VERSION
from django_queue.management.redis_functions import (
    read_library_info,
    resolve_redis_urls,
)


class Command(BaseCommand):
    """Check that application credentials can invoke the Function library."""

    help = "Check application FCALL access to the Redis Function library."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--redis-url",
            help="Exceptional single-target override; may expose credentials in shell history.",
        )

    def handle(self, *args, redis_url: str | None, **options) -> None:
        queue_settings = {} if redis_url is not None else django_queue.queues.settings
        urls = resolve_redis_urls(queue_settings, redis_url=redis_url)
        if not urls:
            raise CommandError("No Redis queue URLs are configured.")
        for url in urls:
            try:
                client = redis.Redis.from_url(url)
            except (redis.RedisError, ValueError) as exc:
                raise CommandError(
                    f"Redis Function compatibility check failed: {exc}"
                ) from exc
            try:
                result = client.fcall("django_queue_info", 0)
                library_version, api_version = read_library_info(result)
                if api_version < FUNCTION_API_VERSION:
                    raise CommandError(
                        "Redis Function library api_version "
                        f"{api_version} is below the required {FUNCTION_API_VERSION}."
                    )
                self.stdout.write(
                    f"Redis Function library {library_version} (api_version {api_version}) is compatible."
                )
            except redis.RedisError as exc:
                raise CommandError(
                    f"Redis Function compatibility check failed: {exc}"
                ) from exc
            finally:
                client.close()
