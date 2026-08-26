"""Check application-role compatibility with the Redis Function library."""

from __future__ import annotations

import redis
from django.core.management.base import BaseCommand, CommandError
from redis.cluster import RedisCluster

import django_queue
from django_queue.backends.redis.functions import FUNCTION_API_VERSION
from django_queue.management.redis_functions import (
    REDIS_TOPOLOGY_CLUSTER,
    REDIS_TOPOLOGY_STANDALONE,
    cluster_from_url_kwargs,
    cluster_node_ids,
    iter_cluster_primary_clients,
    read_library_info,
    resolve_redis_targets,
    warn_duplicate_cluster_seeds,
)


class Command(BaseCommand):
    """Check that application credentials can invoke the Function library."""

    help = "Check application FCALL access to the Redis Function library."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--redis-url",
            help="Exceptional standalone-only override; may expose credentials in shell history.",
        )
        parser.add_argument(
            "--redis-cluster-url",
            help="Exceptional Cluster-only seed override; may expose credentials in shell history.",
        )

    def handle(
        self,
        *args,
        redis_url: str | None = None,
        redis_cluster_url: str | None = None,
        **options,
    ) -> None:
        queue_settings = (
            {}
            if redis_url is not None or redis_cluster_url is not None
            else django_queue.queues.settings
        )
        targets = resolve_redis_targets(
            queue_settings,
            redis_url=redis_url,
            redis_cluster_url=redis_cluster_url,
        )
        if not targets:
            raise CommandError("No Redis queue URLs are configured.")
        cluster_identities: list[tuple[str, frozenset[str]]] = []
        for target in targets:
            if target.topology == REDIS_TOPOLOGY_CLUSTER:
                self._check_cluster_target(target, cluster_identities)
            elif target.topology == REDIS_TOPOLOGY_STANDALONE:
                self._check_standalone_target(target)
            else:
                raise CommandError(f"Unsupported Redis topology {target.topology!r}.")
        warn_duplicate_cluster_seeds(cluster_identities, self.stderr.write)

    def _check_standalone_target(self, target) -> None:
        try:
            client = redis.Redis.from_url(target.url)
        except (redis.RedisError, ValueError) as exc:
            raise CommandError(
                f"Redis Function compatibility check failed: {exc}"
            ) from exc
        try:
            self._report_compatibility(client.fcall("django_queue_info", 0))
        except redis.RedisError as exc:
            raise CommandError(
                f"Redis Function compatibility check failed: {exc}"
            ) from exc
        finally:
            client.close()

    def _check_cluster_target(
        self,
        target,
        cluster_identities: list[tuple[str, frozenset[str]]],
    ) -> None:
        try:
            cluster = RedisCluster.from_url(
                target.url, **cluster_from_url_kwargs(target)
            )
        except (redis.RedisError, ValueError, TypeError) as exc:
            raise CommandError(
                f"Redis Function compatibility check failed: {exc}"
            ) from exc
        try:
            try:
                cluster_identities.append((target.url, cluster_node_ids(cluster)))
            except redis.RedisError, AttributeError, TypeError, ValueError:
                pass
            found = False
            for client, advertised, _node in iter_cluster_primary_clients(cluster):
                found = True
                try:
                    self._report_compatibility(client.fcall("django_queue_info", 0))
                except redis.RedisError as exc:
                    raise CommandError(
                        f"Redis Function compatibility check failed on {advertised}: {exc}"
                    ) from exc
                finally:
                    client.close()
            if not found:
                raise CommandError(
                    "Redis Cluster seed did not discover any primary nodes."
                )
        finally:
            cluster.close()

    def _report_compatibility(self, result) -> None:
        library_version, api_version = read_library_info(result)
        if api_version < FUNCTION_API_VERSION:
            raise CommandError(
                "Redis Function library api_version "
                f"{api_version} is below the required {FUNCTION_API_VERSION}."
            )
        self.stdout.write(
            f"Redis Function library {library_version} (api_version {api_version}) is compatible."
        )
