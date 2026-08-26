"""Check and explicitly deploy the bundled Redis Function library."""

from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import redis
from django.core.management.base import BaseCommand, CommandError
from redis.cluster import RedisCluster

import django_queue
from django_queue.backends.redis.functions import (
    FUNCTION_LIBRARY_NAME,
    load_function_library,
)
from django_queue.management.redis_functions import (
    DEPLOYMENT_LOCK_KEY,
    REDIS_TOPOLOGY_CLUSTER,
    REDIS_TOPOLOGY_STANDALONE,
    cluster_from_url_kwargs,
    cluster_node_ids,
    deployment_lock_key,
    iter_cluster_primary_clients,
    raise_redis_command_error,
    read_installed_library_info,
    resolve_redis_targets,
    warn_duplicate_cluster_seeds,
)

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
            help="Exceptional standalone-only override; may expose credentials in shell history.",
        )
        parser.add_argument(
            "--redis-cluster-url",
            help="Exceptional Cluster-only seed override; may expose credentials in shell history.",
        )

    def handle(
        self,
        *args,
        deploy: bool,
        redis_url: str | None = None,
        redis_cluster_url: str | None = None,
        rollback: bool = False,
        **options,
    ) -> None:
        if rollback and not deploy:
            raise CommandError("--rollback requires --deploy.")
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
        library = load_function_library()
        cluster_identities: list[tuple[str, frozenset[str]]] = []
        for target in targets:
            if target.topology == REDIS_TOPOLOGY_CLUSTER:
                self._handle_cluster_target(
                    target,
                    library=library,
                    deploy=deploy,
                    rollback=rollback,
                    cluster_identities=cluster_identities,
                )
            elif target.topology == REDIS_TOPOLOGY_STANDALONE:
                self._handle_standalone_target(
                    target, library=library, deploy=deploy, rollback=rollback
                )
            else:
                raise CommandError(f"Unsupported Redis topology {target.topology!r}.")
        warn_duplicate_cluster_seeds(cluster_identities, self.stderr.write)

    def _handle_standalone_target(
        self, target, *, library, deploy: bool, rollback: bool
    ) -> None:
        try:
            client = redis.Redis.from_url(target.url, **target.client_kwargs)
        except (redis.RedisError, ValueError) as exc:
            raise_redis_command_error(exc, target.url, "Redis Function check failed")
        try:
            self._process_client(
                client, library=library, deploy=deploy, rollback=rollback
            )
        except redis.RedisError as exc:
            raise_redis_command_error(exc, target.url, "Redis Function check failed")
        finally:
            client.close()

    def _handle_cluster_target(
        self,
        target,
        *,
        library,
        deploy: bool,
        rollback: bool,
        cluster_identities: list[tuple[str, frozenset[str]]],
    ) -> None:
        try:
            cluster = RedisCluster.from_url(
                target.url, **cluster_from_url_kwargs(target)
            )
        except (redis.RedisError, ValueError, TypeError) as exc:
            raise_redis_command_error(exc, target.url, "Redis Function check failed")
        try:
            try:
                cluster_identities.append((target.url, cluster_node_ids(cluster)))
            except redis.RedisError, AttributeError, TypeError, ValueError:
                pass
            found = False
            for client, advertised, node in iter_cluster_primary_clients(cluster):
                found = True
                try:
                    self._process_client(
                        client,
                        library=library,
                        deploy=deploy,
                        rollback=rollback,
                        node=advertised,
                        lock_key=deployment_lock_key(cluster, node),
                    )
                except redis.RedisError as exc:
                    raise_redis_command_error(
                        exc,
                        target.url,
                        f"Redis Function check failed on {advertised}",
                    )
                finally:
                    client.close()
            if not found:
                raise CommandError(
                    "Redis Cluster seed did not discover any primary nodes."
                )
        finally:
            cluster.close()

    def _process_client(
        self,
        client,
        *,
        library,
        deploy: bool,
        rollback: bool,
        node: str | None = None,
        lock_key: str = DEPLOYMENT_LOCK_KEY,
    ) -> None:
        if deploy:
            with _deployment_lease(client, node=node, lock_key=lock_key):
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
                        return
                client.function_load(library.source.decode("utf-8"), replace=True)
            self.stdout.write(
                f"Loaded {FUNCTION_LIBRARY_NAME} {library.library_version}."
            )
            return

        installed = client.function_list(library=FUNCTION_LIBRARY_NAME, withcode=True)
        if not installed:
            raise CommandError(
                f"Redis Function library {FUNCTION_LIBRARY_NAME} is not "
                "installed. Deployment required."
            )
        library_version, api_version = read_installed_library_info(installed)
        api_status = (
            "compatible" if api_version >= library.api_version else "incompatible"
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


@contextmanager
def _deployment_lease(
    client, *, node: str | None = None, lock_key: str = DEPLOYMENT_LOCK_KEY
):
    token = uuid4().hex
    if not client.set(
        lock_key,
        token,
        nx=True,
        ex=_DEPLOYMENT_LOCK_SECONDS,
    ):
        location = f" on {node}" if node else ""
        raise CommandError(
            f"Another Redis Function-library deployment is in progress{location}."
        )
    try:
        yield
    finally:
        client.eval(_RELEASE_DEPLOYMENT_LOCK, 1, lock_key, token)
