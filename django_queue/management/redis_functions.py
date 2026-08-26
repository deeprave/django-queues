"""Shared support for Redis Function-library management commands."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, NoReturn, cast

import redis
from django.core.management.base import CommandError
from django.utils.module_loading import import_string
from redis.crc import key_slot

from django_queue.backends.exceptions import InvalidQueueBackendError
from django_queue.backends.redis.transport import (
    redact_redis_url,
    redis_client_kwargs,
    redis_tls_failure,
    stricter_tls_client_kwargs,
)

REDIS_TOPOLOGY_STANDALONE = "standalone"
REDIS_TOPOLOGY_CLUSTER = "cluster"
DEPLOYMENT_LOCK_KEY = "django_queues:function-deploy-lock"

_CLUSTER_NODE_ID = re.compile(r"^[0-9a-fA-F]{40}$")
_PRIMARY_CLIENT_OMIT = frozenset(
    {
        "host",
        "port",
        "path",
        "redis_connect_func",
        "connection_class",
        "connection_pool",
    }
)
_PRIMARY_CLIENT_KEEP = frozenset(
    {
        "username",
        "password",
        "db",
        "ssl",
        "encoding",
        "encoding_errors",
        "decode_responses",
        "socket_timeout",
        "socket_connect_timeout",
        "socket_keepalive",
        "socket_keepalive_options",
        "socket_type",
        "retry",
        "retry_on_timeout",
        "retry_on_error",
        "health_check_interval",
        "client_name",
        "lib_name",
        "lib_version",
        "credential_provider",
        "protocol",
    }
)


@dataclass(frozen=True, slots=True)
class RedisFunctionTarget:
    """A unique Redis Function-library deployment or compatibility target."""

    url: str
    topology: str
    aliases: tuple[str, ...] = ()
    address_remap: Callable[[tuple[str, int]], tuple[str, int]] | None = None
    client_kwargs: Mapping[str, Any] = field(default_factory=dict)


def warn_duplicate_cluster_seeds(
    cluster_identities: list[tuple[str, frozenset[str]]],
    write: Callable[[str], object],
) -> None:
    """Warn when distinct seed URLs discovered the same CLUSTER NODES ids."""
    seen: dict[frozenset[str], list[str]] = {}
    for url, node_ids in cluster_identities:
        if not node_ids:
            continue
        seen.setdefault(node_ids, []).append(url)
    for urls in seen.values():
        if len(urls) > 1:
            listed = ", ".join(redact_redis_url(url) for url in urls)
            write(
                "Warning: distinct Cluster seed URLs discovered the same "
                f"Redis Cluster node ids: {listed}."
            )


def raise_redis_command_error(exc: BaseException, url: str, prefix: str) -> NoReturn:
    """Re-raise a Redis client failure as CommandError, preserving TLS context."""
    if tls_error := redis_tls_failure(exc, url):
        raise CommandError(str(tls_error)) from exc
    raise CommandError(f"{prefix}: {exc}") from exc


def cluster_from_url_kwargs(target: RedisFunctionTarget) -> dict[str, Any]:
    """Keyword arguments ``RedisCluster.from_url`` needs for this target."""
    kwargs = dict(target.client_kwargs)
    if target.address_remap is not None:
        kwargs["address_remap"] = target.address_remap
    return kwargs


def read_library_info(result: object) -> tuple[str, int]:
    """Validate the stable Function-library introspection response."""
    if not isinstance(result, list) or len(result) != 2:
        raise CommandError(
            "Redis Function library returned invalid introspection data."
        )
    library_version, api_version = result
    if isinstance(library_version, bytes):
        try:
            library_version = library_version.decode("ascii")
        except UnicodeDecodeError as exc:
            raise CommandError(
                "Redis Function library returned invalid introspection data."
            ) from exc
    if (
        not isinstance(library_version, str)
        or isinstance(api_version, bool)
        or not isinstance(api_version, int)
    ):
        raise CommandError(
            "Redis Function library returned invalid introspection data."
        )
    return library_version, api_version


def _mapping_from_redis(value: object) -> Mapping[object, object] | None:
    """Normalise a Redis hash-like value to a mapping.

    FUNCTION LIST may return a dict (RESP3) or a flattened list of pairs
    (RESP2), depending on the client connection.
    """
    if isinstance(value, Mapping):
        return value
    if isinstance(value, list) and len(value) >= 2 and len(value) % 2 == 0:
        mapping: dict[object, object] = {}
        for index in range(0, len(value), 2):
            mapping[value[index]] = value[index + 1]
        return mapping
    return None


def read_installed_library_info(libraries: object) -> tuple[str, int]:
    """Read version metadata from a ``FUNCTION LIST WITHCODE`` response."""
    if not isinstance(libraries, list) or len(libraries) != 1:
        raise CommandError("Redis Function library returned invalid library metadata.")
    library = _mapping_from_redis(libraries[0])
    if library is None:
        raise CommandError("Redis Function library returned invalid library metadata.")
    source = library.get("library_code", library.get(b"library_code"))
    if isinstance(source, str):
        source = source.encode("utf-8")
    if not isinstance(source, bytes):
        raise CommandError("Redis Function library returned invalid library metadata.")

    prefix = b"-- django-queues-library-version: "
    api_prefix = b"-- django-queues-api-version: "
    lines = source.splitlines()
    library_line = next((line for line in lines if line.startswith(prefix)), None)
    api_line = next((line for line in lines if line.startswith(api_prefix)), None)
    if library_line is None or api_line is None:
        raise CommandError("Redis Function library returned invalid library metadata.")
    try:
        library_version = library_line.removeprefix(prefix).decode("ascii")
        api_version = int(api_line.removeprefix(api_prefix))
    except (UnicodeDecodeError, ValueError) as exc:
        raise CommandError(
            "Redis Function library returned invalid library metadata."
        ) from exc
    if not library_version or isinstance(api_version, bool):
        raise CommandError("Redis Function library returned invalid library metadata.")
    return library_version, api_version


def resolve_redis_targets(
    queue_settings: Mapping[str, Mapping[str, object]],
    *,
    redis_url: str | None = None,
    redis_cluster_url: str | None = None,
) -> tuple[RedisFunctionTarget, ...]:
    """Return configured Redis targets, unless an explicit override is set."""
    if redis_url is not None and redis_cluster_url is not None:
        raise CommandError(
            "--redis-url and --redis-cluster-url are mutually exclusive."
        )
    if redis_url is not None:
        return (
            RedisFunctionTarget(
                redis_url,
                REDIS_TOPOLOGY_STANDALONE,
                client_kwargs=_client_kwargs(redis_url),
            ),
        )
    if redis_cluster_url is not None:
        return (
            RedisFunctionTarget(
                redis_cluster_url,
                REDIS_TOPOLOGY_CLUSTER,
                client_kwargs=_client_kwargs(redis_cluster_url),
            ),
        )

    grouped: dict[tuple[str, str], list[str]] = {}
    remaps: dict[
        tuple[str, str], Callable[[tuple[str, int]], tuple[str, int]] | None
    ] = {}
    kwargs_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    order: list[tuple[str, str]] = []
    for alias, options in queue_settings.items():
        backend_path = options.get("BACKEND")
        if not isinstance(backend_path, str):
            raise CommandError(
                f"Queue '{alias}' BACKEND must be a non-empty dotted path."
            )
        try:
            backend = import_string(backend_path)
        except ImportError as exc:
            raise CommandError(
                f"Queue '{alias}' BACKEND could not be imported: {backend_path}"
            ) from exc
        if getattr(backend, "worker_provider_kind", None) != "redis":
            continue
        location = options.get("LOCATION")
        if not isinstance(location, str) or not location:
            raise CommandError(f"Redis queue '{alias}' must define a Redis LOCATION.")
        topology = getattr(backend, "redis_topology", REDIS_TOPOLOGY_STANDALONE)
        if topology not in (REDIS_TOPOLOGY_STANDALONE, REDIS_TOPOLOGY_CLUSTER):
            topology = REDIS_TOPOLOGY_STANDALONE
        key = (topology, location)
        remap = cast(
            Callable[[tuple[str, int]], tuple[str, int]] | None,
            options.get("address_remap")
            if callable(options.get("address_remap"))
            else None,
        )
        client_kwargs = _client_kwargs(location, options)
        if key not in grouped:
            grouped[key] = []
            remaps[key] = remap
            kwargs_by_key[key] = client_kwargs
            order.append(key)
        else:
            kwargs_by_key[key] = stricter_tls_client_kwargs(
                kwargs_by_key[key], client_kwargs
            )
        grouped[key].append(alias)
        if remaps[key] is None and remap is not None:
            remaps[key] = remap
    return tuple(
        RedisFunctionTarget(
            url,
            topology,
            tuple(grouped[(topology, url)]),
            remaps[(topology, url)],
            kwargs_by_key[(topology, url)],
        )
        for topology, url in order
    )


def _client_kwargs(
    url: str, options: Mapping[str, object] | None = None
) -> dict[str, Any]:
    try:
        return redis_client_kwargs(url, options)
    except InvalidQueueBackendError as exc:
        raise CommandError(str(exc)) from exc


def resolve_redis_urls(
    queue_settings: Mapping[str, Mapping[str, object]],
    *,
    redis_url: str | None = None,
    redis_cluster_url: str | None = None,
) -> tuple[str, ...]:
    """Return the configured Redis target URLs, unless an explicit override is set."""
    return tuple(
        target.url
        for target in resolve_redis_targets(
            queue_settings,
            redis_url=redis_url,
            redis_cluster_url=redis_cluster_url,
        )
    )


def parse_cluster_node_ids(nodes: object) -> frozenset[str]:
    """Extract 40-character CLUSTER NODES ids from a client response."""
    if isinstance(nodes, bytes):
        try:
            nodes = nodes.decode("utf-8")
        except UnicodeDecodeError:
            return frozenset()
    if isinstance(nodes, str):
        ids: set[str] = set()
        for line in nodes.splitlines():
            first = line.split(maxsplit=1)[0] if line.strip() else ""
            if _CLUSTER_NODE_ID.fullmatch(first):
                ids.add(first.lower())
        return frozenset(ids)
    if isinstance(nodes, Mapping):
        ids = set()
        for key, detail in nodes.items():
            candidates: list[object] = [key]
            if isinstance(detail, Mapping):
                candidates.extend(
                    detail.get(field)
                    for field in ("node_id", "id", "name", b"node_id", b"id", b"name")
                )
            for candidate in candidates:
                if isinstance(candidate, bytes):
                    try:
                        candidate = candidate.decode("ascii")
                    except UnicodeDecodeError:
                        continue
                if isinstance(candidate, str) and _CLUSTER_NODE_ID.fullmatch(candidate):
                    ids.add(candidate.lower())
        return frozenset(ids)
    return frozenset()


def cluster_node_ids(client) -> frozenset[str]:
    """Return CLUSTER NODES ids discovered by a sync Cluster client."""
    return parse_cluster_node_ids(client.execute_command("CLUSTER NODES"))


def iter_cluster_primary_clients(
    cluster,
) -> Iterator[tuple[redis.Redis, str, object]]:
    """Yield ``(client, advertised host:port, primary node)`` for each primary."""
    manager = getattr(cluster, "nodes_manager", None)
    if manager is not None:
        initialize = getattr(manager, "initialize", None)
        slots = getattr(manager, "slots_cache", None)
        if initialize is not None and not slots:
            initialize()
    remap = getattr(manager, "remap_host_port", None)
    connection_kwargs = _primary_client_kwargs(
        getattr(manager, "connection_kwargs", None)
    )
    for node in cluster.get_primaries():
        host, port = node.host, int(node.port)
        advertised = f"{host}:{port}"
        if remap is not None:
            host, port = remap(host, port)
        yield redis.Redis(host=host, port=port, **connection_kwargs), advertised, node


def _primary_client_kwargs(stored: object) -> dict[str, Any]:
    """Copy seed TLS and credentials onto a per-primary ``Redis()`` client."""
    if not isinstance(stored, Mapping):
        return {}
    kwargs = {
        key: value
        for key, value in stored.items()
        if key not in _PRIMARY_CLIENT_OMIT
        and (key in _PRIMARY_CLIENT_KEEP or key.startswith("ssl_"))
    }
    connection_class = stored.get("connection_class")
    if kwargs.get("ssl") or (
        connection_class is not None
        and "SSL" in getattr(connection_class, "__name__", "")
    ):
        kwargs["ssl"] = True
    return kwargs


def deployment_lock_key(cluster, node) -> str:
    """Return a lock key that hashes to a slot owned by *node* when possible."""
    manager = getattr(cluster, "nodes_manager", None)
    slots_cache = getattr(manager, "slots_cache", None) if manager else None
    if not slots_cache:
        return DEPLOYMENT_LOCK_KEY
    node_name = getattr(node, "name", None) or f"{node.host}:{node.port}"
    slot = None
    for candidate, owners in slots_cache.items():
        if not owners:
            continue
        owner = owners[0]
        owner_name = getattr(owner, "name", None) or f"{owner.host}:{owner.port}"
        if owner is node or owner_name == node_name:
            slot = candidate
            break
    if slot is None:
        return DEPLOYMENT_LOCK_KEY
    return _lock_key_for_slot(slot)


def _lock_key_for_slot(slot: int) -> str:
    for index in range(1_000_000):
        tag = str(index)
        if key_slot(f"{{{tag}}}".encode()) == slot:
            return f"{{{tag}}}:function-deploy-lock"
    raise CommandError(f"Unable to place a deployment lock in hash slot {slot}.")
