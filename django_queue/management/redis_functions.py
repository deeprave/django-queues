"""Shared support for Redis Function-library management commands."""

from __future__ import annotations

from collections.abc import Mapping

from django.core.management.base import CommandError
from django.utils.module_loading import import_string


def read_library_info(result: object) -> tuple[str, int]:
    """Validate the stable Function-library introspection response."""
    if not isinstance(result, list) or len(result) != 2:
        raise CommandError(
            "Redis Function library returned invalid introspection data."
        )
    library_version, api_version = result
    if isinstance(library_version, bytes):
        library_version = library_version.decode("ascii")
    if not isinstance(library_version, str) or not isinstance(api_version, int):
        raise CommandError(
            "Redis Function library returned invalid introspection data."
        )
    return library_version, api_version


def read_installed_library_info(libraries: object) -> tuple[str, int]:
    """Read version metadata from a ``FUNCTION LIST WITHCODE`` response."""
    if not isinstance(libraries, list) or len(libraries) != 1:
        raise CommandError("Redis Function library returned invalid library metadata.")
    library = libraries[0]
    if not isinstance(library, Mapping):
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


def resolve_redis_urls(
    queue_settings: Mapping[str, Mapping[str, object]], *, redis_url: str | None = None
) -> tuple[str, ...]:
    """Return the configured Redis targets, unless an explicit override is set."""
    if redis_url is not None:
        return (redis_url,)

    urls: dict[str, None] = {}
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
        urls[location] = None
    return tuple(urls)
