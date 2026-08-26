"""Derive Redis client constructor kwargs from LOCATION and TLS OPTIONS."""

from __future__ import annotations

import ssl
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import redis
from redis.exceptions import AuthenticationError, AuthorizationError, BusyLoadingError

from django_queue.backends.exceptions import InvalidQueueBackendError

_TLS_OPTION_KEYS = frozenset(
    {
        "ssl",
        "ssl_ca_certs",
        "ssl_ca_data",
        "ssl_ca_path",
        "ssl_certfile",
        "ssl_keyfile",
        "ssl_cert_reqs",
        "ssl_check_hostname",
        "ssl_password",
        "ssl_min_version",
        "ssl_ciphers",
        "ssl_include_verify_flags",
        "ssl_exclude_verify_flags",
        "ssl_validate_ocsp",
        "ssl_validate_ocsp_stapled",
    }
)
_FALSE_SSL_VALUES = frozenset({"false", "0", "no", "none"})
_CERT_REQS_RANK = {
    "none": 0,
    "optional": 1,
    "required": 2,
    ssl.CERT_NONE: 0,
    ssl.CERT_OPTIONAL: 1,
    ssl.CERT_REQUIRED: 2,
    0: 0,
    1: 1,
    2: 2,
}


def redis_client_kwargs(
    url: str, options: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Return extra kwargs for ``from_url`` / ``Redis(...)`` from *url* and OPTIONS.

    ``rediss://`` selects TLS. TLS keys in OPTIONS or the URL query on a
    ``redis://`` LOCATION are a configuration error. OPTIONS override URL query
    SSL keys on ``rediss://``. ``ssl`` is never passed to ``from_url``.
    """
    options = {} if options is None else dict(options)
    scheme = urlsplit(url).scheme.lower()
    tls_options = {key: options[key] for key in _TLS_OPTION_KEYS if key in options}
    url_tls_keys = _url_tls_keys(url)
    if scheme != "rediss" and (tls_options or url_tls_keys):
        raise InvalidQueueBackendError(
            "TLS options require a rediss:// LOCATION; "
            f"the configured URL uses {scheme or 'an empty scheme'}://"
        )
    kwargs: dict[str, Any] = {}
    if scheme == "rediss":
        if _ssl_explicitly_disabled(tls_options.get("ssl")) or "ssl" in url_tls_keys:
            raise InvalidQueueBackendError(
                "rediss:// selects TLS; do not set ssl=False in OPTIONS or ssl "
                "in the URL query"
            )
        tls_options.pop("ssl", None)
        kwargs.update(tls_options)
    return kwargs


def redis_tls_failure(exc: BaseException, url: str) -> InvalidQueueBackendError | None:
    """Return an actionable TLS error when *exc* is a handshake or reachability failure."""
    scheme = urlsplit(url).scheme.lower()
    handshake = _is_tls_failure(exc)
    reachability = scheme == "rediss" and _is_tls_reachability_failure(exc)
    if not handshake and not reachability:
        return None
    redacted = redact_redis_url(url)
    if handshake:
        return InvalidQueueBackendError(
            "TLS handshake failed for the configured rediss:// Redis target; "
            "every advertised endpoint must be reachable with the configured "
            f"TLS settings ({redacted})."
        )
    return InvalidQueueBackendError(
        "Could not establish a TLS connection to the configured rediss:// Redis "
        "target; every advertised endpoint must be reachable with the configured "
        f"TLS settings ({redacted})."
    )


def redact_redis_url(url: str) -> str:
    """Return *url* without userinfo or query string."""
    parsed = urlsplit(url)
    if parsed.username is None and parsed.password is None and not parsed.query:
        return url
    hostname = parsed.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    netloc = hostname
    if parsed.port is not None:
        netloc = f"{hostname}:{parsed.port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, "", parsed.fragment))


def stricter_tls_client_kwargs(
    current: Mapping[str, Any], incoming: Mapping[str, Any]
) -> dict[str, Any]:
    """Return TLS kwargs combining *current* with *incoming*, keeping stricter verify."""
    merged = dict(current)
    if incoming.get("ssl_check_hostname") is True:
        merged["ssl_check_hostname"] = True
    elif "ssl_check_hostname" not in merged and "ssl_check_hostname" in incoming:
        merged["ssl_check_hostname"] = incoming["ssl_check_hostname"]
    stricter_reqs = _stricter_cert_reqs(
        merged.get("ssl_cert_reqs"), incoming.get("ssl_cert_reqs")
    )
    if stricter_reqs is not None:
        merged["ssl_cert_reqs"] = stricter_reqs
    for key in (
        "ssl_ca_certs",
        "ssl_ca_path",
        "ssl_ca_data",
        "ssl_certfile",
        "ssl_keyfile",
    ):
        if incoming.get(key) and not merged.get(key):
            merged[key] = incoming[key]
    return merged


def _ssl_explicitly_disabled(value: object) -> bool:
    if value is False or value == 0:
        return True
    return isinstance(value, str) and value.lower() in _FALSE_SSL_VALUES


def _stricter_cert_reqs(current: object, incoming: object) -> object | None:
    current_rank = _cert_reqs_rank(current)
    incoming_rank = _cert_reqs_rank(incoming)
    if incoming_rank is None:
        return current
    if current_rank is None or incoming_rank > current_rank:
        return incoming
    return current


def _cert_reqs_rank(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        return _CERT_REQS_RANK.get(value.lower())
    return _CERT_REQS_RANK.get(value)


def _url_tls_keys(url: str) -> dict[str, Any]:
    try:
        parsed = redis.connection.parse_url(url)
    except AttributeError, TypeError, ValueError:
        return {}
    return {key: parsed[key] for key in _TLS_OPTION_KEYS if key in parsed}


def _is_tls_failure(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _is_auth_or_loading_error(current):
            return False
        if isinstance(current, ssl.SSLError):
            return True
        name = type(current).__name__
        if "SSL" in name or "Certificate" in name:
            return True
        message = str(current).lower()
        if "ssl" in message or "certificate verify failed" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_tls_reachability_failure(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _is_auth_or_loading_error(current):
            return False
        if _is_connect_timeout(current):
            return True
        if type(current) is redis.ConnectionError:
            return True
        if isinstance(current, ConnectionError) and not isinstance(
            current, redis.RedisError
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_auth_or_loading_error(exc: BaseException) -> bool:
    return isinstance(exc, AuthenticationError | AuthorizationError | BusyLoadingError)


def _is_connect_timeout(exc: BaseException) -> bool:
    if not isinstance(exc, TimeoutError | redis.TimeoutError):
        return False
    return "connecting" in str(exc).lower()
