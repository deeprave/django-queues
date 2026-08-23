"""Validation for queue names used as Redis key-schema segments."""

from .backends import InvalidQueueBackendError

_QUEUE_ALIAS_CHARACTERS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
)


def validate_queue_alias(alias: object) -> str:
    """Return a queue alias that is safe to embed in Redis keys."""
    if not isinstance(alias, str) or not alias:
        raise InvalidQueueBackendError(
            f"Queue alias {alias!r} must be a non-empty string"
        )
    if any(character not in _QUEUE_ALIAS_CHARACTERS for character in alias):
        raise InvalidQueueBackendError(
            f"Queue alias '{alias}' must contain only ASCII letters, digits, _, or -"
        )
    return alias
