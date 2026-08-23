"""Bundled Redis Function-library metadata and package-resource loading."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache
from importlib import resources

_METADATA_PATTERN = re.compile(
    rb"^-- django-queues-library-version: (?P<library_version>\d{6}_\d{6})\n"
    rb"^-- django-queues-api-version: (?P<api_version>\d+)\n",
    re.MULTILINE,
)


@dataclass(frozen=True, slots=True)
class FunctionLibrary:
    """The source and compatibility metadata for the bundled Redis library."""

    name: str
    source: bytes
    library_version: str
    api_version: int


@cache
def load_function_library() -> FunctionLibrary:
    """Load the Function library from the installed package resource."""
    source = resources.files(__package__).joinpath("library.lua").read_bytes()
    if match := _METADATA_PATTERN.search(source):
        return FunctionLibrary(
            name="django_queues",
            source=source,
            library_version=match["library_version"].decode("ascii"),
            api_version=int(match["api_version"]),
        )
    raise RuntimeError("Bundled Redis Function library has invalid metadata")


_FUNCTION_LIBRARY = load_function_library()
FUNCTION_LIBRARY_NAME = _FUNCTION_LIBRARY.name
FUNCTION_LIBRARY_VERSION = _FUNCTION_LIBRARY.library_version
FUNCTION_API_VERSION = _FUNCTION_LIBRARY.api_version
