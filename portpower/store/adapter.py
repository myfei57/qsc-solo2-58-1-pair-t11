"""Selectors for converting documents between two storage formats."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

DocumentTransform = Callable[[Any], Any]


@dataclass(frozen=True)
class DocumentAdapter:
    """Bidirectional conversion for one document shape.

    ``upgrade`` produces the canonical new representation. ``downgrade``
    produces the representation that an old process must continue to read
    while the compatibility layer and rollback window are active.
    """

    upgrade: DocumentTransform
    downgrade: DocumentTransform


def _identity(document: Any) -> Any:
    return document


IDENTITY_ADAPTER = DocumentAdapter(upgrade=_identity, downgrade=_identity)


class AdapterRegistry:
    """Resolve an adapter by exact key or key prefix."""

    def __init__(self, default: DocumentAdapter = IDENTITY_ADAPTER) -> None:
        self._default = default
        self._exact: dict[str, DocumentAdapter] = {}
        self._prefixes: list[tuple[str, DocumentAdapter]] = []

    def register(
        self,
        key_pattern: str,
        adapter: DocumentAdapter,
        *,
        exact: bool = False,
    ) -> None:
        if exact:
            self._exact[key_pattern] = adapter
        else:
            self._prefixes.append((key_pattern, adapter))
            self._prefixes.sort(key=lambda item: len(item[0]), reverse=True)

    def for_key(self, key: str) -> DocumentAdapter:
        adapter = self._exact.get(key)
        if adapter is not None:
            return adapter
        for prefix, candidate in self._prefixes:
            if key.startswith(prefix):
                return candidate
        return self._default
