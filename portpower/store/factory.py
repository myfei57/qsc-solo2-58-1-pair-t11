"""Factory for choosing legacy and online-migrating storage."""

from __future__ import annotations

import os

from portpower.store.adapter import AdapterRegistry
from portpower.store.kvstore import RecordStore
from portpower.store.migration import VersionedRecordStore


def create_record_store(
    directory: str | os.PathLike[str],
    store_format: str = "versioned",
    adapters: AdapterRegistry | None = None,
) -> RecordStore:
    if store_format == "legacy":
        return RecordStore(directory)
    if store_format == "versioned":
        return VersionedRecordStore(directory, adapters=adapters)
    raise ValueError(
        f"invalid store format: {store_format!r} "
        "(expected 'legacy' or 'versioned')"
    )
