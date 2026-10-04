"""File-backed record storage used by every domain package."""

from portpower.store.adapter import AdapterRegistry, DocumentAdapter
from portpower.store.factory import create_record_store
from portpower.store.kvstore import RecordStore
from portpower.store.migration import (
    MigrationStatus,
    VersionedRecordStore,
)

__all__ = [
    "AdapterRegistry",
    "DocumentAdapter",
    "MigrationStatus",
    "RecordStore",
    "VersionedRecordStore",
    "create_record_store",
]
