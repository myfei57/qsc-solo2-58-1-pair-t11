"""Online dual-format storage and resumable document migration.

The old store persists one ``<key>.json`` file per document. The versioned
store adds ``<key>.v2`` as the canonical new representation and keeps the JSON
projection in place for old processes. A small journal makes dual writes
crash-recoverable; per-key state makes background migration idempotent.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portpower.store.adapter import AdapterRegistry, IDENTITY_ADAPTER
from portpower.store.kvstore import RecordStore, decode, encode, validate_key

CURRENT_SUFFIX = ".v2"
LEGACY_SUFFIX = ".json"
STATE_DIRNAME = ".migration"
STATE_FILENAME = "state.json"
CURSOR_FILENAME = "cursor"
JOURNAL_FILENAME = "write-journal.json"
TMP_SUFFIX = ".tmp"

_STATUS_MIGRATED = "migrated"
_STATUS_FAILED = "failed"


@dataclass
class MigrationStatus:
    """Observable progress and compatibility-read counters."""

    total_keys: int
    migrated_keys: int
    pending_keys: int
    failed_keys: int
    current_files: int
    legacy_files: int
    migration_progress: float
    native_reads: int
    compatible_reads: int
    total_reads: int
    compatible_read_ratio: float
    writes: int
    cursor: str | None = None
    has_more: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "total_keys": self.total_keys,
            "migrated_keys": self.migrated_keys,
            "pending_keys": self.pending_keys,
            "failed_keys": self.failed_keys,
            "current_files": self.current_files,
            "legacy_files": self.legacy_files,
            "migration_progress": self.migration_progress,
            "native_reads": self.native_reads,
            "compatible_reads": self.compatible_reads,
            "total_reads": self.total_reads,
            "compatible_read_ratio": self.compatible_read_ratio,
            "writes": self.writes,
            "cursor": self.cursor,
            "has_more": self.has_more,
        }


class MigrationConflictError(RuntimeError):
    """Raised when legacy and current documents diverged unexpectedly."""


class VersionedRecordStore(RecordStore):
    """Store current documents while maintaining old-readable projections."""

    def __init__(
        self,
        directory: str | os.PathLike[str],
        adapters: AdapterRegistry | None = None,
    ) -> None:
        super().__init__(directory)
        self._adapters = adapters or AdapterRegistry(IDENTITY_ADAPTER)
        self._current_suffix = CURRENT_SUFFIX
        self._legacy: dict[str, bytes] = {}
        self._current: dict[str, bytes] = {}
        self._migration_state: dict[str, dict[str, str]] = {}
        self._native_reads = 0
        self._compatible_reads = 0
        self._writes = 0

    def load(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._legacy.clear()
            self._current.clear()
            self._data.clear()
            state_removed = False
            self._migration_state = self._load_state_locked()
            self._recover_journal_locked()

            for path in sorted(self._dir.iterdir()):
                if not path.is_file() or path.name.endswith(TMP_SUFFIX):
                    continue
                if path.name.endswith(self._current_suffix):
                    key = path.name[: -len(self._current_suffix)]
                    state_removed = (
                        self._load_current_file_locked(key, path) or state_removed
                    )
                elif path.name.endswith(LEGACY_SUFFIX):
                    key = path.name[: -len(LEGACY_SUFFIX)]
                    try:
                        self._legacy[validate_key(key)] = path.read_bytes()
                    except KeyError:
                        continue
            if state_removed:
                self._migration_state = {
                    key: value
                    for key, value in self._migration_state.items()
                    if key in self._legacy or key in self._current
                }
                self._write_state_locked()
            self._refresh_logical_snapshot_locked()

    def put(self, key: str, payload: bytes) -> None:
        name = validate_key(key)
        document = decode(payload)
        adapter = self._adapters.for_key(name)
        legacy_payload = encode(adapter.downgrade(document))
        with self._lock:
            self._write_journal_locked(
                {
                    "op": "put",
                    "key": name,
                    "current_payload": base64.b64encode(payload).decode("ascii"),
                    "legacy_payload": base64.b64encode(legacy_payload).decode(
                        "ascii"
                    ),
                    "current_hash": digest(payload),
                    "legacy_hash": digest(legacy_payload),
                }
            )
            # Both files are fsynced as far as this file store's existing
            # contract provides. Their shared hash records identify externally
            # rewritten legacy files even when filesystem timestamps are coarse.
            self._atomic_write_locked(self._current_path(name), payload)
            self._atomic_write_locked(self._legacy_path(name), legacy_payload)
            self._journal_path.unlink(missing_ok=True)
            self._current[name] = payload
            self._legacy[name] = legacy_payload
            self._migration_state[name] = {
                "status": _STATUS_MIGRATED,
                "source_hash": digest(legacy_payload),
                "current_hash": digest(payload),
            }
            self._write_state_locked()
            self._refresh_logical_snapshot_locked()
            self._writes += 1

    def get(self, key: str) -> bytes | None:
        name = validate_key(key)
        with self._lock:
            legacy, current = self._read_physical_locked(name)
            if current is not None and not self._current_is_stale_locked(
                name, legacy, current
            ):
                self._native_reads += 1
                return current

            if legacy is None:
                if current is None:
                    return None
                self._native_reads += 1
                return current

            try:
                upgraded = encode(self._adapters.for_key(name).upgrade(decode(legacy)))
            except (TypeError, ValueError):
                # Corrupt or adapter-incompatible bytes remain visible to raw
                # callers; get_json preserves the old store's None behaviour.
                upgraded = legacy
            self._compatible_reads += 1
            return upgraded

    def has(self, key: str) -> bool:
        name = validate_key(key)
        with self._lock:
            legacy, current = self._read_physical_locked(name)
            return legacy is not None or current is not None

    def delete(self, key: str) -> None:
        name = validate_key(key)
        with self._lock:
            self._write_journal_locked({"op": "delete", "key": name})
            self._legacy_path(name).unlink(missing_ok=True)
            self._current_path(name).unlink(missing_ok=True)
            self._journal_path.unlink(missing_ok=True)
            self._current.pop(name, None)
            self._legacy.pop(name, None)
            self._migration_state.pop(name, None)
            self._refresh_logical_snapshot_locked()

    def update_json(self, key: str, mutator):
        name = validate_key(key)
        with self._lock:
            current_raw = self.get(name)
            current: Any = None
            if current_raw is not None:
                try:
                    current = decode(current_raw)
                except ValueError:
                    current = None
            new_document, result = mutator(current)
            if new_document is not None:
                self.put(name, encode(new_document))
            return result

    def migrate_key(self, key: str) -> str:
        """Migrate one key and return its resulting state."""

        name = validate_key(key)
        with self._lock:
            return self._migrate_key_locked(name)

    def migrate(
        self,
        limit: int | None = None,
        *,
        resume_after: str | None = None,
    ) -> MigrationStatus:
        """Migrate up to ``limit`` pending keys; repeat runs are safe."""

        with self._lock:
            self._scan_disk_locked()
            keys = sorted(set(self._legacy) | set(self._current))
            if resume_after is None and self._cursor_path.exists():
                resume_after = self._cursor_path.read_text(encoding="utf-8")
            if resume_after is not None:
                cursor = validate_key(resume_after)
                keys = [key for key in keys if key > cursor]
        processed = 0
        examined = 0
        last_key = resume_after
        reached_limit = False
        for key in keys:
            if limit is not None and examined >= limit:
                reached_limit = True
                break
            result = self.migrate_key(key)
            examined += 1
            last_key = key
            if result in {"migrated", "reconciled"}:
                processed += 1
            elif result in {"missing", "current-only", "superseded"}:
                continue
        has_more = (
            reached_limit
            and limit is not None
            and examined >= limit
            and examined < len(keys)
        )
        with self._lock:
            if has_more:
                self._atomic_write_locked(
                    self._cursor_path, last_key.encode("utf-8")
                )
            else:
                self._cursor_path.unlink(missing_ok=True)
            status = self._status_locked()
        status.cursor = last_key if has_more else None
        status.has_more = has_more
        return status

    def sync_disk(self) -> MigrationStatus:
        """Pick up files written by an old process still serving traffic."""

        with self._lock:
            self._scan_disk_locked()
            return self._status_locked()

    def migration_status(self) -> MigrationStatus:
        with self._lock:
            self._scan_disk_locked()
            return self._status_locked()

    def _status_locked(self) -> MigrationStatus:
        migrated, pending, failed = self._migration_counts_locked()
        total = len(set(self._legacy) | set(self._current))
        reads = self._native_reads + self._compatible_reads
        progress = migrated / total if total else 1.0
        ratio = self._compatible_reads / reads if reads else 0.0
        return MigrationStatus(
            total_keys=total,
            migrated_keys=migrated,
            pending_keys=pending,
            failed_keys=failed,
            current_files=len(self._current),
            legacy_files=len(self._legacy),
            migration_progress=progress,
            native_reads=self._native_reads,
            compatible_reads=self._compatible_reads,
            total_reads=reads,
            compatible_read_ratio=ratio,
            writes=self._writes,
        )

    def export_documents(self) -> dict[str, Any]:
        """Return the logical snapshot using current documents where present."""

        result: dict[str, Any] = {}
        with self._lock:
            for key in sorted(set(self._legacy) | set(self._current)):
                raw = self.get(key)
                if raw is not None:
                    result[key] = decode(raw)
        return result

    @contextmanager
    def transaction(self) -> Iterator[dict[str, bytes]]:
        with self._lock:
            yield self._data

    @property
    def _state_dir(self) -> Path:
        return self._dir / STATE_DIRNAME

    @property
    def _state_path(self) -> Path:
        return self._state_dir / STATE_FILENAME

    @property
    def _cursor_path(self) -> Path:
        return self._state_dir / CURSOR_FILENAME

    @property
    def _journal_path(self) -> Path:
        return self._state_dir / JOURNAL_FILENAME

    def _legacy_path(self, key: str) -> Path:
        return self._dir / f"{key}{LEGACY_SUFFIX}"

    def _current_path(self, key: str) -> Path:
        return self._dir / f"{key}{self._current_suffix}"

    def _read_physical_locked(self, key: str) -> tuple[bytes | None, bytes | None]:
        """Return ``(legacy, current)`` and refresh one externally changed key."""

        legacy_path = self._legacy_path(key)
        current_path = self._current_path(key)
        disk_legacy = legacy_path.read_bytes() if legacy_path.exists() else None
        disk_current = current_path.read_bytes() if current_path.exists() else None

        if disk_legacy is not None and self._legacy.get(key) != disk_legacy:
            self._legacy[key] = disk_legacy
            self._current.pop(key, None)
            self._refresh_logical_snapshot_locked()
        elif disk_legacy is None and key in self._legacy:
            self._legacy.pop(key, None)
            current = self._current.pop(key, None)
            if current is not None:
                self._quarantine_current_locked(key, current, current_path)
            self._migration_state.pop(key, None)
            self._refresh_logical_snapshot_locked()

        if disk_current is not None and self._current.get(key) != disk_current:
            if disk_legacy is not None or self._migration_state.get(key):
                self._current[key] = disk_current
            else:
                # An uncommitted current file can be created by a crash before
                # the write journal is fsynced; keep it hidden until recovered.
                self._quarantine_current_locked(key, disk_current, current_path)
        elif disk_current is None and key in self._current:
            self._current.pop(key, None)
            self._refresh_logical_snapshot_locked()

        if disk_legacy is not None and self._legacy_is_newer_locked(key):
            return disk_legacy, disk_current
        if key in self._legacy:
            disk_legacy = self._legacy[key]
        if key in self._current:
            disk_current = self._current[key]
        return disk_legacy, disk_current

    def _load_current_file_locked(self, key: str, path: Path) -> bool:
        try:
            name = validate_key(key)
        except KeyError:
            return False
        raw = path.read_bytes()

        # A current file with neither a legacy projection nor a durable write
        # or migration marker is normally one left behind by an old binary's
        # rollback delete. Quarantine rather than make it visible again.
        if name not in self._legacy:
            self._quarantine_current_locked(name, raw, path)
            return True
        self._current[name] = raw
        return False

    def _scan_disk_locked(self) -> None:
        """Refresh the cache for files concurrently written by an old binary."""

        current_paths: dict[str, Path] = {}
        legacy_paths: dict[str, Path] = {}
        for path in sorted(self._dir.iterdir()):
            if not path.is_file() or path.name.endswith(TMP_SUFFIX):
                continue
            if path.name.endswith(self._current_suffix):
                key = path.name[: -len(self._current_suffix)]
                try:
                    current_paths[validate_key(key)] = path
                except KeyError:
                    continue
            elif path.name.endswith(LEGACY_SUFFIX):
                key = path.name[: -len(LEGACY_SUFFIX)]
                try:
                    legacy_paths[validate_key(key)] = path
                except KeyError:
                    continue

        removed_legacy = set(self._legacy) - set(legacy_paths)
        for key in removed_legacy:
            self._legacy.pop(key, None)
            current_path = self._current_path(key)
            current = self._current.pop(key, None)
            if current is not None:
                # An old binary removed its only visible file, so preserve that
                # deletion instead of resurrecting the key after rollback.
                self._quarantine_current_locked(key, current, current_path)
            self._migration_state.pop(key, None)

        for name, path in current_paths.items():
            if name not in legacy_paths:
                current = self._current.pop(name, None)
                if current is not None:
                    self._quarantine_current_locked(name, current, path)
                elif path.exists():
                    path.unlink()
                self._migration_state.pop(name, None)
            elif name not in self._current:
                self._current[name] = path.read_bytes()

        for name, path in legacy_paths.items():
            raw = path.read_bytes()
            if self._legacy.get(name) != raw:
                self._legacy[name] = raw
                self._current.pop(name, None)
                # The old binary is authoritative for this externally written
                # revision; the source hash mismatch makes the next migration
                # pass rebuild the current document.

        self._write_state_locked()
        self._refresh_logical_snapshot_locked()

    def _quarantine_current_locked(
        self, key: str, raw: bytes, path: Path
    ) -> None:
        quarantine_dir = self._state_dir / "quarantine"
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        token = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
        destination = quarantine_dir / f"{token}{self._current_suffix}"
        counter = 1
        while destination.exists():
            destination = quarantine_dir / f"{token}-{counter}{self._current_suffix}"
            counter += 1
        os.replace(path, destination)

    def _recover_journal_locked(self) -> None:
        path = self._journal_path
        if not path.exists():
            return
        try:
            journal = json.loads(path.read_bytes().decode("utf-8"))
            name = validate_key(str(journal["key"]))
            op = journal["op"]
        except (KeyError, TypeError, UnicodeDecodeError, ValueError) as exc:
            raise ValueError("invalid storage write journal") from exc

        if op == "delete":
            self._current_path(name).unlink(missing_ok=True)
            self._legacy_path(name).unlink(missing_ok=True)
            path.unlink(missing_ok=True)
            return
        if op != "put":
            raise ValueError(f"unknown storage journal operation: {op!r}")

        current_path = self._current_path(name)
        legacy_path = self._legacy_path(name)
        current_raw = current_path.read_bytes() if current_path.exists() else None
        legacy_raw = legacy_path.read_bytes() if legacy_path.exists() else None
        try:
            desired_current = base64.b64decode(journal["current_payload"])
            desired_legacy = base64.b64decode(journal["legacy_payload"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid storage write journal payload") from exc

        if (
            legacy_raw != desired_legacy
            or current_raw != desired_current
        ):
            # Journal the complete desired value, so replaying is idempotent
            # even if the crash occurred during either os.replace call.
            self._atomic_write_locked(current_path, desired_current)
            self._atomic_write_locked(legacy_path, desired_legacy)

        path.unlink(missing_ok=True)

    def _migrate_key_locked(self, name: str) -> str:
        legacy = self._legacy.get(name)
        current = self._current.get(name)
        state = self._migration_state.get(name)

        if legacy is None and current is None:
            return "missing"
        if legacy is None:
            return "current-only"

        legacy_hash = digest(legacy)
        if current is not None:
            current_hash = digest(current)
            if (
                state is not None
                and state.get("status") == _STATUS_MIGRATED
                and state.get("source_hash") == legacy_hash
                and state.get("current_hash") == current_hash
            ):
                return "migrated"

        if current is not None:
            current_is_stale = self._current_is_stale_locked(
                name, legacy, current
            )
            if not current_is_stale:
                if state is None:
                    # The current file was produced by another new-process write
                    # and its legacy projection is older. Do not turn back time.
                    return "superseded"
                try:
                    raise MigrationConflictError(
                        f"current document for {name!r} diverged during migration"
                    )
                except MigrationConflictError as exc:
                    self._mark_failed_locked(
                        name, legacy_hash, digest(current), str(exc)
                    )
                    raise

        try:
            new_current = encode(
                self._adapters.for_key(name).upgrade(decode(legacy))
            )
        except (TypeError, ValueError) as exc:
            current_hash = digest(current) if current is not None else ""
            self._mark_failed_locked(name, legacy_hash, current_hash, str(exc))
            raise
        self._atomic_write_locked(self._current_path(name), new_current)
        self._current[name] = new_current
        self._mark_migrated_locked(name, legacy_hash, new_current)
        self._refresh_logical_snapshot_locked()
        return "reconciled" if state is not None else "migrated"

    def _legacy_is_newer_locked(self, key: str) -> bool:
        current_path = self._current_path(key)
        legacy_path = self._legacy_path(key)
        if not current_path.exists() or not legacy_path.exists():
            return False
        return legacy_path.stat().st_mtime_ns > current_path.stat().st_mtime_ns

    def _current_is_stale_locked(
        self,
        key: str,
        legacy: bytes | None,
        current: bytes | None,
    ) -> bool:
        if legacy is None or current is None:
            return False
        state = self._migration_state.get(key)
        if state is not None:
            source_hash = state.get("source_hash")
            return source_hash is not None and source_hash != digest(legacy)
        return self._legacy_is_newer_locked(key)

    def _migration_counts_locked(self) -> tuple[int, int, int]:
        migrated = 0
        pending = 0
        failed = 0
        for key in set(self._legacy) | set(self._current):
            state = self._migration_state.get(key)
            legacy = self._legacy.get(key)
            current = self._current.get(key)
            if state is not None and state.get("status") == _STATUS_FAILED:
                failed += 1
                continue
            if state is not None and state.get("status") == _STATUS_MIGRATED:
                current_hash = digest(current) if current is not None else ""
                source_hash = digest(legacy) if legacy is not None else ""
                if (
                    current is not None
                    and state.get("current_hash") == current_hash
                    and (legacy is None or state.get("source_hash") == source_hash)
                ):
                    migrated += 1
                else:
                    pending += 1
            elif legacy is not None:
                pending += 1
            elif current is not None:
                migrated += 1
        return migrated, pending, failed

    def _mark_migrated_locked(
        self, key: str, source_hash: str, current: bytes
    ) -> None:
        self._migration_state[key] = {
            "status": _STATUS_MIGRATED,
            "source_hash": source_hash,
            "current_hash": digest(current),
        }
        self._write_state_locked()

    def _mark_failed_locked(
        self,
        key: str,
        source_hash: str,
        current_hash: str,
        error: str,
    ) -> None:
        self._migration_state[key] = {
            "status": _STATUS_FAILED,
            "source_hash": source_hash,
            "current_hash": current_hash,
            "error": error,
        }
        self._write_state_locked()

    def _load_state_locked(self) -> dict[str, dict[str, str]]:
        if not self._state_path.exists():
            return {}
        try:
            payload = json.loads(self._state_path.read_bytes().decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return {}
        if not isinstance(payload, dict):
            return {}
        state: dict[str, dict[str, str]] = {}
        for key, value in payload.items():
            if isinstance(key, str) and isinstance(value, dict):
                state[key] = {
                    str(item_key): str(item_value)
                    for item_key, item_value in value.items()
                }
        return state

    def _write_state_locked(self) -> None:
        payload = encode(self._migration_state)
        self._atomic_write_locked(self._state_path, payload)

    def _write_journal_locked(self, payload: dict[str, Any]) -> None:
        self._atomic_write_locked(self._journal_path, encode(payload))

    def _atomic_write_locked(self, path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + TMP_SUFFIX)
        tmp.write_bytes(payload)
        os.replace(tmp, path)

    def _refresh_logical_snapshot_locked(self) -> None:
        snapshot: dict[str, bytes] = {}
        for key in set(self._legacy) | set(self._current):
            current = self._current.get(key)
            legacy = self._legacy.get(key)
            if (
                current is not None
                and not self._current_is_stale_locked(key, legacy, current)
            ):
                snapshot[key] = current
            elif legacy is not None:
                snapshot[key] = legacy
            elif current is not None:
                snapshot[key] = current
        self._data = snapshot


def digest(payload: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).decode(
        "ascii"
    )
