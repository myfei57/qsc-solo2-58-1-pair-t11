"""Two-layout record store that supports online schema migration.

During a migration window a logical key can exist in two places:

* ``<dir>/<key>.json``        -- legacy v1 bytes, identical to every older
  release.  An old process opens the directory with a plain
  :class:`RecordStore` and never sees the ``v2/`` subdirectory (its
  ``glob("*.json")`` is non-recursive), so prefix scans are not polluted.
* ``<dir>/v2/<key>.json``     -- canonical v2 envelope::

      {"_v": 2, "_rev": 3, "_updated": "<iso8601>", "_pending": false,
       "_src_sha256": "<sha of the legacy bytes this canonical view syncs>",
       "data": <new-structure document>}

Writes always target the canonical envelope; the legacy file is a downgrade
projection maintained for rolled-back old processes.  Reads of legacy-only
documents run the registered converter's ``upgrade`` on the fly.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from portpower.store.kvstore import RecordStore, decode, encode

V2_DIRNAME = "v2"
ENVELOPE_VERSION = 2
MIGRATION_KEY = "_migration"

ReadOutcome = Literal["v2", "v1", "recovered", "healed"]
MigrateOutcome = Literal["migrated", "skipped", "healed", "recovered"]


class ConversionError(ValueError):
    """Raised when a document cannot be converted in either direction."""


class Converter:
    """Bidirectional converter between legacy v1 and canonical v2 documents.

    ``downgrade`` is mandatory, not optional: as long as the compatibility
    layer is on, every new write must produce a legacy projection an old
    process can read back to an equivalent conclusion.  "Equivalent" is
    defined per document type on the fields the old code reads; byte
    equality is not required.
    """

    def upgrade(self, key: str, legacy: Any) -> Any:
        raise NotImplementedError

    def downgrade(self, key: str, document: Any) -> Any:
        raise NotImplementedError


class IdentityConverter(Converter):
    """Pass-through converter for keys whose structure did not change."""

    def upgrade(self, key: str, legacy: Any) -> Any:
        return legacy

    def downgrade(self, key: str, document: Any) -> Any:
        return document


IDENTITY = IdentityConverter()


class ConverterRegistry:
    """Matches keys (by prefix) to the converter that owns them."""

    def __init__(self) -> None:
        self._rules: list[tuple[str, Converter]] = []

    def register_prefix(self, prefix: str, converter: Converter) -> None:
        self._rules.append((prefix, converter))

    def for_key(self, key: str) -> Converter:
        for prefix, converter in self._rules:
            if key.startswith(prefix):
                return converter
        return IDENTITY


class Metrics:
    """Thread-safe counters/gauges for migration observability."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}

    def incr(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + amount

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
        v1 = counters.get("store.reads_v1", 0)
        v2 = counters.get("store.reads_v2", 0)
        total = v1 + v2
        counters["store.compat_read_ratio"] = (v1 / total) if total else 0.0
        return {"counters": counters, "gauges": gauges}


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class VersionedStore:
    """Drop-in key/value store presenting the new structure to callers.

    The method surface mirrors :class:`RecordStore` (``get``/``put``/
    ``get_json``/``put_json``/``update_json``/``has``/``delete``); bytes and
    decoded values always use the *new* structure -- legacy conversion is
    invisible to callers.
    """

    def __init__(
        self,
        directory: str | Path,
        converters: ConverterRegistry | None = None,
        *,
        dual_write: bool = True,
        lazy_upgrade: bool = True,
        metrics: Metrics | None = None,
    ) -> None:
        self._root = Path(directory)
        self._v2_dir = self._root / V2_DIRNAME
        self._v2_dir.mkdir(parents=True, exist_ok=True)
        self._legacy = RecordStore(self._root)
        self._canonical = RecordStore(self._v2_dir)
        self._converters = converters or ConverterRegistry()
        self._dual_write = dual_write
        self._lazy_upgrade = lazy_upgrade
        self.metrics = metrics or Metrics()
        self._lock = threading.RLock()

    # -- lifecycle -------------------------------------------------------

    def load(self) -> None:
        with self._lock:
            self._legacy.load()
            self._canonical.load()

    # -- read path -------------------------------------------------------

    def get(self, key: str) -> bytes | None:
        with self._lock:
            document = self.get_json(key)
            return None if document is None else encode(document)

    def get_json(self, key: str) -> Any | None:
        with self._lock:
            legacy_raw = self._legacy.get(key)
            envelope_raw = self._canonical.get(key)

            if envelope_raw is not None:
                envelope = decode(envelope_raw)
                if envelope.get("_pending"):
                    envelope = self._resolve_pending(key, envelope, legacy_raw)
                elif (
                    legacy_raw is not None
                    and envelope.get("_src_sha256") != _digest(legacy_raw)
                ):
                    envelope = self._heal_from_legacy(key, legacy_raw, envelope)
                else:
                    self.metrics.incr("store.reads_v2")
                return envelope["data"]

            if legacy_raw is not None:
                self.metrics.incr("store.reads_v1")
                data = self._converters.for_key(key).upgrade(key, decode(legacy_raw))
                if self._lazy_upgrade:
                    self._install_migrated(key, data, legacy_raw)
                return data
            return None

    def has(self, key: str) -> bool:
        with self._lock:
            return self._canonical.has(key) or self._legacy.has(key)

    # -- write path ------------------------------------------------------

    def put(self, key: str, payload: bytes) -> None:
        self.put_json(key, decode(payload))

    def put_json(self, key: str, data: Any) -> None:
        with self._lock:
            converter = self._converters.for_key(key)
            previous = self._read_envelope(key)
            revision = int(previous.get("_rev", 0)) + 1 if previous else 1

            if self._dual_write:
                try:
                    projection = encode(converter.downgrade(key, data))
                except Exception as exc:  # noqa: BLE001 - surfaced to caller
                    raise ConversionError(
                        f"cannot write {key!r}: downgrade projection failed"
                    ) from exc

                # Pending first: a crash before the flip is unambiguously
                # recoverable (see _resolve_pending).
                self._write_envelope(
                    key,
                    data,
                    revision=revision,
                    pending=True,
                    source_digest=_digest(projection),
                )
                self._legacy.put(key, projection)
                self._write_envelope(
                    key,
                    data,
                    revision=revision,
                    pending=False,
                    source_digest=_digest(projection),
                )
            else:
                # Compatibility layer retired: do not leave stale projections
                # around for a future rollback to misread.
                self._legacy.delete(key)
                self._write_envelope(
                    key, data, revision=revision, pending=False, source_digest=None
                )
            self.metrics.incr("store.writes")
            if not self._dual_write:
                self.metrics.incr("store.writes_without_legacy")

    def delete(self, key: str) -> None:
        with self._lock:
            self._canonical.delete(key)
            self._legacy.delete(key)
            self.metrics.incr("store.deletes")

    def update_json(
        self, key: str, mutator: Callable[[Any], tuple[Any, Any]]
    ) -> Any:
        with self._lock:
            current = self.get_json(key)
            new_document, result = mutator(current)
            if new_document is not None:
                self.put_json(key, new_document)
            return result

    # -- migration support ----------------------------------------------

    def legacy_keys(self) -> list[str]:
        with self._lock:
            with self._legacy.transaction() as data:
                return sorted(data)

    def canonical_keys(self) -> list[str]:
        with self._lock:
            with self._canonical.transaction() as data:
                return sorted(key for key in data if not self._is_meta(key))

    def keys(self) -> list[str]:
        with self._lock:
            return sorted(set(self.legacy_keys()) | set(self.canonical_keys()))

    def inspect(self, key: str) -> Literal["v2", "v1", "missing", "pending", "drift"]:
        """On-disk state of one key without performing any repair."""

        with self._lock:
            envelope_raw = self._canonical.get(key)
            legacy_raw = self._legacy.get(key)
            if envelope_raw is None:
                return "v1" if legacy_raw is not None else "missing"
            envelope = decode(envelope_raw)
            if envelope.get("_pending"):
                return "pending"
            if legacy_raw is not None and envelope.get(
                "_src_sha256"
            ) != _digest(legacy_raw):
                return "drift"
            return "v2"

    def migrate_if_needed(self, key: str) -> MigrateOutcome:
        """Bring one key to a healthy canonical state.

        Idempotent: a key whose legacy bytes digest to the recorded
        ``_src_sha256`` is left untouched, so re-running a migration never
        converts an already-migrated document twice.
        """

        with self._lock:
            envelope_raw = self._canonical.get(key)
            legacy_raw = self._legacy.get(key)

            if envelope_raw is not None:
                envelope = decode(envelope_raw)
                if envelope.get("_pending"):
                    self._resolve_pending(key, envelope, legacy_raw)
                    return "recovered"
                if legacy_raw is not None and envelope.get(
                    "_src_sha256"
                ) != _digest(legacy_raw):
                    self._heal_from_legacy(key, legacy_raw, envelope)
                    return "healed"
                return "skipped"

            if legacy_raw is None:
                return "skipped"

            data = self._converters.for_key(key).upgrade(key, decode(legacy_raw))
            self._install_migrated(key, data, legacy_raw)
            return "migrated"

    @contextmanager
    def transaction(self) -> Iterator[dict[str, bytes]]:
        """Merged logical view (new-structure bytes) under one read lock.

        Legacy-only documents are upgraded for the view but are *not* marked
        as served reads and are not lazily rewritten: bulk scans/exports must
        not inflate the compatibility-read ratio.  Pending/drift envelopes
        are resolved so bulk views match point reads.
        """

        with self._lock:
            view: dict[str, bytes] = {}
            with self._legacy.transaction() as legacy:
                for key, raw in legacy.items():
                    view[key] = raw
            with self._canonical.transaction() as canonical:
                for key, raw in canonical.items():
                    if self._is_meta(key):
                        continue
                    envelope = decode(raw)
                    if envelope.get("_pending"):
                        envelope = self._resolve_pending(key, envelope, view.get(key))
                    elif (
                        key in view
                        and envelope.get("_src_sha256") != _digest(view[key])
                    ):
                        envelope = self._heal_from_legacy(key, view[key], envelope)
                    view[key] = encode(envelope["data"])
            # Upgrade legacy-only entries for the exported view.
            for key, raw in list(view.items()):
                if self._canonical.get(key) is None:
                    view[key] = encode(
                        self._converters.for_key(key).upgrade(key, decode(raw))
                    )
            yield view

    # -- internals -------------------------------------------------------

    @staticmethod
    def _is_meta(key: str) -> bool:
        return key == MIGRATION_KEY or key.startswith("__")

    def _read_envelope(self, key: str) -> dict[str, Any] | None:
        raw = self._canonical.get(key)
        return None if raw is None else decode(raw)

    def _write_envelope(
        self,
        key: str,
        data: Any,
        *,
        revision: int,
        pending: bool,
        source_digest: str | None,
    ) -> None:
        envelope = {
            "_v": ENVELOPE_VERSION,
            "_rev": revision,
            "_updated": _utc_now(),
            "_pending": pending,
            "_src_sha256": source_digest,
            "data": data,
        }
        self._canonical.put(key, encode(envelope))

    def _install_migrated(self, key: str, data: Any, legacy_raw: bytes) -> None:
        """Record a background/lazy upgrade without touching the legacy file."""

        self._write_envelope(
            key,
            data,
            revision=1,
            pending=False,
            source_digest=_digest(legacy_raw),
        )
        self.metrics.incr("store.lazy_upgrades")

    def _resolve_pending(
        self,
        key: str,
        envelope: dict[str, Any],
        legacy_raw: bytes | None,
    ) -> dict[str, Any]:
        """Complete or roll forward a dual-write interrupted by a crash."""

        expected = envelope.get("_src_sha256")
        if legacy_raw is not None and _digest(legacy_raw) == expected:
            # Projection landed; only the final flip was lost.
            repaired = dict(envelope)
            repaired["_pending"] = False
            self._canonical.put(key, encode(repaired))
        else:
            # Projection missing or stale: the canonical view is authoritative.
            converter = self._converters.for_key(key)
            projection = encode(converter.downgrade(key, envelope["data"]))
            self._legacy.put(key, projection)
            repaired = dict(envelope)
            repaired["_pending"] = False
            repaired["_src_sha256"] = _digest(projection)
            self._canonical.put(key, encode(repaired))
        self.metrics.incr("store.reads_pending_recovered")
        return repaired

    def _heal_from_legacy(
        self,
        key: str,
        legacy_raw: bytes,
        envelope: dict[str, Any],
    ) -> dict[str, Any]:
        """Re-upgrade a legacy file overwritten by a rolled-back old process."""

        data = self._converters.for_key(key).upgrade(key, decode(legacy_raw))
        repaired = {
            "_v": ENVELOPE_VERSION,
            "_rev": int(envelope.get("_rev", 0)) + 1,
            "_updated": _utc_now(),
            "_pending": False,
            "_src_sha256": _digest(legacy_raw),
            "data": data,
        }
        self._canonical.put(key, encode(repaired))
        self.metrics.incr("store.reads_legacy_drift_healed")
        return repaired
