"""Resumable background migration from legacy files to canonical envelopes.

The runner is a restartable cursor scan over the sorted legacy keyspace.
Progress is checkpointed to ``v2/_migration.json``; per-key work is
digest-guarded, so restarting mid-run never re-converts a document whose
legacy bytes are unchanged (an old process writing the same key after a
rollback *will* be noticed and re-upgraded).
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any

from portpower.store.kvstore import encode
from portpower.store.versioned import MIGRATION_KEY, Metrics, VersionedStore

_CHECKPOINT_VERSION = 1
_POISON_THRESHOLD = 5


class MigrationRunner:
    def __init__(
        self,
        store: VersionedStore,
        *,
        batch_size: int = 100,
        poll_interval: float = 0.1,
        metrics: Metrics | None = None,
    ) -> None:
        self._store = store
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self.metrics = metrics or store.metrics
        self._stop = threading.Event()

    def request_stop(self) -> None:
        """Ask the running pass to stop at the next batch boundary."""

        self._stop.set()

    # -- checkpoint storage ---------------------------------------------

    def _state_raw(self) -> bytes | None:
        # Written through the underlying v2 RecordStore key so the envelope
        # format never collides with application data.
        return self._store._canonical.get(MIGRATION_KEY)  # noqa: SLF001

    def _load_state(self) -> dict[str, Any]:
        raw = self._state_raw()
        if raw is None:
            return {
                "_v": _CHECKPOINT_VERSION,
                "started_at": None,
                "updated_at": None,
                "completed_at": None,
                "cursor": None,
                "processed_keys": 0,
                "failures": {},
                "attempts": {},
            }
        state = json.loads(raw.decode("utf-8"))
        state.setdefault("failures", {})
        state.setdefault("attempts", {})
        return state

    def _save_state(self, state: dict[str, Any]) -> None:
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._store._canonical.put(MIGRATION_KEY, encode(state))  # noqa: SLF001

    # -- execution -------------------------------------------------------

    def run_batch(self, state: dict[str, Any]) -> dict[str, Any]:
        """Migrate the next ``batch_size`` keys past the checkpoint cursor.

        Already-healthy keys count as ``skipped``; that is the normal case
        on a restart, which is why completion cannot be inferred from batch
        results alone.
        """

        cursor = state.get("cursor")
        keys = self._store.legacy_keys()
        pending = [key for key in keys if cursor is None or key > cursor]

        batch = pending[: self._batch_size]
        results = {"migrated": 0, "skipped": 0, "healed": 0, "recovered": 0,
                   "failed": 0}
        for key in batch:
            attempts = int(state["attempts"].get(key, 0))
            try:
                outcome = self._store.migrate_if_needed(key)
                results[outcome] += 1
                self.metrics.incr(f"migration.{outcome}")
                state["failures"].pop(key, None)
                state["attempts"].pop(key, None)
            except Exception as exc:  # noqa: BLE001 - recorded, not fatal
                attempts += 1
                state["attempts"][key] = attempts
                state["failures"][key] = f"{type(exc).__name__}: {exc}"
                results["failed"] += 1
                self.metrics.incr("migration.failed")
            finally:
                state["cursor"] = key
                state["processed_keys"] = int(state.get("processed_keys", 0)) + 1

        self._save_state(state)
        for name, value in results.items():
            if value:
                self.metrics.gauge(f"migration.batch.{name}", value)
        return results

    def _retries_pending(self, state: dict[str, Any]) -> bool:
        return any(
            int(state["attempts"].get(key, 0)) < _POISON_THRESHOLD
            for key in state["failures"]
        )

    def _record_poison(self, state: dict[str, Any]) -> list[str]:
        poison = [
            key for key in list(state["failures"])
            if int(state["attempts"].get(key, 0)) >= _POISON_THRESHOLD
        ]
        if poison:
            state["poison_keys"] = sorted(set(state.get("poison_keys", [])) | set(poison))
            self._save_state(state)
        return poison

    def run(self) -> dict[str, Any]:
        """Run one full resumable pass until caught up or stopped.

        Phase 1 is the checkpointed cursor scan (restart resumes at the
        cursor, skipping already-healthy keys).  Phase 2 is a full digest
        verification sweep that repairs anything an old process changed
        while the scan was running.
        """

        state = self._load_state()
        if state.get("started_at") is None:
            state["started_at"] = datetime.now(timezone.utc).isoformat()
            self._save_state(state)

        # Phase 1: advance until every sorted legacy key passed the cursor.
        while not self._stop.is_set():
            cursor = state.get("cursor")
            keys = self._store.legacy_keys()
            if not any(cursor is None or key > cursor for key in keys):
                break
            self.run_batch(state)
            if self._stop.is_set():
                return self.status()
            self._stop.wait(self._poll_interval)

        # Retry keys that failed during the scan, bounded by the threshold.
        while not self._stop.is_set() and self._retries_pending(state):
            for key in list(state["failures"]):
                if int(state["attempts"].get(key, 0)) >= _POISON_THRESHOLD:
                    continue
                try:
                    outcome = self._store.migrate_if_needed(key)
                    self.metrics.incr(f"migration.{outcome}")
                    state["failures"].pop(key, None)
                    state["attempts"].pop(key, None)
                except Exception as exc:  # noqa: BLE001
                    state["attempts"][key] = int(state["attempts"].get(key, 0)) + 1
                    state["failures"][key] = f"{type(exc).__name__}: {exc}"
                    self.metrics.incr("migration.failed")
            self._save_state(state)

        self._record_poison(state)
        if self._stop.is_set():
            return self.status()

        # Phase 2: full verification sweep (cheap: digest compare per key).
        if not state["failures"]:
            self.run_verification()
        return self.status()

    def run_verification(self) -> dict[str, Any]:
        """Final digest sweep: repair anything left inconsistent."""

        repaired = {"migrated": 0, "healed": 0, "recovered": 0, "failed": 0}
        state = self._load_state()
        for key in self._store.legacy_keys():
            try:
                outcome = self._store.migrate_if_needed(key)
                if outcome in repaired:
                    repaired[outcome] += 1
            except Exception as exc:  # noqa: BLE001
                repaired["failed"] += 1
                state["failures"][key] = f"{type(exc).__name__}: {exc}"
        if repaired["failed"] == 0 and not state["failures"]:
            state["completed_at"] = datetime.now(timezone.utc).isoformat()
        state["cursor"] = None  # next rollout (e.g. legacy cleanup) rescans
        self._save_state(state)
        self.metrics.gauge("migration.verification_failures", repaired["failed"])
        return repaired

    # -- status ----------------------------------------------------------

    def status(self) -> dict[str, Any]:
        state = self._load_state()
        legacy = set(self._store.legacy_keys())
        canonical = set(self._store.canonical_keys())
        migrated_or_healthy = 0
        for key in legacy:
            if self._store.inspect(key) == "v2":
                migrated_or_healthy += 1
        total_logical = len(legacy | canonical)
        progress = (migrated_or_healthy / total_logical) if total_logical else 1.0
        self.metrics.gauge("migration.progress", progress)
        self.metrics.gauge("migration.failure_count", len(state["failures"]))
        return {
            "cursor": state.get("cursor"),
            "started_at": state.get("started_at"),
            "updated_at": state.get("updated_at"),
            "completed_at": state.get("completed_at"),
            "processed_keys": state.get("processed_keys", 0),
            "legacy_files": len(legacy),
            "canonical_files": len(canonical),
            "migrated_or_healthy": migrated_or_healthy,
            "total_keys": total_logical,
            "progress": progress,
            "failures": dict(state["failures"]),
            "poison_keys": state.get("poison_keys", []),
        }
