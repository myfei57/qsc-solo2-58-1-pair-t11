"""Tests for online v1 -> v2 migration mechanics."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from portpower.store.kvstore import RecordStore, encode
from portpower.store.migration import MigrationRunner
from portpower.store.versioned import (
    ConversionError,
    Converter,
    ConverterRegistry,
    VersionedStore,
)


class FlattenConverter(Converter):
    """Example structural change:

    v1: {"ship": "s1", "max_load": 100.0}
    v2: {"ship": "s1", "power": {"max_load": 100.0}}

    Equivalence is limited to fields the old process reads: after a
    downgrade, the old document is byte-identical to its v1 ancestor.
    """

    def upgrade(self, key: str, legacy: object) -> object:
        doc = dict(legacy)
        doc["power"] = {"max_load": doc.pop("max_load")}
        return doc

    def downgrade(self, key: str, document: object) -> object:
        doc = dict(document)
        nested = doc.pop("power")
        doc["max_load"] = nested["max_load"]
        return doc


def make_store(root: Path, *, dual_write: bool = True, lazy_upgrade: bool = True) -> VersionedStore:
    registry = ConverterRegistry()
    registry.register_prefix("ship.profile.", FlattenConverter())
    store = VersionedStore(
        root,
        registry,
        dual_write=dual_write,
        lazy_upgrade=lazy_upgrade,
    )
    store.load()
    return store


class VersionedStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # -- coexistence -----------------------------------------------------

    def test_old_process_does_not_see_v2_directory(self) -> None:
        new = make_store(self.root)
        new.put_json("ship.profile.s1", {"ship": "s1", "power": {"max_load": 100.0}})

        old = RecordStore(self.root)
        old.load()
        # Non-recursive glob: v2/ contents are invisible, prefix scans clean.
        self.assertEqual(set(self._transaction_keys(old)), {"ship.profile.s1"})
        # ... and the legacy file is exactly the old structure.
        self.assertEqual(
            old.get_json("ship.profile.s1"),
            {"ship": "s1", "max_load": 100.0},
        )

    @staticmethod
    def _transaction_keys(store: RecordStore) -> list[str]:
        with store.transaction() as data:
            return list(data)

    def test_new_process_reads_new_and_legacy(self) -> None:
        # Existing document written entirely by an old release.
        RecordStore(self.root).put_json("ship.profile.s2",
                                       {"ship": "s2", "max_load": 5.0})

        new = make_store(self.root)
        self.assertEqual(
            new.get_json("ship.profile.s2"),
            {"ship": "s2", "power": {"max_load": 5.0}},
        )
        counters = new.metrics.snapshot()["counters"]
        self.assertEqual(counters["store.reads_v1"], 1)
        self.assertNotIn("store.reads_v2", counters)

        self.assertEqual(
            new.get_json("ship.profile.s1") if new.has("ship.profile.s1") else None,
            None,
        )

    def test_reads_v2_after_migration_and_compat_ratio(self) -> None:
        RecordStore(self.root).put_json("ship.profile.s2",
                                       {"ship": "s2", "max_load": 5.0})
        new = make_store(self.root)
        new.get_json("ship.profile.s2")  # lazy upgrade
        new.get_json("ship.profile.s2")  # now canonical
        snap = new.metrics.snapshot()
        self.assertAlmostEqual(snap["counters"]["store.compat_read_ratio"], 0.5)

    # -- write unification + rollback equivalence -----------------------

    def test_new_write_dual_projects_for_rolled_back_old_process(self) -> None:
        new = make_store(self.root)
        new.put_json("ship.profile.s1", {"ship": "s1", "power": {"max_load": 100.0}})
        # Simulate rollback: old process reads its projection and mutates.
        old = RecordStore(self.root)
        old.load()
        legacy = old.get_json("ship.profile.s1")
        legacy["max_load"] = 250.0
        old.put_json("ship.profile.s1", legacy)

        new2 = make_store(self.root)
        # Drift is detected by digest and healed by re-upgrading the legacy
        # bytes -- the old process's update is not lost.
        self.assertEqual(
            new2.get_json("ship.profile.s1"),
            {"ship": "s1", "power": {"max_load": 250.0}},
        )
        self.assertEqual(new2.inspect("ship.profile.s1"), "v2")
        self.assertGreaterEqual(
            new2.metrics.snapshot()["counters"]["store.reads_legacy_drift_healed"], 1
        )

    def test_downgrade_failure_blocks_write_and_is_recoverable(self) -> None:
        class Broken(Converter):
            def upgrade(self, key, legacy):
                return legacy

            def downgrade(self, key, document):
                raise RuntimeError("new field cannot project to old schema")

        registry = ConverterRegistry()
        registry.register_prefix("danger.", Broken())
        store = VersionedStore(self.root, registry)
        with self.assertRaises(ConversionError):
            store.put_json("danger.x", {"only_in_v2": True})
        self.assertFalse(store.has("danger.x"))

    def test_cutover_mode_stops_projections(self) -> None:
        store = make_store(self.root)
        store.put_json("ship.profile.s1", {"ship": "s1", "power": {"max_load": 1.0}})
        store2 = make_store(self.root, dual_write=False)
        store2.put_json("ship.profile.s1", {"ship": "s1", "power": {"max_load": 2.0}})
        self.assertFalse((self.root / "ship.profile.s1.json").exists())

    def test_delete_removes_both_layouts(self) -> None:
        store = make_store(self.root)
        store.put_json("ship.profile.s1", {"ship": "s1", "power": {"max_load": 1.0}})
        store.delete("ship.profile.s1")
        self.assertFalse(store.has("ship.profile.s1"))
        self.assertFalse((self.root / "ship.profile.s1.json").exists())
        self.assertFalse((self.root / "v2" / "ship.profile.s1.json").exists())

    def test_bulk_transaction_view_is_consistent_with_point_reads(self) -> None:
        RecordStore(self.root).put_json("ship.profile.s9",
                                       {"ship": "s9", "max_load": 9.0})
        store = make_store(self.root)
        with store.transaction() as view:
            doc = json.loads(view["ship.profile.s9"].decode())
        self.assertEqual(doc, {"ship": "s9", "power": {"max_load": 9.0}})
        # Bulk scans must not count as compatibility reads.
        self.assertNotIn(
            "store.reads_v1", store.metrics.snapshot()["counters"]
        )

    def test_update_json_upgrades_legacy_then_writes_canonical(self) -> None:
        RecordStore(self.root).put_json("ship.profile.s2",
                                       {"ship": "s2", "max_load": 5.0})
        store = make_store(self.root)

        def bump(doc):
            doc["power"]["max_load"] += 1
            return doc, "ok"

        self.assertEqual(store.update_json("ship.profile.s2", bump), "ok")
        old = RecordStore(self.root)
        old.load()
        self.assertEqual(old.get_json("ship.profile.s2")["max_load"], 6.0)

    # -- crash recovery --------------------------------------------------

    def _write_pending(self, key: str, data: object, projection: bytes | None) -> None:
        v2_dir = self.root / "v2"
        v2_dir.mkdir(parents=True, exist_ok=True)
        envelope = {
            "_v": 2,
            "_rev": 1,
            "_updated": "2026-01-01T00:00:00+00:00",
            "_pending": True,
            "_src_sha256": __import__("hashlib").sha256(
                projection or b""
            ).hexdigest(),
            "data": data,
        }
        (v2_dir / f"{key}.json").write_bytes(encode(envelope))
        if projection is not None:
            (self.root / f"{key}.json").write_bytes(projection)

    def test_crash_between_projection_and_flip_recovers(self) -> None:
        projection = encode({"ship": "s1", "max_load": 100.0})
        self._write_pending(
            "ship.profile.s1",
            {"ship": "s1", "power": {"max_load": 100.0}},
            projection,
        )
        store = make_store(self.root)
        doc = store.get_json("ship.profile.s1")
        self.assertEqual(doc["power"]["max_load"], 100.0)
        self.assertEqual(store.inspect("ship.profile.s1"), "v2")
        # legacy untouched, pending flag flipped
        self.assertEqual((self.root / "ship.profile.s1.json").read_bytes(), projection)

    def test_crash_between_pending_and_projection_replays(self) -> None:
        self._write_pending(
            "ship.profile.s1",
            {"ship": "s1", "power": {"max_load": 100.0}},
            None,
        )
        store = make_store(self.root)
        store.get_json("ship.profile.s1")
        self.assertEqual(store.inspect("ship.profile.s1"), "v2")
        self.assertEqual(
            json.loads((self.root / "ship.profile.s1.json").read_text()),
            {"ship": "s1", "max_load": 100.0},
        )


class MigrationRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def seed_legacy(self, count: int) -> None:
        old = RecordStore(self.root)
        for i in range(count):
            old.put_json(f"ship.profile.s{i:03d}",
                         {"ship": f"s{i:03d}", "max_load": float(i)})

    def test_full_migration_progress_and_status(self) -> None:
        self.seed_legacy(25)
        store = make_store(self.root)
        runner = MigrationRunner(store, batch_size=10, poll_interval=0)
        status = runner.run()
        self.assertEqual(status["migrated_or_healthy"], 25)
        self.assertEqual(status["progress"], 1.0)
        self.assertIsNotNone(status["completed_at"])
        self.assertEqual(status["failures"], {})
        # legacy projections are retained during the compatibility window
        self.assertEqual(status["legacy_files"], 25)

    def test_restart_is_resumable_and_does_not_reconvert(self) -> None:
        self.seed_legacy(15)
        store = make_store(self.root)
        runner = MigrationRunner(store, batch_size=5, poll_interval=0)
        state = runner._load_state()
        runner.run_batch(state)  # 5 migrated, checkpoint at s004
        self.assertEqual(state["cursor"], "ship.profile.s004")

        # New process resumes from the checkpoint.
        store2 = make_store(self.root)
        runner2 = MigrationRunner(store2, batch_size=5, poll_interval=0)
        status = runner2.run()
        self.assertEqual(status["migrated_or_healthy"], 15)

        # A third run converts nothing: digest-guarded skip, not re-upgrade.
        counters_before = store2.metrics.snapshot()["counters"].get("migration.migrated", 0)
        runner3 = MigrationRunner(make_store(self.root), batch_size=5, poll_interval=0)
        runner3.run()
        migrated_total = runner3._store.metrics.snapshot()["counters"].get(
            "migration.migrated", 0
        )
        self.assertEqual(migrated_total, 0)
        self.assertGreaterEqual(counters_before, 0)

    def test_rollback_write_after_migration_is_re_migrated(self) -> None:
        self.seed_legacy(3)
        store = make_store(self.root)
        MigrationRunner(store, batch_size=10, poll_interval=0).run()

        # Rolled-back process changes one legacy file after migration.
        old = RecordStore(self.root)
        old.load()
        changed = old.get_json("ship.profile.s001")
        changed["max_load"] = 999.0
        old.put_json("ship.profile.s001", changed)

        # Verification sweep catches the digest mismatch and re-upgrades.
        repaired = MigrationRunner(make_store(self.root)).run_verification()
        self.assertEqual(repaired["healed"], 1)
        healed = make_store(self.root).get_json("ship.profile.s001")
        self.assertEqual(healed["power"]["max_load"], 999.0)

    def test_failing_key_is_recorded_not_fatal(self) -> None:
        self.seed_legacy(2)

        class Exploding(FlattenConverter):
            def upgrade(self, key, legacy):
                if key.endswith("s001"):
                    raise RuntimeError("bad document")
                return super().upgrade(key, legacy)

        registry = ConverterRegistry()
        registry.register_prefix("ship.profile.", Exploding())
        store = VersionedStore(self.root, registry)
        store.load()
        runner = MigrationRunner(store, batch_size=10, poll_interval=0)
        status = runner.run()
        self.assertIn("ship.profile.s001", status["failures"])
        self.assertEqual(status["migrated_or_healthy"], 1)
        self.assertTrue(status["poison_keys"])  # retried up to threshold


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
