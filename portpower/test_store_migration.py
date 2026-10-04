from __future__ import annotations

import base64
import json
import tempfile
import unittest
from pathlib import Path

from portpower.store.adapter import AdapterRegistry, DocumentAdapter
from portpower.store.kvstore import RecordStore, encode
from portpower.store.migration import VersionedRecordStore, digest


def upgrade(document):
    return {"schema": 2, "value": document["v"]}


def downgrade(document):
    return {"v": document.get("value", document.get("v"))}


class OnlineMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.directory = Path(self._temp.name)
        adapters = AdapterRegistry()
        adapters.register("ship.", DocumentAdapter(upgrade, downgrade))
        self.adapters = adapters

    def store(self) -> VersionedRecordStore:
        store = VersionedRecordStore(self.directory, adapters=self.adapters)
        store.load()
        return store

    def old_store(self) -> RecordStore:
        store = RecordStore(self.directory)
        store.load()
        return store

    def test_reads_legacy_with_conversion_and_observes_ratio(self) -> None:
        old = self.old_store()
        old.put_json("ship.a", {"v": 1})

        store = self.store()
        self.assertEqual(store.get_json("ship.a"), {"schema": 2, "value": 1})
        self.assertEqual(store.migration_status().compatible_read_ratio, 1.0)

    def test_writes_new_canonical_and_legacy_projection(self) -> None:
        store = self.store()
        store.put_json("ship.a", {"schema": 2, "value": 7})

        self.assertEqual(json.loads((self.directory / "ship.a.v2").read_text()),
                         {"schema": 2, "value": 7})
        self.assertEqual(json.loads((self.directory / "ship.a.json").read_text()),
                         {"v": 7})

        old = self.old_store()
        self.assertEqual(old.get_json("ship.a"), {"v": 7})

    def test_migration_is_idempotent(self) -> None:
        old = self.old_store()
        old.put_json("ship.a", {"v": 1})

        first = self.store().migrate()
        second = self.store().migrate()
        self.assertEqual(first.migrated_keys, 1)
        self.assertEqual(second.migrated_keys, 1)
        self.assertEqual(second.pending_keys, 0)
        self.assertEqual(json.loads((self.directory / "ship.a.json").read_text()),
                         {"v": 1})
        self.assertEqual(json.loads((self.directory / "ship.a.v2").read_text()),
                         {"schema": 2, "value": 1})

    def test_changed_legacy_document_is_re_migrated(self) -> None:
        old = self.old_store()
        old.put_json("ship.a", {"v": 1})
        store = self.store()
        store.migrate()

        old = self.old_store()
        old.put_json("ship.a", {"v": 2})
        status = store.sync_disk()
        self.assertEqual(status.pending_keys, 1)
        status = store.migrate()
        self.assertEqual(status.pending_keys, 0)
        self.assertEqual(store.get_json("ship.a"), {"schema": 2, "value": 2})

    def test_batch_migration_checkpoint_resumes_and_clears_at_end(self) -> None:
        old = self.old_store()
        for index in range(3):
            old.put_json(f"ship.{index}", {"v": index})

        store = self.store()
        first = store.migrate(limit=1)
        self.assertTrue(first.has_more)
        self.assertEqual(first.cursor, "ship.0")

        reopened = self.store()
        second = reopened.migrate(limit=10)
        self.assertFalse(second.has_more)
        self.assertIsNone(second.cursor)
        self.assertEqual(second.pending_keys, 0)

    def test_failed_migration_remains_recoverable(self) -> None:
        old = self.old_store()
        old.put_json("bad", {"unexpected": True})

        def failing_upgrade(document):
            raise ValueError("bad shape")

        adapters = AdapterRegistry(
            DocumentAdapter(failing_upgrade, downgrade)
        )
        store = VersionedRecordStore(self.directory, adapters=adapters)
        store.load()
        with self.assertRaises(ValueError):
            store.migrate_key("bad")
        self.assertEqual(store.migration_status().failed_keys, 1)

        adapters = AdapterRegistry()
        recovered = VersionedRecordStore(self.directory, adapters=adapters)
        recovered.load()
        recovered.migrate()
        self.assertEqual(recovered.migration_status().failed_keys, 0)

    def test_journal_completes_an_interrupted_write(self) -> None:
        old = self.old_store()
        old.put_json("a", {"v": 1})
        current = encode({"v": 2})
        legacy = encode({"v": 2})
        journal = {
            "op": "put",
            "key": "a",
            "current_payload": base64.b64encode(current).decode("ascii"),
            "legacy_payload": base64.b64encode(legacy).decode("ascii"),
            "current_hash": digest(current),
            "legacy_hash": digest(legacy),
        }
        state_dir = self.directory / ".migration"
        state_dir.mkdir(exist_ok=True)
        (state_dir / "write-journal.json").write_bytes(encode(journal))
        (self.directory / "a.v2").unlink(missing_ok=True)

        store = self.store()
        self.assertEqual(store.get_json("a"), {"v": 2})
        self.assertTrue((self.directory / "a.v2").exists())

    def test_rollback_delete_does_not_resurrect_current_file(self) -> None:
        store = self.store()
        store.put_json("a", {"v": 1})
        (self.directory / "a.json").unlink()
        (self.directory / "a.v2").unlink()
        store.put_json("b", {"v": 2})
        (self.directory / "b.json").unlink()

        reopened = self.store()
        self.assertFalse(reopened.has("b"))
        self.assertEqual(list(self.directory.glob("*.v2")), [])


if __name__ == "__main__":
    unittest.main()
