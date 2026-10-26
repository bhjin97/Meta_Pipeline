import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark"))

from generated.manifest import (  # noqa: E402
    ENTITY_NAMES,
    ENTITY_PATHS,
    EVENT_NAMES,
    EVENT_PATHS,
    build_manifest,
    serialize_manifest,
)
from generated.publication import (  # noqa: E402
    CurrentConflictError,
    PublicationError,
    PublicationStateUnknownError,
    SnapshotConflictError,
    SnapshotPublisher,
    current_path,
    get_entity_paths,
    load_current_snapshot,
    load_snapshot,
    manifest_path,
    parse_current,
    read_current_state,
    serialize_current,
    snapshot_path,
)
from generated.snapshot import (  # noqa: E402
    SnapshotValidationError,
    ValidatedSnapshot,
    build_snapshot,
    serialize_snapshot,
    validate_append_only,
    validate_snapshot,
)


ROOT = "memory://generated"
RUN1 = "gen_20260921_001"
RUN2 = "gen_20261011_001"
RUN3 = "gen_20261012_001"
NOW = datetime(2026, 10, 13, tzinfo=timezone.utc)


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.parquet_prefixes = set()
        self.fail_write_paths = set()
        self.write_then_fail_paths = set()
        self.fail_read_paths = set()
        self.read_hooks = {}
        self.read_counts = {}

    def exists(self, path):
        return path in self.objects

    def read_bytes(self, path):
        self.read_counts[path] = self.read_counts.get(path, 0) + 1
        hook = self.read_hooks.get((path, self.read_counts[path]))
        if hook:
            hook(self)
        if path in self.fail_read_paths:
            raise OSError(f"injected read failure: {path}")
        if path not in self.objects:
            raise FileNotFoundError(path)
        return self.objects[path]

    def write_bytes(self, path, payload, overwrite):
        if path in self.fail_write_paths:
            raise OSError(f"injected write failure: {path}")
        if not overwrite and path in self.objects:
            raise FileExistsError(path)
        self.objects[path] = payload
        if path in self.write_then_fail_paths:
            raise OSError(f"injected lost response: {path}")

    def has_parquet_objects(self, path):
        return path.rstrip("/") in self.parquet_prefixes


def manifest_for(run_id, committed=True):
    entity_artifacts = {
        name: {
            "path": ENTITY_PATHS[name],
            "row_count": 1,
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "name": "id",
                        "type": "string",
                        "nullable": False,
                        "metadata": {},
                    }
                ],
            },
        }
        for name in ENTITY_NAMES
    }
    event_artifacts = {
        name: {
            "path": EVENT_PATHS[name],
            "line_count": 1,
            "size_bytes": 1,
            "sha256": "0" * 64,
        }
        for name in EVENT_NAMES
    }
    manifest = build_manifest(run_id, 1, entity_artifacts, event_artifacts)
    if not committed:
        manifest["upload"]["status"] = "IN_PROGRESS"
    return manifest


def add_run(storage, run_id):
    storage.objects[manifest_path(ROOT, run_id)] = serialize_manifest(
        manifest_for(run_id)
    )
    for name in ENTITY_NAMES:
        storage.parquet_prefixes.add(
            f"{ROOT}/runs/{run_id}/entities/{name}"
        )


def add_snapshot(storage, runs):
    snapshot = build_snapshot(runs, NOW)
    storage.objects[snapshot_path(ROOT, snapshot["snapshot_id"])] = (
        serialize_snapshot(snapshot)
    )
    return snapshot


def point_current(storage, snapshot):
    storage.objects[current_path(ROOT)] = serialize_current(snapshot["snapshot_id"])


class Phase3PublicationTests(unittest.TestCase):
    def setUp(self):
        self.storage = FakeStorage()
        for run_id in (RUN1, RUN2, RUN3):
            add_run(self.storage, run_id)
        self.publisher = SnapshotPublisher(self.storage, ROOT)

    def test_t01_bootstrap(self):
        result = self.publisher.publish([RUN1])
        self.assertEqual("PUBLISHED", result.status)
        self.assertEqual([RUN1], load_current_snapshot(self.storage, ROOT)["runs"])

    def test_t02_append(self):
        first = self.publisher.publish([RUN1])
        second = self.publisher.publish([RUN2])
        self.assertEqual([RUN1, RUN2], second.snapshot["runs"])
        self.assertNotEqual(first.snapshot["snapshot_id"], second.snapshot["snapshot_id"])

    def test_t03_retry_is_no_op(self):
        first = self.publisher.publish([RUN1])
        count = len(self.storage.objects)
        retry = self.publisher.publish([RUN1])
        self.assertEqual("NO_OP", retry.status)
        self.assertEqual(first.snapshot["snapshot_id"], retry.snapshot["snapshot_id"])
        self.assertEqual(count, len(self.storage.objects))

    def test_t04_duplicate_request_rejected(self):
        with self.assertRaisesRegex(PublicationError, "duplicate"):
            self.publisher.publish([RUN1, RUN1])

    def test_t05_missing_run_rejected(self):
        with self.assertRaisesRegex(PublicationError, "manifest does not exist"):
            self.publisher.publish(["gen_20261013_001"])

    def test_t06_malformed_manifest_rejected(self):
        self.storage.objects[manifest_path(ROOT, RUN1)] = b"not json"
        with self.assertRaisesRegex(PublicationError, "invalid committed manifest"):
            self.publisher.publish([RUN1])

    def test_t07_uncommitted_manifest_rejected(self):
        self.storage.objects[manifest_path(ROOT, RUN1)] = serialize_manifest(
            manifest_for(RUN1)
        ).replace(b'"status": "COMPLETED"', b'"status": "IN_PROGRESS"', 1)
        with self.assertRaisesRegex(PublicationError, "invalid committed manifest"):
            self.publisher.publish([RUN1])

    def test_t08_manifest_run_id_mismatch_rejected(self):
        self.storage.objects[manifest_path(ROOT, RUN1)] = serialize_manifest(
            manifest_for(RUN2)
        )
        with self.assertRaisesRegex(PublicationError, "run_id does not match"):
            self.publisher.publish([RUN1])

    def test_t09_missing_entity_parquet_rejected(self):
        self.storage.parquet_prefixes.remove(
            f"{ROOT}/runs/{RUN1}/entities/reviews"
        )
        with self.assertRaisesRegex(PublicationError, "entity=reviews"):
            self.publisher.publish([RUN1])

    def test_t10_snapshot_schema_and_version_rejected(self):
        snapshot = add_snapshot(self.storage, [RUN1])
        path = snapshot_path(ROOT, snapshot["snapshot_id"])
        payload = json.loads(self.storage.objects[path])
        payload["snapshot_version"] = 2
        self.storage.objects[path] = json.dumps(payload).encode()
        point_current(self.storage, snapshot)
        with self.assertRaisesRegex(PublicationError, "snapshot_version"):
            load_current_snapshot(self.storage, ROOT)

    def test_t11_malformed_current_rejected_not_uninitialized(self):
        self.storage.objects[current_path(ROOT)] = b"bad pointer with spaces\n"
        with self.assertRaisesRegex(PublicationError, "invalid pointer"):
            read_current_state(self.storage, ROOT)

    def test_t12_snapshot_id_collision_rejected(self):
        candidate = build_snapshot([RUN1], NOW)
        collision = dict(candidate)
        collision["runs"] = [RUN2]
        self.storage.objects[
            snapshot_path(ROOT, candidate["snapshot_id"])
        ] = serialize_snapshot(collision)
        with self.assertRaises(SnapshotConflictError):
            self.publisher.publish([RUN1])

    def test_t13_snapshot_creation_failure_keeps_current(self):
        old = add_snapshot(self.storage, [RUN1])
        point_current(self.storage, old)
        candidate = build_snapshot([RUN1, RUN2])
        self.storage.fail_write_paths.add(
            snapshot_path(ROOT, candidate["snapshot_id"])
        )
        with self.assertRaisesRegex(PublicationError, "creation failed"):
            self.publisher.publish([RUN2])
        self.assertEqual(old["snapshot_id"], parse_current(self.storage.objects[current_path(ROOT)]))

    def test_t14_current_write_response_loss_resolved_by_read_back(self):
        self.storage.write_then_fail_paths.add(current_path(ROOT))
        result = self.publisher.publish([RUN1])
        self.assertEqual("PUBLISHED", result.status)
        self.assertEqual(result.snapshot["snapshot_id"], read_current_state(self.storage, ROOT).snapshot_id)

        storage = FakeStorage()
        add_run(storage, RUN1)
        storage.fail_write_paths.add(current_path(ROOT))
        with self.assertRaisesRegex(PublicationError, "previous pointer remains"):
            SnapshotPublisher(storage, ROOT).publish([RUN1])
        self.assertFalse(read_current_state(storage, ROOT).initialized)

    def test_t15_current_read_back_failure_is_fail_closed(self):
        path = current_path(ROOT)

        def fail_future_reads(storage):
            storage.fail_read_paths.add(path)

        self.storage.read_hooks[(path, 1)] = fail_future_reads
        # An absent CURRENT is checked by exists, so inject failure after its write.
        original_write = self.storage.write_bytes

        def write_and_break_reads(write_path, payload, overwrite):
            original_write(write_path, payload, overwrite)
            if write_path == path:
                self.storage.fail_read_paths.add(path)

        self.storage.write_bytes = write_and_break_reads
        with self.assertRaises(PublicationStateUnknownError):
            self.publisher.publish([RUN1])

    def test_t16_stale_current_rejected(self):
        old = add_snapshot(self.storage, [RUN1])
        point_current(self.storage, old)
        other = add_snapshot(self.storage, [RUN1, RUN3])
        path = current_path(ROOT)

        def replace_current(storage):
            storage.objects[path] = serialize_current(other["snapshot_id"])

        self.storage.read_hooks[(path, 2)] = replace_current
        with self.assertRaises(CurrentConflictError):
            self.publisher.publish([RUN2])

    def test_t17_reader_snapshot_is_fixed(self):
        first = self.publisher.publish([RUN1]).snapshot
        fixed = load_current_snapshot(self.storage, ROOT)
        self.publisher.publish([RUN2])
        self.assertEqual([RUN1], fixed["runs"])
        self.assertEqual(
            [f"{ROOT}/runs/{RUN1}/entities/orders"],
            get_entity_paths(fixed, "orders", ROOT),
        )
        self.assertNotEqual(first["snapshot_id"], load_current_snapshot(self.storage, ROOT)["snapshot_id"])

    def test_t18_run_removal_or_reorder_rejected(self):
        previous = build_snapshot([RUN1, RUN2], NOW)
        removed = build_snapshot([RUN1], NOW)
        reordered = build_snapshot([RUN2, RUN1], NOW)
        with self.assertRaises(SnapshotValidationError):
            validate_append_only(previous, removed)
        with self.assertRaises(SnapshotValidationError):
            validate_append_only(previous, reordered)

    def test_snapshot_version_requires_exact_supported_int(self):
        snapshot = build_snapshot([RUN1], NOW)
        validate_snapshot(snapshot, snapshot["snapshot_id"])
        for invalid in (True, False, 1.0, "1", 2):
            candidate = dict(snapshot)
            candidate["snapshot_version"] = invalid
            with self.subTest(snapshot_version=invalid):
                with self.assertRaisesRegex(SnapshotValidationError, "snapshot_version"):
                    validate_snapshot(candidate, snapshot["snapshot_id"])

    def test_reader_returns_validated_snapshot_and_preserves_run_order(self):
        snapshot = add_snapshot(self.storage, [RUN2, RUN1])
        point_current(self.storage, snapshot)
        loaded = load_current_snapshot(self.storage, ROOT)
        self.assertIsInstance(loaded, ValidatedSnapshot)
        self.assertEqual([RUN2, RUN1], loaded["runs"])
        self.assertEqual(
            [
                f"{ROOT}/runs/{RUN2}/entities/orders",
                f"{ROOT}/runs/{RUN1}/entities/orders",
            ],
            get_entity_paths(loaded, "orders", ROOT),
        )

    def test_entity_paths_reject_unvalidated_snapshots(self):
        for snapshot in (
            {"runs": [RUN1]},
            {"runs": []},
            {"runs": ["not-a-run"]},
        ):
            with self.subTest(snapshot=snapshot):
                with self.assertRaisesRegex(TypeError, "loaded and validated"):
                    get_entity_paths(snapshot, "orders", ROOT)
        with self.assertRaisesRegex(TypeError, "snapshot reader"):
            ValidatedSnapshot({"runs": [RUN1]})

    def test_validated_snapshot_runs_cannot_be_changed(self):
        snapshot = add_snapshot(self.storage, [RUN1])
        loaded = load_snapshot(self.storage, snapshot["snapshot_id"], ROOT)
        with self.assertRaisesRegex(TypeError, "immutable"):
            loaded["runs"].append(RUN2)
        with self.assertRaisesRegex(TypeError, "immutable"):
            loaded["runs"][0] = RUN2

    def test_entity_paths_performs_no_storage_io(self):
        snapshot = add_snapshot(self.storage, [RUN1])
        point_current(self.storage, snapshot)
        loaded = load_current_snapshot(self.storage, ROOT)
        reads_before = dict(self.storage.read_counts)
        objects_before = dict(self.storage.objects)
        self.assertEqual(
            [f"{ROOT}/runs/{RUN1}/entities/orders"],
            get_entity_paths(loaded, "orders", ROOT),
        )
        self.assertEqual(reads_before, self.storage.read_counts)
        self.assertEqual(objects_before, self.storage.objects)


if __name__ == "__main__":
    unittest.main()
