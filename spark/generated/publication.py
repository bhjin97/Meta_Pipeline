from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Protocol

from generated.manifest import (
    ENTITY_NAMES,
    ManifestValidationError,
    parse_manifest,
    validate_committed_manifest,
)
from generated.snapshot import (
    SnapshotValidationError,
    ValidatedSnapshot,
    _make_validated_snapshot,
    build_snapshot,
    parse_snapshot,
    serialize_snapshot,
    validate_append_only,
    validate_run_id,
    validate_snapshot_id,
)


DEFAULT_ROOT = "s3a://ecommerce/bronze/generated"
CURRENT_NAME = "CURRENT"


class PublicationError(RuntimeError):
    pass


class MissingRunError(PublicationError):
    pass


class SnapshotConflictError(PublicationError):
    pass


class CurrentConflictError(PublicationError):
    pass


class PublicationStateUnknownError(PublicationError):
    pass


class Storage(Protocol):
    def exists(self, path: str) -> bool: ...
    def read_bytes(self, path: str) -> bytes: ...
    def write_bytes(self, path: str, payload: bytes, overwrite: bool) -> None: ...
    def has_parquet_objects(self, path: str) -> bool: ...


@dataclass(frozen=True)
class CurrentState:
    initialized: bool
    snapshot_id: str | None = None


@dataclass(frozen=True)
class PublishResult:
    status: str
    snapshot: Mapping[str, Any]


def _join(root: str, *parts: str) -> str:
    return "/".join([root.rstrip("/"), *(part.strip("/") for part in parts)])


def snapshot_path(root: str, snapshot_id: str) -> str:
    return _join(root, "metadata_snapshots", f"{snapshot_id}.json")


def manifest_path(root: str, run_id: str) -> str:
    return _join(root, "runs", run_id, "manifest.json")


def current_path(root: str) -> str:
    return _join(root, CURRENT_NAME)


def parse_current(payload: bytes) -> str:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PublicationError("CURRENT is not valid UTF-8") from exc
    lines = text.splitlines()
    if len(lines) != 1 or lines[0] != lines[0].strip():
        raise PublicationError("CURRENT must contain exactly one snapshot_id")
    try:
        return validate_snapshot_id(lines[0])
    except SnapshotValidationError as exc:
        raise PublicationError(f"CURRENT has invalid pointer: {exc}") from exc


def serialize_current(snapshot_id: str) -> bytes:
    validate_snapshot_id(snapshot_id)
    return f"{snapshot_id}\n".encode("utf-8")


def read_current_state(storage: Storage, root: str = DEFAULT_ROOT) -> CurrentState:
    path = current_path(root)
    if not storage.exists(path):
        return CurrentState(initialized=False)
    return CurrentState(initialized=True, snapshot_id=parse_current(storage.read_bytes(path)))


def validate_committed_run(storage: Storage, root: str, run_id: str) -> Dict[str, Any]:
    try:
        validate_run_id(run_id)
    except SnapshotValidationError as exc:
        raise PublicationError(f"run_id={run_id!r}: {exc}") from exc
    path = manifest_path(root, run_id)
    if not storage.exists(path):
        raise MissingRunError(f"run_id={run_id}: manifest does not exist")
    try:
        manifest = parse_manifest(storage.read_bytes(path))
        validate_committed_manifest(manifest, run_id)
    except ManifestValidationError as exc:
        raise PublicationError(f"run_id={run_id}: invalid committed manifest: {exc}") from exc
    for entity_name in ENTITY_NAMES:
        relative_path = manifest["artifacts"]["entities"][entity_name]["path"]
        entity_path = _join(root, "runs", run_id, relative_path)
        if not storage.has_parquet_objects(entity_path):
            raise PublicationError(
                f"run_id={run_id}: entity={entity_name} has no Parquet objects"
            )
    return manifest


def load_snapshot(
    storage: Storage,
    snapshot_id: str,
    root: str = DEFAULT_ROOT,
) -> ValidatedSnapshot:
    try:
        validate_snapshot_id(snapshot_id)
    except SnapshotValidationError as exc:
        raise PublicationError(f"snapshot_id={snapshot_id!r}: {exc}") from exc
    path = snapshot_path(root, snapshot_id)
    if not storage.exists(path):
        raise PublicationError(f"snapshot_id={snapshot_id}: snapshot does not exist")
    try:
        snapshot = parse_snapshot(storage.read_bytes(path), snapshot_id)
    except SnapshotValidationError as exc:
        raise PublicationError(f"snapshot_id={snapshot_id}: invalid snapshot: {exc}") from exc
    for run_id in snapshot["runs"]:
        validate_committed_run(storage, root, run_id)
    return _make_validated_snapshot(snapshot)


def load_current_snapshot(
    storage: Storage,
    root: str = DEFAULT_ROOT,
) -> ValidatedSnapshot | None:
    state = read_current_state(storage, root)
    if not state.initialized:
        return None
    return load_snapshot(storage, state.snapshot_id, root)


def get_entity_paths(
    snapshot: ValidatedSnapshot,
    entity_name: str,
    root: str = DEFAULT_ROOT,
) -> List[str]:
    if not isinstance(snapshot, ValidatedSnapshot):
        raise TypeError("snapshot must be loaded and validated by the snapshot reader")
    if entity_name not in ENTITY_NAMES:
        raise ValueError(f"unsupported entity_name: {entity_name}")
    return [
        _join(root, "runs", run_id, "entities", entity_name)
        for run_id in snapshot["runs"]
    ]


def _same_current(left: CurrentState, right: CurrentState) -> bool:
    return left == right


class SnapshotPublisher:
    def __init__(self, storage: Storage, root: str = DEFAULT_ROOT):
        self.storage = storage
        self.root = root.rstrip("/")

    def _write_or_reuse_snapshot(self, snapshot: Mapping[str, Any]) -> Dict[str, Any]:
        snapshot_id = snapshot["snapshot_id"]
        path = snapshot_path(self.root, snapshot_id)
        if self.storage.exists(path):
            existing = load_snapshot(self.storage, snapshot_id, self.root)
            if existing["runs"] != snapshot["runs"]:
                raise SnapshotConflictError(
                    f"snapshot_id={snapshot_id}: existing snapshot content conflicts"
                )
            return existing
        try:
            self.storage.write_bytes(path, serialize_snapshot(snapshot), overwrite=False)
        except Exception as exc:
            if self.storage.exists(path):
                existing = load_snapshot(self.storage, snapshot_id, self.root)
                if existing["runs"] == snapshot["runs"]:
                    return existing
            raise PublicationError(
                f"snapshot_id={snapshot_id}: snapshot creation failed"
            ) from exc
        read_back = load_snapshot(self.storage, snapshot_id, self.root)
        if read_back != snapshot:
            raise PublicationError(
                f"snapshot_id={snapshot_id}: snapshot read-back differs"
            )
        return read_back

    def _update_current(
        self,
        expected: CurrentState,
        desired_snapshot_id: str,
    ) -> None:
        observed = read_current_state(self.storage, self.root)
        if not _same_current(observed, expected):
            raise CurrentConflictError(
                "CURRENT changed while publication was in progress"
            )
        path = current_path(self.root)
        payload = serialize_current(desired_snapshot_id)
        try:
            self.storage.write_bytes(path, payload, overwrite=expected.initialized)
        except Exception as exc:
            try:
                read_back = read_current_state(self.storage, self.root)
            except Exception as read_exc:
                raise PublicationStateUnknownError(
                    "CURRENT write failed and read-back also failed"
                ) from read_exc
            if read_back.initialized and read_back.snapshot_id == desired_snapshot_id:
                return
            if _same_current(read_back, expected):
                raise PublicationError("CURRENT update failed; previous pointer remains") from exc
            raise CurrentConflictError(
                "CURRENT write failed and read-back contains an unexpected pointer"
            ) from exc
        try:
            read_back = read_current_state(self.storage, self.root)
        except Exception as exc:
            raise PublicationStateUnknownError("CURRENT read-back failed") from exc
        if not read_back.initialized or read_back.snapshot_id != desired_snapshot_id:
            raise PublicationStateUnknownError("CURRENT read-back does not match publication")

    def publish(self, new_runs: Iterable[str]) -> PublishResult:
        requested = list(new_runs)
        if not requested:
            raise PublicationError("at least one run_id is required")
        if len(requested) != len(set(requested)):
            raise PublicationError("publish request contains duplicate run_id")

        expected_current = read_current_state(self.storage, self.root)
        previous = None
        existing_runs: List[str] = []
        if expected_current.initialized:
            previous = load_snapshot(
                self.storage, expected_current.snapshot_id, self.root
            )
            existing_runs = list(previous["runs"])

        for run_id in requested:
            validate_committed_run(self.storage, self.root, run_id)

        additions = [run_id for run_id in requested if run_id not in existing_runs]
        if not additions:
            if previous is None:
                raise PublicationError("cannot publish an empty initial snapshot")
            return PublishResult(status="NO_OP", snapshot=previous)

        candidate = build_snapshot(existing_runs + additions)
        if previous is not None:
            validate_append_only(previous, candidate)
        committed = self._write_or_reuse_snapshot(candidate)
        self._update_current(expected_current, committed["snapshot_id"])
        return PublishResult(status="PUBLISHED", snapshot=committed)
