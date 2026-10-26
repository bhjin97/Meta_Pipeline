import hashlib
import json
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Dict, Mapping, Sequence


SNAPSHOT_VERSION = 1
SNAPSHOT_ID_PATTERN = re.compile(r"^snapshot_[a-z0-9][a-z0-9_-]{0,63}$")
RUN_ID_PATTERN = re.compile(r"^gen_\d{8}_\d{3}$")


class SnapshotValidationError(ValueError):
    """Raised when metadata snapshot content violates the Phase 3 contract."""


class _ImmutableRuns(Sequence[str]):
    """List-compatible run order that rejects in-place changes."""

    def __init__(self, runs: Sequence[str]):
        self._runs = tuple(runs)

    def __getitem__(self, index: Any) -> Any:
        return self._runs[index]

    def __len__(self) -> int:
        return len(self._runs)

    def __iter__(self) -> Iterator[str]:
        return iter(self._runs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Sequence):
            return False
        return list(self._runs) == list(other)

    def _reject(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("validated snapshot runs are immutable")

    __setitem__ = _reject
    __delitem__ = _reject
    __iadd__ = _reject
    __imul__ = _reject
    append = _reject
    clear = _reject
    extend = _reject
    insert = _reject
    pop = _reject
    remove = _reject
    reverse = _reject
    sort = _reject


_VALIDATED_SNAPSHOT_TOKEN = object()


class ValidatedSnapshot(Mapping[str, Any]):
    """Immutable snapshot produced only after reader validation."""

    def __init__(self, snapshot: Mapping[str, Any], token: object = None):
        if token is not _VALIDATED_SNAPSHOT_TOKEN:
            raise TypeError("ValidatedSnapshot must be created by the snapshot reader")
        values = dict(snapshot)
        values["runs"] = _ImmutableRuns(values["runs"])
        self._values = MappingProxyType(values)

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


def _make_validated_snapshot(snapshot: Mapping[str, Any]) -> ValidatedSnapshot:
    """Internal bridge used after the reader has completed all validation."""
    validate_snapshot(snapshot, snapshot.get("snapshot_id"))
    return ValidatedSnapshot(snapshot, _VALIDATED_SNAPSHOT_TOKEN)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SnapshotValidationError(message)


def validate_snapshot_id(snapshot_id: Any) -> str:
    _require(
        isinstance(snapshot_id, str)
        and SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id) is not None,
        "invalid snapshot_id",
    )
    return snapshot_id


def validate_run_id(run_id: Any) -> str:
    _require(
        isinstance(run_id, str) and RUN_ID_PATTERN.fullmatch(run_id) is not None,
        f"invalid run_id: {run_id!r}",
    )
    try:
        datetime.strptime(run_id[4:12], "%Y%m%d")
    except ValueError as exc:
        raise SnapshotValidationError(
            f"invalid run_id date: {run_id!r}"
        ) from exc
    return run_id


def _validate_created_at(value: Any) -> None:
    _require(isinstance(value, str), "created_at must be a string")
    _require(value.endswith("Z"), "created_at must be an ISO-8601 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise SnapshotValidationError(
            "created_at must be an ISO-8601 UTC timestamp"
        ) from exc
    _require(
        parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0,
        "created_at must be an ISO-8601 UTC timestamp",
    )


def validate_snapshot(
    snapshot: Mapping[str, Any],
    expected_snapshot_id: str,
) -> None:
    _require(isinstance(snapshot, dict), "snapshot root must be an object")
    _require(
        set(snapshot) == {"snapshot_version", "snapshot_id", "created_at", "runs"},
        "snapshot fields are invalid",
    )
    _require(
        type(snapshot["snapshot_version"]) is int
        and snapshot["snapshot_version"] == SNAPSHOT_VERSION,
        f"snapshot_version must be {SNAPSHOT_VERSION}",
    )
    validate_snapshot_id(expected_snapshot_id)
    validate_snapshot_id(snapshot["snapshot_id"])
    _require(
        snapshot["snapshot_id"] == expected_snapshot_id,
        "snapshot_id does not match its object name",
    )
    _validate_created_at(snapshot["created_at"])
    runs = snapshot["runs"]
    _require(isinstance(runs, list) and bool(runs), "runs must be a non-empty array")
    for run_id in runs:
        validate_run_id(run_id)
    _require(len(runs) == len(set(runs)), "snapshot contains duplicate run_id")


def parse_snapshot(payload: bytes, expected_snapshot_id: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError("snapshot is not valid UTF-8 JSON") from exc
    validate_snapshot(parsed, expected_snapshot_id)
    return parsed


def serialize_snapshot(snapshot: Mapping[str, Any]) -> bytes:
    validate_snapshot(snapshot, snapshot.get("snapshot_id"))
    return (
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def make_snapshot_id(runs: Sequence[str]) -> str:
    _require(bool(runs), "cannot create an empty snapshot")
    for run_id in runs:
        validate_run_id(run_id)
    _require(len(runs) == len(set(runs)), "snapshot contains duplicate run_id")
    canonical = json.dumps(list(runs), separators=(",", ":")).encode("utf-8")
    return f"snapshot_{hashlib.sha256(canonical).hexdigest()[:16]}"


def build_snapshot(
    runs: Sequence[str],
    created_at: datetime | None = None,
) -> Dict[str, Any]:
    instant = created_at or datetime.now(timezone.utc)
    _require(
        instant.tzinfo is not None and instant.utcoffset().total_seconds() == 0,
        "created_at must use UTC",
    )
    snapshot_id = make_snapshot_id(runs)
    snapshot = {
        "snapshot_version": SNAPSHOT_VERSION,
        "snapshot_id": snapshot_id,
        "created_at": instant.isoformat().replace("+00:00", "Z"),
        "runs": list(runs),
    }
    validate_snapshot(snapshot, snapshot_id)
    return snapshot


def validate_append_only(previous: Mapping[str, Any], candidate: Mapping[str, Any]) -> None:
    previous_runs = previous["runs"]
    candidate_runs = candidate["runs"]
    _require(
        candidate_runs[: len(previous_runs)] == previous_runs,
        "candidate snapshot removes, replaces, or reorders existing runs",
    )
