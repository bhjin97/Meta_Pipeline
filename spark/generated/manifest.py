import json
from typing import Any, Dict, Mapping


MANIFEST_VERSION = 1
COMPLETED = "COMPLETED"

ENTITY_NAMES = (
    "orders",
    "order_items",
    "payments",
    "reviews",
)
EVENT_NAMES = (
    "order_events",
    "delivery_events",
    "review_events",
    "all_events_sorted",
)

ENTITY_PATHS = {
    name: f"entities/{name}/"
    for name in ENTITY_NAMES
}
EVENT_PATHS = {
    name: f"events/{name}.jsonl"
    for name in EVENT_NAMES
}


class ManifestValidationError(ValueError):
    """Raised when a remote manifest cannot prove a committed run."""


def build_manifest(
    run_id: str,
    seed: int,
    entity_artifacts: Mapping[str, Mapping[str, Any]],
    event_artifacts: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "run_id": run_id,
        "seed": seed,
        "generation": {
            "status": COMPLETED,
        },
        "upload": {
            "status": COMPLETED,
        },
        "artifacts": {
            "entities": dict(entity_artifacts),
            "events": dict(event_artifacts),
        },
    }
    validate_committed_manifest(manifest, run_id)
    return manifest


def serialize_manifest(manifest: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def parse_manifest(payload: bytes) -> Dict[str, Any]:
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestValidationError(
            "manifest is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(parsed, dict):
        raise ManifestValidationError("manifest root must be an object")
    return parsed


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ManifestValidationError(message)


def _validate_entity_artifact(
    name: str,
    artifact: Any,
) -> None:
    _require(isinstance(artifact, dict), f"entity artifact {name} must be an object")
    _require(
        set(artifact) == {"path", "row_count", "schema"},
        f"entity artifact {name} has invalid fields",
    )
    _require(
        artifact["path"] == ENTITY_PATHS[name],
        f"entity artifact {name} has an invalid path",
    )
    _require(
        isinstance(artifact["row_count"], int)
        and not isinstance(artifact["row_count"], bool)
        and artifact["row_count"] >= 0,
        f"entity artifact {name} has an invalid row_count",
    )
    _require(
        isinstance(artifact["schema"], dict),
        f"entity artifact {name} has an invalid schema",
    )


def _validate_event_artifact(
    name: str,
    artifact: Any,
) -> None:
    _require(isinstance(artifact, dict), f"event artifact {name} must be an object")
    _require(
        set(artifact) == {"path", "line_count", "size_bytes", "sha256"},
        f"event artifact {name} has invalid fields",
    )
    _require(
        artifact["path"] == EVENT_PATHS[name],
        f"event artifact {name} has an invalid path",
    )
    for field in ("line_count", "size_bytes"):
        _require(
            isinstance(artifact[field], int)
            and not isinstance(artifact[field], bool)
            and artifact[field] >= 0,
            f"event artifact {name} has an invalid {field}",
        )
    sha256 = artifact["sha256"]
    _require(
        isinstance(sha256, str)
        and len(sha256) == 64
        and all(character in "0123456789abcdef" for character in sha256),
        f"event artifact {name} has an invalid sha256",
    )


def validate_committed_manifest(
    manifest: Mapping[str, Any],
    expected_run_id: str,
) -> None:
    _require(isinstance(manifest, dict), "manifest root must be an object")
    _require(
        manifest.get("manifest_version") == MANIFEST_VERSION,
        f"manifest_version must be {MANIFEST_VERSION}",
    )
    _require(
        manifest.get("run_id") == expected_run_id,
        "manifest run_id does not match its remote path",
    )
    seed = manifest.get("seed")
    _require(
        isinstance(seed, int) and not isinstance(seed, bool) and seed >= 0,
        "manifest seed must be a non-negative integer",
    )
    _require(
        isinstance(manifest.get("generation"), dict)
        and manifest["generation"].get("status") == COMPLETED,
        "generation.status must be COMPLETED",
    )
    _require(
        isinstance(manifest.get("upload"), dict)
        and manifest["upload"].get("status") == COMPLETED,
        "upload.status must be COMPLETED",
    )

    artifacts = manifest.get("artifacts")
    _require(isinstance(artifacts, dict), "artifacts must be an object")
    _require(
        set(artifacts) == {"entities", "events"},
        "artifacts must contain exactly entities and events",
    )
    entities = artifacts["entities"]
    events = artifacts["events"]
    _require(isinstance(entities, dict), "artifacts.entities must be an object")
    _require(isinstance(events, dict), "artifacts.events must be an object")
    _require(
        set(entities) == set(ENTITY_NAMES),
        "manifest must contain all four entity artifacts",
    )
    _require(
        set(events) == set(EVENT_NAMES),
        "manifest must contain all four event artifacts",
    )
    for name in ENTITY_NAMES:
        _validate_entity_artifact(name, entities[name])
    for name in EVENT_NAMES:
        _validate_event_artifact(name, events[name])


def is_committed_run_manifest(
    manifest: Mapping[str, Any],
    expected_run_id: str,
) -> bool:
    try:
        validate_committed_manifest(manifest, expected_run_id)
    except ManifestValidationError:
        return False
    return True
