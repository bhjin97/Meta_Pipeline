import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

from common.spark_session import create_spark_session
from generated.manifest import (
    ENTITY_NAMES,
    ENTITY_PATHS,
    EVENT_NAMES,
    EVENT_PATHS,
    ManifestValidationError,
    build_manifest,
    parse_manifest,
    serialize_manifest,
    validate_committed_manifest,
)


RUN_ID_PATTERN = re.compile(r"^gen_\d{8}_\d{3}$")
DEFAULT_LOCAL_RUNS_ROOT = Path(
    "/app/origin_data_processing/data/generated/runs"
)
REMOTE_RUNS_ROOT = "s3a://ecommerce/bronze/generated/runs"
READ_CHUNK_SIZE = 1024 * 1024


class AlreadyCommittedError(RuntimeError):
    pass


class RemoteStateError(RuntimeError):
    pass


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Upload one Phase 1 final run dataset to MinIO and publish "
            "its manifest commit marker"
        )
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--seed",
        required=True,
        type=int,
        help=(
            "Phase 1 generator seed. Phase 1 does not persist this value, "
            "so the caller must provide it for manifest provenance."
        ),
    )
    parser.add_argument(
        "--local-runs-root",
        type=Path,
        default=DEFAULT_LOCAL_RUNS_ROOT,
    )
    return parser.parse_args()


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("invalid run_id: expected gen_YYYYMMDD_NNN")
    try:
        datetime.strptime(run_id[4:12], "%Y%m%d")
    except ValueError as exc:
        raise ValueError("invalid run_id: YYYYMMDD must be a valid date") from exc


def validate_local_run(
    local_runs_root: Path,
    run_id: str,
) -> Tuple[Path, Dict[str, Path], Dict[str, Path]]:
    print("[LOCAL] validating run dataset")
    root = local_runs_root.resolve()
    run_dir = root / run_id
    if run_dir.is_symlink():
        raise ValueError(f"local final run directory must not be a symlink: {run_dir}")
    if run_dir.parent != root or run_dir.name != run_id:
        raise ValueError("local run directory escaped the configured runs root")
    if not run_dir.is_dir():
        raise FileNotFoundError(f"local final run directory not found: {run_dir}")

    entity_files = {
        name: run_dir / "entities" / f"{name}.parquet"
        for name in ENTITY_NAMES
    }
    event_files = {
        name: run_dir / "events" / f"{name}.jsonl"
        for name in EVENT_NAMES
    }
    required_files = {**entity_files, **event_files}
    for name, path in required_files.items():
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(
                f"required local final artifact missing or invalid: {name}={path}"
            )
    print(f"[LOCAL] validation PASS run_dir={run_dir}")
    return run_dir, entity_files, event_files


def get_filesystem(spark, uri: str):
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(uri),
        spark._jsc.hadoopConfiguration(),
    )


def hadoop_path(spark, path: str):
    return spark._jvm.org.apache.hadoop.fs.Path(path)


def read_remote_bytes(fs, path) -> bytes:
    stream = fs.open(path)
    chunks = []
    try:
        while True:
            chunk = bytes(stream.readNBytes(READ_CHUNK_SIZE))
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        stream.close()
    return b"".join(chunks)


def _update_metrics(
    digest,
    chunk: bytes,
    size_bytes: int,
    newline_count: int,
    last_byte: int,
) -> Tuple[int, int, int]:
    digest.update(chunk)
    size_bytes += len(chunk)
    newline_count += chunk.count(b"\n")
    if chunk:
        last_byte = chunk[-1]
    return size_bytes, newline_count, last_byte


def local_file_metrics(path: Path) -> Dict[str, Any]:
    digest = hashlib.sha256()
    size_bytes = 0
    newline_count = 0
    last_byte = -1
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(READ_CHUNK_SIZE)
            if not chunk:
                break
            size_bytes, newline_count, last_byte = _update_metrics(
                digest, chunk, size_bytes, newline_count, last_byte
            )
    line_count = newline_count + int(size_bytes > 0 and last_byte != ord("\n"))
    return {
        "line_count": line_count,
        "size_bytes": size_bytes,
        "sha256": digest.hexdigest(),
    }


def remote_file_metrics(fs, path) -> Dict[str, Any]:
    digest = hashlib.sha256()
    size_bytes = 0
    newline_count = 0
    last_byte = -1
    stream = fs.open(path)
    try:
        while True:
            chunk = bytes(stream.readNBytes(READ_CHUNK_SIZE))
            if not chunk:
                break
            size_bytes, newline_count, last_byte = _update_metrics(
                digest, chunk, size_bytes, newline_count, last_byte
            )
    finally:
        stream.close()
    line_count = newline_count + int(size_bytes > 0 and last_byte != ord("\n"))
    return {
        "line_count": line_count,
        "size_bytes": size_bytes,
        "sha256": digest.hexdigest(),
    }


def read_and_validate_manifest(fs, spark, manifest_uri: str, run_id: str):
    manifest_path = hadoop_path(spark, manifest_uri)
    payload = read_remote_bytes(fs, manifest_path)
    manifest = parse_manifest(payload)
    validate_committed_manifest(manifest, run_id)
    return manifest


def prepare_remote_run(fs, spark, run_uri: str, run_id: str) -> None:
    print("[REMOTE] checking existing state")
    run_path = hadoop_path(spark, run_uri)
    manifest_uri = f"{run_uri}/manifest.json"
    manifest_path = hadoop_path(spark, manifest_uri)

    if fs.exists(manifest_path):
        try:
            read_and_validate_manifest(fs, spark, manifest_uri, run_id)
        except ManifestValidationError as exc:
            raise RemoteStateError(
                "remote manifest exists but is malformed or invalid; "
                "refusing automatic cleanup"
            ) from exc
        raise AlreadyCommittedError(
            f"remote run is already committed and immutable: {run_uri}"
        )

    if not fs.exists(run_path):
        print("[REMOTE] no existing run prefix; starting new upload")
        return

    print("[REMOTE] partial run prefix found; rechecking manifest before cleanup")
    if fs.exists(manifest_path):
        raise RemoteStateError(
            "manifest appeared during partial-run cleanup; single-writer "
            "contract may have been violated"
        )
    expected_uri = f"{REMOTE_RUNS_ROOT}/{run_id}"
    if run_uri != expected_uri:
        raise RemoteStateError(
            f"refusing to delete unexpected remote prefix: {run_uri}"
        )
    if not fs.delete(run_path, True):
        raise RemoteStateError(f"failed to delete partial remote run: {run_uri}")
    if fs.exists(run_path):
        raise RemoteStateError(
            f"partial remote run still exists after cleanup: {run_uri}"
        )
    print(f"[REMOTE] partial run cleanup PASS prefix={run_uri}")


def upload_entities(
    spark,
    run_uri: str,
    entity_files: Dict[str, Path],
) -> Dict[str, Dict[str, Any]]:
    artifacts = {}
    for name in ENTITY_NAMES:
        source_path = str(entity_files[name])
        remote_uri = f"{run_uri}/{ENTITY_PATHS[name]}"
        print(f"[UPLOAD][ENTITY] {name} source={source_path} target={remote_uri}")
        source_df = spark.read.parquet(source_path)
        source_rows = source_df.count()
        source_schema_json = source_df.schema.json()
        source_schema = json.loads(source_schema_json)

        source_df.write.mode("errorifexists").parquet(remote_uri)

        remote_df = spark.read.parquet(remote_uri)
        remote_rows = remote_df.count()
        remote_schema_json = remote_df.schema.json()
        if remote_rows != source_rows:
            raise RuntimeError(
                f"entity row count mismatch for {name}: "
                f"source={source_rows}, remote={remote_rows}"
            )
        if remote_schema_json != source_schema_json:
            raise RuntimeError(f"entity schema mismatch for {name}")
        artifacts[name] = {
            "path": ENTITY_PATHS[name],
            "row_count": source_rows,
            "schema": source_schema,
        }
        print(
            f"[VERIFY][ENTITY] {name} source_rows={source_rows} "
            f"remote_rows={remote_rows} schema=PASS"
        )
    return artifacts


def upload_events(
    fs,
    spark,
    run_uri: str,
    event_files: Dict[str, Path],
) -> Dict[str, Dict[str, Any]]:
    artifacts = {}
    for name in EVENT_NAMES:
        local_path = event_files[name]
        remote_uri = f"{run_uri}/{EVENT_PATHS[name]}"
        remote_path = hadoop_path(spark, remote_uri)
        if fs.exists(remote_path):
            raise FileExistsError(
                f"refusing to overwrite existing event object: {remote_uri}"
            )
        source_metrics = local_file_metrics(local_path)
        source_hadoop_path = hadoop_path(spark, local_path.as_uri())
        print(f"[UPLOAD][EVENT] {name} source={local_path} target={remote_uri}")
        fs.copyFromLocalFile(
            False,
            False,
            source_hadoop_path,
            remote_path,
        )

        if not fs.exists(remote_path):
            raise RuntimeError(
                f"remote event object missing after upload: {remote_uri}"
            )
        status = fs.getFileStatus(remote_path)
        if not status.isFile() or status.isDirectory():
            raise RuntimeError(f"remote event path is not a file: {remote_uri}")
        remote_metrics = remote_file_metrics(fs, remote_path)
        if remote_metrics != source_metrics:
            raise RuntimeError(
                f"event verification mismatch for {name}: "
                f"source={source_metrics}, remote={remote_metrics}"
            )
        if status.getLen() != source_metrics["size_bytes"]:
            raise RuntimeError(
                f"event FileStatus length mismatch for {name}: "
                f"status={status.getLen()}, source={source_metrics['size_bytes']}"
            )
        artifacts[name] = {
            "path": EVENT_PATHS[name],
            **source_metrics,
        }
        print(
            f"[VERIFY][EVENT] {name} lines={source_metrics['line_count']} "
            f"size={source_metrics['size_bytes']} "
            f"sha256={source_metrics['sha256']}"
        )
    return artifacts


def write_and_verify_manifest(
    fs,
    spark,
    run_uri: str,
    run_id: str,
    manifest: Dict[str, Any],
) -> None:
    manifest_uri = f"{run_uri}/manifest.json"
    manifest_path = hadoop_path(spark, manifest_uri)
    if fs.exists(manifest_path):
        raise RemoteStateError(
            f"manifest appeared before commit write: {manifest_uri}"
        )
    payload = serialize_manifest(manifest)
    print("[MANIFEST] writing final commit marker")
    stream = fs.create(manifest_path, False)
    try:
        stream.write(bytearray(payload))
    finally:
        stream.close()

    read_back = read_and_validate_manifest(fs, spark, manifest_uri, run_id)
    if read_back != manifest:
        raise ManifestValidationError(
            "manifest read-back content differs from the committed payload"
        )
    print("[MANIFEST] read-back validation PASS")


def run(args) -> None:
    validate_run_id(args.run_id)
    if args.seed < 0:
        raise ValueError("seed must be non-negative")
    print(f"[PHASE2] run_id={args.run_id}")
    _, entity_files, event_files = validate_local_run(
        args.local_runs_root,
        args.run_id,
    )
    run_uri = f"{REMOTE_RUNS_ROOT}/{args.run_id}"

    spark = create_spark_session(
        f"Phase 2 Upload Run Dataset {args.run_id}"
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        fs = get_filesystem(spark, run_uri)
        prepare_remote_run(fs, spark, run_uri, args.run_id)
        entity_artifacts = upload_entities(
            spark,
            run_uri,
            entity_files,
        )
        event_artifacts = upload_events(
            fs,
            spark,
            run_uri,
            event_files,
        )
        manifest = build_manifest(
            args.run_id,
            args.seed,
            entity_artifacts,
            event_artifacts,
        )
        write_and_verify_manifest(
            fs,
            spark,
            run_uri,
            args.run_id,
            manifest,
        )
        print(f"[PHASE2] COMMITTED run_id={args.run_id} uri={run_uri}")
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    try:
        run(args)
    except Exception as exc:
        print(
            f"[PHASE2] FAILED run_id={getattr(args, 'run_id', None)} "
            f"error={type(exc).__name__}: {exc}"
        )
        raise


if __name__ == "__main__":
    main()
