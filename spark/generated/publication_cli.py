import argparse

from generated.publication import (
    DEFAULT_ROOT,
    SnapshotPublisher,
    get_entity_paths,
    load_current_snapshot,
    read_current_state,
    validate_committed_run,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Phase 3 metadata publication")
    parser.add_argument("--root", default=DEFAULT_ROOT)
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="validate committed runs")
    validate.add_argument("--run-id", action="append", required=True)
    commands.add_parser("show-current", help="show and validate CURRENT")
    publish = commands.add_parser("publish", help="publish runs (writes storage)")
    publish.add_argument("--run-id", action="append", required=True)
    return parser.parse_args()


def run(args) -> None:
    from common.spark_session import create_spark_session
    from generated.hadoop_storage import HadoopStorage

    print(f"[PHASE3] command={args.command} root={args.root}")
    spark = create_spark_session(f"Phase 3 Metadata {args.command}")
    try:
        storage = HadoopStorage(spark, args.root)
        if args.command == "validate":
            for run_id in args.run_id:
                validate_committed_run(storage, args.root, run_id)
                print(f"[VALID] run_id={run_id}")
            return
        if args.command == "show-current":
            state = read_current_state(storage, args.root)
            if not state.initialized:
                print("[CURRENT] UNINITIALIZED")
                return
            snapshot = load_current_snapshot(storage, args.root)
            print(
                f"[CURRENT] snapshot_id={snapshot['snapshot_id']} "
                f"runs={snapshot['runs']}"
            )
            for entity_name in ("orders", "order_items", "payments", "reviews"):
                print(
                    f"[PATHS] entity={entity_name} "
                    f"paths={get_entity_paths(snapshot, entity_name, args.root)}"
                )
            return
        state = read_current_state(storage, args.root)
        print(
            f"[PUBLISH] requested_runs={args.run_id} "
            f"current_snapshot={state.snapshot_id if state.initialized else 'UNINITIALIZED'}"
        )
        result = SnapshotPublisher(storage, args.root).publish(args.run_id)
        print(
            f"[PUBLISH] status={result.status} "
            f"snapshot_id={result.snapshot['snapshot_id']} "
            f"runs={result.snapshot['runs']}"
        )
    finally:
        spark.stop()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
