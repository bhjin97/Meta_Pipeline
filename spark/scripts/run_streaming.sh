#!/bin/bash
set -e

/opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --conf spark.cores.max="${SPARK_STREAMING_CORES_MAX}" \
  --conf spark.executor.cores="${SPARK_STREAMING_EXECUTOR_CORES}" \
  --conf spark.executor.memory="${SPARK_STREAMING_EXECUTOR_MEMORY}" \
  --conf spark.driver.memory="${SPARK_STREAMING_DRIVER_MEMORY}" \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS}" \
  /app/spark/streaming/all_events_to_minio_redis.py