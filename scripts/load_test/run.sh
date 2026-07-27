#!/usr/bin/env bash

set -Eeuo pipefail

# --------------------------------------------------
# 경로 및 실행 변수
# --------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

LOAD_TEST_ID="${1:-load_test_1000000}"
EPS="${2:-2500}"
TOPIC_PREFIX="${3:-load-test-}"

KAFKA_COMPOSE="${PROJECT_ROOT}/kafka/docker-compose.yml"

PRODUCER_SERVICE="producer"
PRODUCER_SCRIPT="kafka/producer/load_test_producer.py"

INPUT_FILE_HOST="${PROJECT_ROOT}/origin_data_processing/data/load_test/${LOAD_TEST_ID}/all_events_sorted.jsonl"
INPUT_FILE_CONTAINER="origin_data_processing/data/load_test/${LOAD_TEST_ID}/all_events_sorted.jsonl"

BOOTSTRAP_SERVER="kafka:29092"

# --------------------------------------------------
# 출력 및 오류 함수
# --------------------------------------------------
log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

fail() {
  printf '\n[ERROR] %s\n' "$1" >&2
  exit 1
}

# --------------------------------------------------
# 사전 검사
# --------------------------------------------------
command -v docker >/dev/null 2>&1 ||
  fail "docker 명령을 찾을 수 없습니다."

[[ -f "${KAFKA_COMPOSE}" ]] ||
  fail "Kafka Compose 파일을 찾을 수 없습니다: ${KAFKA_COMPOSE}"

[[ -f "${INPUT_FILE_HOST}" ]] ||
  fail "부하 테스트 입력 파일을 찾을 수 없습니다: ${INPUT_FILE_HOST}"

if ! [[ "${EPS}" =~ ^[0-9]+$ ]]; then
  fail "EPS는 0 이상의 정수여야 합니다: ${EPS}"
fi

if [[ -z "${TOPIC_PREFIX}" ]]; then
  fail "TOPIC_PREFIX가 비어 있습니다."
fi

if ! docker inspect kafka >/dev/null 2>&1; then
  fail "Kafka 컨테이너를 찾을 수 없습니다."
fi

KAFKA_RUNNING="$(
  docker inspect \
    --format '{{.State.Running}}' \
    kafka 2>/dev/null || echo "false"
)"

if [[ "${KAFKA_RUNNING}" != "true" ]]; then
  fail "Kafka 컨테이너가 실행 중이 아닙니다."
fi

STREAMING_RUNNING="$(
  docker inspect \
    --format '{{.State.Running}}' \
    spark-streaming-load-test 2>/dev/null || echo "false"
)"

if [[ "${STREAMING_RUNNING}" != "true" ]]; then
  fail "spark-streaming-load-test 컨테이너가 실행 중이 아닙니다."
fi

FILE_SIZE="$(du -h "${INPUT_FILE_HOST}" | cut -f1)"

printf '\n========================================\n'
printf ' Load test producer\n'
printf ' LOAD_TEST_ID : %s\n' "${LOAD_TEST_ID}"
printf ' EPS          : %s\n' "${EPS}"
printf ' TOPIC_PREFIX : %s\n' "${TOPIC_PREFIX}"
printf ' INPUT_SIZE   : %s\n' "${FILE_SIZE}"
printf '========================================\n'

# --------------------------------------------------
# Producer 실행
# --------------------------------------------------
log "부하 테스트 Producer를 실행합니다."

cd "${PROJECT_ROOT}"

docker compose \
  -f "${KAFKA_COMPOSE}" \
  run \
  --rm \
  "${PRODUCER_SERVICE}" \
  python "${PRODUCER_SCRIPT}" \
    --input-file "${INPUT_FILE_CONTAINER}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --events-per-sec "${EPS}" \
    --topic-prefix "${TOPIC_PREFIX}"

log "Producer 실행이 완료되었습니다."
