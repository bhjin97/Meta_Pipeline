#!/usr/bin/env bash

set -Eeuo pipefail

# --------------------------------------------------
# 기본 설정
# --------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

LOAD_TEST_ID="${1:-load_test_1000000}"

SPARK_COMPOSE="${PROJECT_ROOT}/spark/docker-compose.yml"
STREAMING_SERVICE="spark-streaming-load-test"
STREAMING_CONTAINER="spark-streaming-load-test"

KAFKA_CONTAINER="kafka"
KAFKA_BOOTSTRAP_SERVER="kafka:29092"

REDIS_CONTAINER="redis"

MINIO_ALIAS="minio-vm"
MINIO_BUCKET="ecommerce"

LOCAL_CHECKPOINT_PATH="${PROJECT_ROOT}/data/checkpoints/load_test/${LOAD_TEST_ID}"

TOPICS=(
  "load-test-order-events"
  "load-test-delivery-events"
  "load-test-review-events"
)

# --------------------------------------------------
# 출력 함수
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

command -v mc >/dev/null 2>&1 ||
  fail "MinIO Client(mc)를 찾을 수 없습니다."

[[ -f "${SPARK_COMPOSE}" ]] ||
  fail "Spark Compose 파일을 찾을 수 없습니다: ${SPARK_COMPOSE}"

docker inspect "${KAFKA_CONTAINER}" >/dev/null 2>&1 ||
  fail "Kafka 컨테이너를 찾을 수 없습니다: ${KAFKA_CONTAINER}"

docker inspect "${REDIS_CONTAINER}" >/dev/null 2>&1 ||
  fail "Redis 컨테이너를 찾을 수 없습니다: ${REDIS_CONTAINER}"

printf '\n========================================\n'
printf ' Load test reset\n'
printf ' LOAD_TEST_ID: %s\n' "${LOAD_TEST_ID}"
printf '========================================\n'

if [[ "${FORCE:-0}" != "1" ]]; then
  printf '\n다음 데이터가 삭제됩니다.\n'
  printf '  - Kafka load-test 토픽\n'
  printf '  - MinIO 테스트 체크포인트\n'
  printf '  - MinIO 테스트 Bronze 데이터\n'
  printf '  - 로컬 Metrics 체크포인트\n'
  printf '  - Redis 전체 데이터\n\n'

  read -r -p "계속하려면 yes를 입력하세요: " answer

  [[ "${answer}" == "yes" ]] ||
    fail "초기화가 취소되었습니다."
fi

# --------------------------------------------------
# 1. Spark Streaming 중지
# --------------------------------------------------
log "Spark Streaming 테스트 서비스를 중지합니다."

docker compose \
  -f "${SPARK_COMPOSE}" \
  stop "${STREAMING_SERVICE}" || true

# --------------------------------------------------
# 2. Kafka 토픽 삭제
# --------------------------------------------------
log "Kafka 테스트 토픽을 삭제합니다."

for topic in "${TOPICS[@]}"; do
  if docker exec "${KAFKA_CONTAINER}" kafka-topics \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --describe \
    --topic "${topic}" >/dev/null 2>&1; then

    echo "삭제: ${topic}"

    docker exec "${KAFKA_CONTAINER}" kafka-topics \
      --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
      --delete \
      --topic "${topic}"
  else
    echo "이미 존재하지 않음: ${topic}"
  fi
done

# Kafka 토픽 삭제는 비동기적으로 처리될 수 있으므로 완료될 때까지 대기
log "Kafka 토픽 삭제 완료를 기다립니다."

for topic in "${TOPICS[@]}"; do
  for attempt in {1..45}; do
    if ! docker exec "${KAFKA_CONTAINER}" kafka-topics \
      --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
      --describe \
      --topic "${topic}" >/dev/null 2>&1; then
      echo "삭제 확인: ${topic}"
      break
    fi

    if [[ "${attempt}" -eq 45 ]]; then
      fail "Kafka 토픽 삭제가 완료되지 않았습니다: ${topic}"
    fi

    sleep 1
  done
done

# --------------------------------------------------
# 3. Kafka 토픽 재생성
# --------------------------------------------------
log "Kafka 테스트 토픽을 재생성합니다."

for topic in "${TOPICS[@]}"; do
  docker exec "${KAFKA_CONTAINER}" kafka-topics \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --create \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1
done

# --------------------------------------------------
# 4. MinIO 데이터 삭제
# --------------------------------------------------
log "MinIO 체크포인트를 삭제합니다."

mc rm \
  --recursive \
  --force \
  "${MINIO_ALIAS}/${MINIO_BUCKET}/checkpoints/load_test/${LOAD_TEST_ID}" \
  2>/dev/null || true

log "MinIO Bronze 테스트 데이터를 삭제합니다."

mc rm \
  --recursive \
  --force \
  "${MINIO_ALIAS}/${MINIO_BUCKET}/bronze/load_test/${LOAD_TEST_ID}" \
  2>/dev/null || true

# --------------------------------------------------
# 5. 로컬 체크포인트 삭제
# --------------------------------------------------
log "로컬 Metrics 체크포인트를 삭제합니다."

if [[ -e "${LOCAL_CHECKPOINT_PATH}" ]]; then
  sudo rm -rf "${LOCAL_CHECKPOINT_PATH}"
  echo "삭제 완료: ${LOCAL_CHECKPOINT_PATH}"
else
  echo "이미 존재하지 않음: ${LOCAL_CHECKPOINT_PATH}"
fi

# --------------------------------------------------
# 6. Redis 초기화
# --------------------------------------------------
log "Redis 데이터를 초기화합니다."

docker exec "${REDIS_CONTAINER}" redis-cli FLUSHALL

# --------------------------------------------------
# 7. Spark Streaming 재기동
# --------------------------------------------------
log "Spark Streaming 테스트 서비스를 재생성합니다."

LOAD_TEST_ID="${LOAD_TEST_ID}" \
docker compose \
  -f "${SPARK_COMPOSE}" \
  up -d \
  --force-recreate \
  "${STREAMING_SERVICE}"

# --------------------------------------------------
# 8. 상태 확인
# --------------------------------------------------
log "컨테이너 기동을 기다립니다."

sleep 5

RUNNING_STATE="$(
  docker inspect \
    --format '{{.State.Running}}' \
    "${STREAMING_CONTAINER}" 2>/dev/null || echo "false"
)"

if [[ "${RUNNING_STATE}" != "true" ]]; then
  docker logs --tail 100 "${STREAMING_CONTAINER}" || true
  fail "Spark Streaming 컨테이너가 실행 상태가 아닙니다."
fi

RESTART_COUNT="$(
  docker inspect \
    --format '{{.RestartCount}}' \
    "${STREAMING_CONTAINER}"
)"

log "적용된 환경변수를 확인합니다."

docker exec "${STREAMING_CONTAINER}" env |
  grep -E 'LOAD_TEST_ID|CHECKPOINT_BASE_PATH|METRICS_CHECKPOINT_BASE_PATH' ||
  true

printf '\n========================================\n'
printf ' 초기화 완료\n'
printf ' LOAD_TEST_ID : %s\n' "${LOAD_TEST_ID}"
printf ' Running      : %s\n' "${RUNNING_STATE}"
printf ' RestartCount : %s\n' "${RESTART_COUNT}"
printf '========================================\n'
