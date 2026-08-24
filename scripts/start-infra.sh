#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

echo "============================================================"
echo " Starting Ride-Hailing Local Infrastructure (Kafka & Redis) "
echo "============================================================"

# 1. Ensure Docker Desktop daemon is running
ensure_docker_daemon() {
  echo "[1/4] Checking Docker daemon status..."
  if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon is not responsive. Attempting to start Docker Desktop..."
    if [ -d "/Applications/Docker.app" ]; then
      open -g -a Docker || open -g -a /Applications/Docker.app
    else
      echo "ERROR: /Applications/Docker.app not found. Please start Docker manually." >&2
      exit 1
    fi

    local elapsed=0
    local timeout=60
    echo "Waiting for Docker daemon to become ready (up to ${timeout}s)..."
    until docker info >/dev/null 2>&1; do
      if [ "$elapsed" -ge "$timeout" ]; then
        echo "ERROR: Docker daemon failed to start within ${timeout} seconds." >&2
        exit 1
      fi
      sleep 2
      elapsed=$((elapsed + 2))
      echo "  Still waiting for Docker... (${elapsed}s elapsed)"
    done
    echo "Docker daemon is ready (${elapsed}s elapsed)."
  else
    echo "Docker daemon is running."
  fi
}

ensure_docker_daemon

# 2. Check for port conflicts on host (ports 9092 and 6379)
echo "[2/4] Validating host port availability..."
for port in 9092 6379; do
  if lsof -iTCP:"${port}" -sTCP:LISTEN -n -P >/dev/null 2>&1; then
    OCCUPIED_BY=$(lsof -iTCP:"${port}" -sTCP:LISTEN -n -P | tail -n +2 | awk '{print $1}' | head -n 1)
    echo "Note: Port ${port} is currently bound by ${OCCUPIED_BY}."
  else
    echo "Port ${port} is available."
  fi
done

# 3. Start Docker Compose with health wait
echo "[3/4] Starting containers via Docker Compose..."
docker compose up -d --wait --wait-timeout 60

echo "Waiting for init-kafka to finish topic provisioning..."
docker wait init-kafka >/dev/null 2>&1 || true

# 4. Verification Suite
echo "[4/4] Executing Infrastructure Verification Suite..."

# 4a. Container Statuses
echo "--> Checking container lifecycle statuses:"
docker compose ps -a

KAFKA_STATUS=$(docker inspect kafka --format '{{.State.Health.Status}}' 2>/dev/null || echo "unknown")
REDIS_STATUS=$(docker inspect redis --format '{{.State.Health.Status}}' 2>/dev/null || echo "unknown")
INIT_STATUS=$(docker inspect init-kafka --format '{{.State.ExitCode}}' 2>/dev/null || echo "unknown")

if [ "$KAFKA_STATUS" != "healthy" ]; then
  echo "FAIL: Kafka container health status is '${KAFKA_STATUS}', expected 'healthy'." >&2
  exit 1
fi
echo "PASS: Kafka is healthy."

if [ "$REDIS_STATUS" != "healthy" ]; then
  echo "FAIL: Redis container health status is '${REDIS_STATUS}', expected 'healthy'." >&2
  exit 1
fi
echo "PASS: Redis is healthy."

if [ "$INIT_STATUS" != "0" ]; then
  echo "FAIL: init-kafka exit code is '${INIT_STATUS}', expected '0'." >&2
  exit 1
fi
echo "PASS: init-kafka completed with exit code 0."

# 4b. Redis PING verification
echo "--> Verifying Redis responsiveness:"
REDIS_PING=$(docker compose exec -T redis redis-cli ping | tr -d '\r')
if [ "$REDIS_PING" != "PONG" ]; then
  echo "FAIL: Redis ping returned '${REDIS_PING}', expected 'PONG'." >&2
  exit 1
fi
echo "PASS: Redis responded with PONG."

# 4c. Kafka Topics Verification
echo "--> Verifying mandatory Kafka topics exist:"
TOPICS_OUTPUT=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)

MANDATORY_TOPICS=("driver-locations" "ride-requests" "ride-matches")
for topic in "${MANDATORY_TOPICS[@]}"; do
  if echo "$TOPICS_OUTPUT" | grep -qx "$topic"; then
    echo "PASS: Topic '${topic}' exists."
  else
    echo "FAIL: Mandatory topic '${topic}' is missing from Kafka." >&2
    exit 1
  fi
done

# 4d. Kafka Topic Partition and Replication Factor Verification
echo "--> Verifying partition counts and replication factors:"
for topic in "${MANDATORY_TOPICS[@]}"; do
  DESCRIBE_OUTPUT=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$topic")
  if echo "$DESCRIBE_OUTPUT" | grep -q "PartitionCount: 1" && echo "$DESCRIBE_OUTPUT" | grep -q "ReplicationFactor: 1"; then
    echo "PASS: Topic '${topic}' has PartitionCount=1 and ReplicationFactor=1."
  else
    echo "FAIL: Topic '${topic}' does not match expected partition/replication spec:" >&2
    echo "$DESCRIBE_OUTPUT" >&2
    exit 1
  fi
done

# 4e. End-to-End Produce and Consume Smoke Test
echo "--> Executing end-to-end Kafka produce & consume smoke test:"
TEST_PAYLOAD='{"driverId":"verify_probe_01","latitude":37.7749,"longitude":-122.4194,"status":"AVAILABLE","bearing":90.0,"timestamp":1718000000000}'

echo "$TEST_PAYLOAD" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic driver-locations

CONSUMED_MESSAGE=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000 | grep "verify_probe_01" | head -n 1 | tr -d '\r')

if [ -z "$CONSUMED_MESSAGE" ]; then
  echo "FAIL: No message consumed containing verify_probe_01 from driver-locations within timeout." >&2
  exit 1
fi
echo "PASS: Successfully produced and consumed event from driver-locations: ${CONSUMED_MESSAGE}"

echo "============================================================"
echo " Infrastructure initialization and verification SUCCESSFUL! "
echo "============================================================"
exit 0
