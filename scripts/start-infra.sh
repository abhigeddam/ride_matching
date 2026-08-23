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
