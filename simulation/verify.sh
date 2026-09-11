#!/usr/bin/env bash
# ==============================================================================
# simulation/verify.sh
#
# Executable verification wrapper & dependency bootstrapper for the
# Real-Time Ride-Hailing Driver-Location Ingestion & Proximity Dispatch System.
#
# Usage:
#   ./simulation/verify.sh                   # Run verification suite (auto-probes live or mock)
#   ./simulation/verify.sh --dry-run         # Run self-contained verification
#   ./simulation/verify.sh --mode fleet      # Run 15-driver continuous simulation
#   ./simulation/verify.sh --scenario S1     # Run specific Tier 4 scenario
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python3"

# 1. Virtual environment & dependency bootstrap
if [ ! -f "${VENV_PYTHON}" ]; then
    echo "[*] Creating isolated Python virtual environment at ${VENV_DIR}..."
    python3 -m venv "${VENV_DIR}"
    "${VENV_DIR}/bin/pip" install --upgrade pip --quiet
    echo "[*] Installing required dependencies (h3, redis, kafka-python-ng)..."
    "${VENV_DIR}/bin/pip" install redis kafka-python-ng h3 --quiet
    echo "[+] Virtual environment bootstrap complete."
fi

# 2. Execute verification harness
exec "${VENV_PYTHON}" "${SCRIPT_DIR}/simulate_and_verify.py" "$@"
