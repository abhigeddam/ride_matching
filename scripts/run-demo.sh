#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DIR"

if [ -f "$DIR/simulation/.venv/bin/python3" ]; then
    PYTHON_BIN="$DIR/simulation/.venv/bin/python3"
else
    PYTHON_BIN="python3"
fi

"$PYTHON_BIN" "$DIR/scripts/demo-simulation.py"
