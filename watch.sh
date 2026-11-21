#!/usr/bin/env bash
# Clip Watcher launcher
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find the virtual environment's Python
if [[ -f "$HERE/.venv/Scripts/python.exe" ]]; then
    PYTHON="$HERE/.venv/Scripts/python.exe"
elif [[ -f "$HERE/.venv/bin/python" ]]; then
    PYTHON="$HERE/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
    PYTHON="$(command -v python)"
else
    echo "Python not found. Install Python first."
    exit 1
fi

# Put the venv's programs (streamlink, whisper, ...) on PATH, like activating it would
for d in "$HERE/.venv/Scripts" "$HERE/.venv/bin"; do
    [[ -d "$d" ]] && PATH="$d:$PATH"
done
export PATH

# Launch Clip Watcher
exec "$PYTHON" "$HERE/clipwatcher.py" "$@"
