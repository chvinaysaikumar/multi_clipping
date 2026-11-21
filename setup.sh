#!/usr/bin/env bash
# One-time setup for Windows (Git Bash), macOS and Linux:
# finds Python, checks ffmpeg, creates .venv, installs dependencies, runs the doctor.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PY=""
for c in python3 python py; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c 'import sys; sys.exit(sys.version_info < (3, 9))' 2>/dev/null; then
    PY="$c"; break
  fi
done
if [[ -z "$PY" ]]; then
  echo "Python 3.9+ not found. Install it from https://www.python.org/downloads/ (tick 'Add to PATH' on Windows)."
  exit 1
fi

if ! command -v ffmpeg >/dev/null 2>&1 || ! command -v ffprobe >/dev/null 2>&1; then
  echo "ffmpeg is missing. Install it, then run setup.sh again:"
  echo "  Windows: winget install ffmpeg   (then open a NEW Git Bash window)"
  echo "  macOS:   brew install ffmpeg"
  echo "  Ubuntu:  sudo apt install ffmpeg"
  exit 1
fi

echo "Using $($PY --version) and $(command -v ffmpeg)"
[[ -d .venv ]] || "$PY" -m venv .venv

if [[ -f .venv/Scripts/python.exe ]]; then VPY=".venv/Scripts/python.exe"; else VPY=".venv/bin/python"; fi
"$VPY" -m pip install --upgrade pip
"$VPY" -m pip install -r requirements.txt

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env - open it and paste your API key if you want AI titles (optional)."
fi

echo
"$VPY" clipwatcher.py --doctor || true
echo
echo "Next:  ./watch.sh --selftest   then   ./watch.sh <channel-or-youtube-url>"
