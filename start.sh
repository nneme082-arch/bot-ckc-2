#!/usr/bin/env sh
set -eu

TARGET=""

if [ -f "./run_with_mailing.py" ]; then
  TARGET="./run_with_mailing.py"
elif [ -f "./new-chat/run_with_mailing.py" ]; then
  TARGET="./new-chat/run_with_mailing.py"
else
  TARGET="$(find /app -maxdepth 6 -name run_with_mailing.py | head -n 1 || true)"
fi

if [ -z "$TARGET" ]; then
  echo "ERROR: run_with_mailing.py not found."
  echo "Files under /app (first 200):"
  find /app -maxdepth 5 -type f | head -n 200
  exit 1
fi

echo "Starting bot with: $TARGET"
exec python -u "$TARGET"
