#!/bin/bash
# DQT 실행 래퍼 — 어떤 머신에서도 동작 (경로 자동 감지)

# 이 스크립트가 있는 디렉토리를 WORKSPACE로 사용
WORKSPACE="$(cd "$(dirname "$0")" && pwd)"
VENV="$WORKSPACE/venv"
LOG="$WORKSPACE/logs/run.log"

mkdir -p "$WORKSPACE/logs"
cd "$WORKSPACE" || exit 1

echo "=== DQT start $(date) ===" >> "$LOG"
echo "WORKSPACE=$WORKSPACE" >> "$LOG"

# venv 존재 확인
if [ ! -d "$VENV" ]; then
    echo "ERROR: venv not found at $VENV" >> "$LOG"
    exit 1
fi

# venv의 python 자동 감지 (python3.x 버전 무관)
PYTHON=$(ls "$VENV/bin/python"* 2>/dev/null | grep -E 'python[0-9]' | head -1)
[ -z "$PYTHON" ] && PYTHON="$VENV/bin/python3"
[ -z "$PYTHON" ] && PYTHON="$VENV/bin/python"

echo "PYTHON=$PYTHON" >> "$LOG"

# venv 환경 활성화
PYTHON_VERSION=$(ls "$VENV/lib/" 2>/dev/null | grep "^python" | head -1)
export PYTHONPATH="$VENV/lib/$PYTHON_VERSION/site-packages:$PYTHONPATH"
export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

echo "exec python main.py" >> "$LOG"
exec "$PYTHON" main.py "$@"
