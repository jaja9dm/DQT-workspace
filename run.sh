#!/bin/bash
# DQT 실행 래퍼 — 어떤 머신에서도 동작 (경로 자동 감지)
# 2026-05-18: launchd/cron 환경에서 .env 못 읽어 settings EnvironmentError → exit 78 발생.
#            → .env 자동 export로 root-cause 차단.

# 이 스크립트가 있는 디렉토리를 WORKSPACE로 사용
WORKSPACE="$(cd "$(dirname "$0")" && pwd)"
VENV="$WORKSPACE/venv"
LOG="$WORKSPACE/logs/run.log"

mkdir -p "$WORKSPACE/logs"
cd "$WORKSPACE" || exit 1

echo "=== DQT start $(date) ===" >> "$LOG"
echo "WORKSPACE=$WORKSPACE" >> "$LOG"
echo "INVOKER=${USER:-unknown} PPID=$PPID" >> "$LOG"

# .env 파일을 환경 변수로 export
# launchd/cron 환경에서 dotenv 로드 실패 시 안전망 (settings.py가 os.environ에서 잡음).
# .env 파일 권한 600 권장 (chmod 600 .env).
if [ -f "$WORKSPACE/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$WORKSPACE/.env"
    set +a
    echo "ENV loaded from $WORKSPACE/.env" >> "$LOG"
else
    echo "WARNING: .env not found at $WORKSPACE/.env" >> "$LOG"
fi

# 한글 처리 (launchd 기본 C 로케일 대비)
export LANG="${LANG:-ko_KR.UTF-8}"
export LC_ALL="${LC_ALL:-ko_KR.UTF-8}"

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
