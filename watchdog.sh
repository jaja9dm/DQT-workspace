#!/bin/bash
# watchdog.sh — DQT 자동 재시작 데몬
# main.py가 비정상 종료 시 자동으로 재시작. Ctrl+C 또는 SIGTERM으로 완전 종료.

WORKSPACE="$(cd "$(dirname "$0")" && pwd)"
VENV="$WORKSPACE/venv"
LOG="$WORKSPACE/logs/watchdog.log"
PIDFILE="$WORKSPACE/watchdog.pid"
MAX_RESTARTS=10          # 연속 실패 이 횟수 초과 시 중단 (무한 크래시 루프 방지)
RESTART_DELAY=15         # 재시작 전 대기 시간 (초)

mkdir -p "$WORKSPACE/logs"
cd "$WORKSPACE" || exit 1

_log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG"; }

# 중복 실행 방지
if [ -f "$PIDFILE" ]; then
    old_pid=$(cat "$PIDFILE")
    if kill -0 "$old_pid" 2>/dev/null; then
        _log "watchdog 이미 실행 중 (PID $old_pid). 종료."
        exit 1
    fi
fi
echo $$ > "$PIDFILE"

_cleanup() {
    _log "watchdog 종료 신호 수신 — DQT 프로세스 종료 중..."
    [ -n "$DQT_PID" ] && kill "$DQT_PID" 2>/dev/null
    rm -f "$PIDFILE"
    exit 0
}
trap _cleanup INT TERM

PYTHON=$(ls "$VENV/bin/python"* 2>/dev/null | grep -E 'python[0-9]' | head -1)
[ -z "$PYTHON" ] && PYTHON="$VENV/bin/python3"

export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

consecutive_fails=0
_log "=== watchdog 시작 (PID $$) ==="

while true; do
    _log "DQT 시작..."
    start_ts=$(date +%s)
    "$PYTHON" main.py >> "$WORKSPACE/logs/nohup.out" 2>&1 &
    DQT_PID=$!
    _log "DQT PID=$DQT_PID"

    wait "$DQT_PID"
    exit_code=$?
    elapsed=$(( $(date +%s) - start_ts ))

    if [ $exit_code -eq 0 ]; then
        _log "DQT 정상 종료 (exit 0) — watchdog도 종료"
        rm -f "$PIDFILE"
        exit 0
    fi

    # 300초(5분) 이상 살아있었으면 건강한 실행으로 간주 → 연속 실패 카운터 리셋
    if [ $elapsed -ge 300 ]; then
        consecutive_fails=0
    fi

    consecutive_fails=$((consecutive_fails + 1))
    _log "DQT 비정상 종료 (exit $exit_code, 가동 ${elapsed}초). 연속 실패 $consecutive_fails/${MAX_RESTARTS}"

    if [ $consecutive_fails -ge $MAX_RESTARTS ]; then
        _log "연속 실패 $MAX_RESTARTS 회 초과 — watchdog 중단 (수동 확인 필요)"
        rm -f "$PIDFILE"
        exit 1
    fi

    _log "${RESTART_DELAY}초 후 재시작..."
    sleep $RESTART_DELAY
done
