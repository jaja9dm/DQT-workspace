#!/bin/bash
# DQT cron 백업 — launchd가 실패할 경우 대비
# 평일 07:05 실행. 이미 가동 중이면 종료 (main.py 자체 중복 검사).
WORKSPACE="/Users/dongmin.jung/Documents/DQT-workspace"
PIDFILE="$WORKSPACE/dqt.pid"

# 이미 가동 중이면 종료
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "$(date '+%F %T') 이미 가동 중 (PID $OLD_PID) — cron 스킵"
        exit 0
    fi
fi

# 가동 안 됨 → 시작
cd "$WORKSPACE" || exit 1
echo "$(date '+%F %T') launchd 실패 감지 — cron으로 백업 시작"
nohup bash run.sh > logs/stdout.log 2>&1 &
disown
echo "$(date '+%F %T') cron 백업 시작 완료 (PID $!)"
