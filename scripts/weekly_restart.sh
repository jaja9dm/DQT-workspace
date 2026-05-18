#!/bin/bash
# DQT 주간 재시작 — 메모리 누수 청소 (매주 일요일 03:00)
# cron으로 실행. ~/Documents 외부 (~/dqt_auto/)라 TCC 우회.

WORKSPACE="/Users/dongmin.jung/Documents/DQT-workspace"
PIDFILE="$WORKSPACE/dqt.pid"
LOGFILE="/Users/dongmin.jung/dqt_auto/weekly_restart.log"

echo "$(date '+%F %T') ===== 주간 재시작 시작 =====" >> "$LOGFILE"

# 1) 기존 프로세스 종료
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "$(date '+%F %T') 기존 PID $OLD_PID 종료 시도" >> "$LOGFILE"
        kill -TERM "$OLD_PID" 2>&1 >> "$LOGFILE"
        sleep 8
        # SIGTERM 무시면 SIGKILL
        if kill -0 "$OLD_PID" 2>/dev/null; then
            kill -KILL "$OLD_PID" 2>&1 >> "$LOGFILE"
            sleep 2
        fi
    fi
fi

# 2) 신규 시작
cd "$WORKSPACE" || exit 1
nohup bash run.sh > logs/stdout.log 2>&1 &
disown
NEW_PID=$!
echo "$(date '+%F %T') 신규 시작 PID $NEW_PID" >> "$LOGFILE"

sleep 5
if kill -0 "$NEW_PID" 2>/dev/null; then
    echo "$(date '+%F %T') ✅ 재시작 성공 (PID $NEW_PID 살아있음)" >> "$LOGFILE"
else
    echo "$(date '+%F %T') ❌ 재시작 실패!" >> "$LOGFILE"
fi
echo "" >> "$LOGFILE"
