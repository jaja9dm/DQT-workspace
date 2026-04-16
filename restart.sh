#!/bin/bash
# DQT 재시작 스크립트 — 깔끔하게 종료 후 재시작

WORKSPACE=/Users/dongmin.jung/Documents/DQT-workspace
PID_FILE=$WORKSPACE/dqt.pid
LOG=$WORKSPACE/logs/dqt.log

cd "$WORKSPACE"

# 1. 기존 프로세스 종료
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "기존 프로세스 종료 중 (PID $OLD_PID)..."
        kill "$OLD_PID"
        # 최대 10초 대기
        for i in $(seq 1 10); do
            sleep 1
            kill -0 "$OLD_PID" 2>/dev/null || break
            echo "  대기 중... ($i초)"
        done
        # 아직 살아있으면 강제 종료
        if kill -0 "$OLD_PID" 2>/dev/null; then
            echo "강제 종료 (SIGKILL)..."
            kill -9 "$OLD_PID" 2>/dev/null
            sleep 1
        fi
    fi
    rm -f "$PID_FILE"
fi

# 혹시 남은 main.py 프로세스 정리
REMAINING=$(pgrep -f "python.*main.py" 2>/dev/null)
if [ -n "$REMAINING" ]; then
    echo "잔여 프로세스 정리: $REMAINING"
    kill -9 $REMAINING 2>/dev/null
    sleep 1
fi

# 2. 재시작
echo "DQT 시작 중..."
bash "$WORKSPACE/run.sh" "$@" >> "$WORKSPACE/logs/stdout.log" 2>&1 &
NEW_PID=$!
echo "DQT 시작 완료 (PID $NEW_PID)"
