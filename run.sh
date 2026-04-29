#!/bin/bash
# DQT 실행 래퍼 — launchd가 직접 호출

WORKSPACE=/Users/dongmin.jung/Documents/DQT-workspace
VENV=$WORKSPACE/venv
PYTHON=/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3.9
LOG=$WORKSPACE/logs/run.log

cd "$WORKSPACE" || exit 78

# 기동 시 환경 기록 (launchd 디버깅용)
echo "=== DQT start $(date) ===" >> "$LOG"
echo "PYTHON=$PYTHON exists=$(test -f $PYTHON && echo yes || echo no)" >> "$LOG"
echo "VENV=$VENV" >> "$LOG"

# venv site-packages를 명시적으로 PYTHONPATH에 추가
export PYTHONPATH="$VENV/lib/python3.9/site-packages:$PYTHONPATH"
export VIRTUAL_ENV="$VENV"
export PATH="$VENV/bin:$PATH"

echo "exec $PYTHON main.py" >> "$LOG"
exec "$PYTHON" main.py "$@"
