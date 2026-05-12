#!/bin/bash
# enable_launchd.sh — DQT launchd 에이전트 활성화 (부팅 시 자동 시작)
#
# 어시스턴트 모델 전환 (2026-05-12) — Phase E.
#
# 사용자 결정 후 한 번만 실행:
#   bash scripts/enable_launchd.sh
#
# 목적:
#   회사 컴퓨터가 자동 업데이트 등으로 재부팅되어도 DQT가 자동 재시작.
#   ~/Library/LaunchAgents/com.dqt.trader.plist는 이미 등록되어 있으나
#   현재 disabled 상태. 이 스크립트로 enable + bootstrap.
#
# 해제 (비활성화):
#   launchctl disable gui/$(id -u)/com.dqt.trader
#   launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.dqt.trader.plist

set -e

PLIST="$HOME/Library/LaunchAgents/com.dqt.trader.plist"
UID_NUM=$(id -u)

echo "═══════════════════════════════════════════════════════════════"
echo "  DQT launchd 에이전트 활성화"
echo "═══════════════════════════════════════════════════════════════"
echo

if [ ! -f "$PLIST" ]; then
    echo "[ERROR] $PLIST 가 없습니다."
    echo "        launchd plist를 먼저 작성해야 합니다."
    exit 1
fi

echo "[1/2] launchctl enable"
launchctl enable "gui/$UID_NUM/com.dqt.trader" || true

echo "[2/2] launchctl bootstrap"
# 이미 로드되어 있을 수 있으니 bootout 후 bootstrap (idempotent)
launchctl bootout "gui/$UID_NUM" "$PLIST" 2>/dev/null || true
launchctl bootstrap "gui/$UID_NUM" "$PLIST"

echo
echo "[상태 확인]"
launchctl print "gui/$UID_NUM/com.dqt.trader" 2>&1 | head -20 || true
echo

echo "완료. 재부팅 시 DQT가 자동 시작됩니다."
echo
echo "비활성화 시:"
echo "  launchctl disable gui/$UID_NUM/com.dqt.trader"
echo "  launchctl bootout gui/$UID_NUM $PLIST"
