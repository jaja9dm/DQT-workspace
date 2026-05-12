#!/bin/bash
# setup_macos.sh — DQT 시스템용 macOS 전원/잠자기 설정
#
# 어시스턴트 모델 전환 (2026-05-12) — Phase D.
#
# 사용자가 한 번만 실행 (sudo 필요):
#   bash scripts/setup_macos.sh
#
# 목적:
#   매일 24/7 가동되는 DQT가 매크 자동 잠자기로 멈추지 않도록 강제.
#   디스플레이만 30분 후 꺼서 전력은 절약.
#
# 주의:
#   회사 정책으로 pmset이 잠긴 경우 (MDM) sudo 실행 자체가 차단될 수 있음.
#   그 경우엔 시스템 환경설정 > 배터리에서 수동 설정 필요.

set -e

echo "═══════════════════════════════════════════════════════════════"
echo "  DQT macOS 전원/잠자기 설정"
echo "═══════════════════════════════════════════════════════════════"
echo

# 현재 설정 출력
echo "[현재 설정]"
pmset -g | grep -E "sleep|disksleep|displaysleep" || true
echo

# 적용
echo "[변경 사항 적용]"
echo "  - sleep 0          (전원 연결 시 시스템 잠자기 비활성화)"
echo "  - disksleep 0      (디스크 잠자기 비활성화)"
echo "  - displaysleep 30  (디스플레이 30분 후 꺼짐 — 전력 절약)"
echo

sudo pmset -c sleep 0
sudo pmset -c disksleep 0
sudo pmset -c displaysleep 30

echo
echo "[변경 후 설정]"
pmset -g | grep -E "sleep|disksleep|displaysleep" || true
echo
echo "완료. 시스템이 자동 잠자기 없이 24/7 가동됩니다."
echo "디스플레이만 30분 후 꺼지며, 시스템·디스크·네트워크는 항상 활성."
