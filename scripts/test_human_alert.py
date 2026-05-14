"""테스트 — 사람 개입 알림 모듈 검증.

이 스크립트는 시뮬레이션 모드로 동작.
실제 텔레그램 발송을 막으려면 monkeypatch로 notify를 가짜 함수로 교체.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.infra.database import execute, fetch_all, fetch_one
from src.utils import human_alert as ha
from src.utils.logger import get_logger

logger = get_logger("test_human_alert")


# ── 텔레그램 모킹 ──────────────────────────────────────────
_sent_messages: list[str] = []


def _fake_notify(text: str, parse_mode: str = "HTML") -> bool:
    _sent_messages.append(text)
    print(f"━━━ MOCK TELEGRAM SEND ({len(text)}자) ━━━")
    print(text[:600] + ("..." if len(text) > 600 else ""))
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return True


# notify 함수를 가짜로 교체
ha.notify = _fake_notify


def _clear_alerts():
    """system_alerts 테이블 정리 (테스트 격리)."""
    execute("DELETE FROM system_alerts WHERE category LIKE 'test_%' OR dedup_key LIKE 'TEST_%'")


def test_a_self_contradiction():
    """시나리오 A — 자기 모순 알림 발송."""
    print("\n=== 시나리오 A: 자기 모순 / 검토 요청 ===")
    _sent_messages.clear()
    ok = ha.send_human_alert(
        ha.AlertSeverity.URGENT,
        title="검토 요청 — 평가 기준 모순 (TEST)",
        body=(
            "사유:\n"
            "confidence=2 추천 종목이 +5% 급등 시 '실패'로 분류되는 것은 비논리적.\n"
            "회피 정답(약하락 예측)이었으나 강상승했다면 신뢰도 미스로 '부분 적중' 분류가 옳음.\n\n"
            "제안:\n"
            "_classify_pick() 함수의 partial 기준 재정의 — conf<=2 + 강상승은 'partial(신뢰도 미스)'\n\n"
            "이는 evening_review.py에 이미 반영되어 있으나, 실제 적용된 후 결과 재검증 권장."
        ),
        category="test_self_contradiction",
        dedup_key="TEST_self_contradiction_A",
    )
    assert ok, "첫 발송은 성공해야 함"
    print(f"✅ 첫 발송: {'성공' if ok else '실패'}")

    # dedup 차단 확인
    ok2 = ha.send_human_alert(
        ha.AlertSeverity.URGENT,
        title="검토 요청 — 평가 기준 모순 (DUP)",
        body="중복 발송 테스트",
        category="test_self_contradiction",
        dedup_key="TEST_self_contradiction_A",
    )
    assert not ok2, "같은 dedup_key는 24h 내 차단되어야 함"
    print(f"✅ dedup 차단: {'성공' if not ok2 else '실패'}")

    # DB 확인
    rows = fetch_all(
        "SELECT severity, category, title, dedup_key FROM system_alerts "
        "WHERE category = 'test_self_contradiction' ORDER BY id DESC"
    )
    print(f"✅ DB 기록: {len(rows)}건 — 발송 1 + 차단 1 = 2건 예상")
    for r in rows:
        print(f"   - [{r['severity']}] {r['title'][:50]}")
    assert len(rows) >= 1


def test_b_data_failure_simulation():
    """시나리오 B — 데이터 수집 실패 점검.

    실제 DB의 daily_news 등 상태를 점검한다. 환경에 따라 결과가 달라짐.
    여기서는 함수가 에러 없이 실행되는지, 그리고 알림 dict 구조가 올바른지만 검증.
    """
    print("\n=== 시나리오 B: 데이터 수집 실패 점검 ===")
    issues = ha.check_data_health()
    print(f"감지된 데이터 이슈: {len(issues)}건")
    for issue in issues:
        print(f"  - [{issue['severity']}] {issue['title']}")
        assert "title" in issue
        assert "body" in issue
        assert "category" in issue
        assert "dedup_key" in issue
        assert issue["category"] == "data_failure"
    print("✅ check_data_health() 정상 동작")


def test_b_simulated_data_failure():
    """시나리오 B-2 — 강제 데이터 실패 알림 발송 (구조 검증)."""
    print("\n=== 시나리오 B-2: 시뮬레이션 데이터 실패 알림 ===")
    ok = ha.send_human_alert(
        ha.AlertSeverity.URGENT,
        title="뉴스 수집 3일 연속 실패 (TEST)",
        body=(
            "테이블: daily_news\n"
            "증상: 2026-05-12~05-14 3거래일 동안 row 5개 미만.\n\n"
            "권장 조치:\n"
            "  1. logs/dqt.log에서 news_collector 잡 에러 로그 확인\n"
            "  2. RSS 피드 가용성 확인 (네이버/Yahoo/Reuters)\n"
            "  3. 필요 시 수동 백필 또는 잡 재실행"
        ),
        category="test_data_failure",
        dedup_key="TEST_data_failure_B",
    )
    print(f"✅ 시뮬레이션 알림: {'발송' if ok else '실패'}")


def test_e_ops_health():
    """시나리오 E — 운영 안전망 점검."""
    print("\n=== 시나리오 E: 운영 안전망 점검 ===")
    issues = ha.check_ops_health()
    print(f"감지된 운영 이슈: {len(issues)}건")
    for issue in issues:
        print(f"  - [{issue['severity']}] {issue['title']}")
        assert issue["category"] == "ops"
    print("✅ check_ops_health() 정상 동작")


def test_e_simulated_ops_alert():
    """시나리오 E-2 — 강제 운영 알림 발송 (구조 검증)."""
    print("\n=== 시나리오 E-2: 시뮬레이션 운영 알림 (DB 크기) ===")
    ok = ha.send_human_alert(
        ha.AlertSeverity.OPS,
        title="DB 크기 120.5MB 초과 (한도 100MB) (TEST)",
        body=(
            "파일: /Users/dongmin.jung/Documents/DQT-workspace/db/dqt.db\n"
            "현재 크기: 120.5MB\n\n"
            "권장 조치:\n"
            "  - 오래된 intraday_candles/fetch_checkpoint VACUUM\n"
            "  - 백업 후 일부 테이블 정리"
        ),
        category="test_ops",
        dedup_key="TEST_ops_E",
    )
    print(f"✅ 시뮬레이션 알림: {'발송' if ok else '실패'}")


def test_full_health_check():
    """통합 진입점 — run_health_checks() 실행."""
    print("\n=== 통합: run_health_checks() ===")
    summary = ha.run_health_checks()
    print(f"점검 결과: {summary}")
    assert "data_issues" in summary
    assert "ops_issues" in summary
    assert "alerts_sent" in summary
    assert "alerts_blocked" in summary
    print("✅ 통합 진입점 정상 동작")


if __name__ == "__main__":
    print("=" * 60)
    print("DQT human_alert 시뮬레이션 테스트")
    print("=" * 60)

    _clear_alerts()

    test_a_self_contradiction()
    test_b_data_failure_simulation()
    test_b_simulated_data_failure()
    test_e_ops_health()
    test_e_simulated_ops_alert()
    test_full_health_check()

    # 정리
    _clear_alerts()
    print("\n" + "=" * 60)
    print(f"테스트 모두 통과 — 시뮬레이션 발송 메시지 {len(_sent_messages)}건")
    print("=" * 60)
