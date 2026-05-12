"""
seed_learnings.py — 초기 learnings 시드 1회 INSERT.

어시스턴트 모델 전환 (2026-05-12) — Phase 7.

최근 14거래일 trade_review.improvements 중 5/7개 review에서 추출한 핵심 교훈을
learnings 테이블에 INSERT.

실행:
    venv/bin/python -m scripts.seed_learnings
또는
    venv/bin/python scripts/seed_learnings.py

이미 동일 content가 있으면 스킵 (idempotent).
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

# 프로젝트 루트 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.infra.database import execute, fetch_all, fetch_one  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


_SEED_LESSONS: list[dict] = [
    {
        "category": "avoid",
        "content": "RSI 80 이상 종목 매수 금지 — 한온시스템·미래에셋증권 등 과열 구간 진입은 즉시 손절 반복. 풀사이즈 진입 시 RSI ≤ 68 권장.",
        "confidence": 0.8,
        "times_validated": 2,
        "evidence": "trade_review 2026-05-06 / 2026-04-30",
    },
    {
        "category": "risk",
        "content": "연속 손절 2회 이후 30분간 신규 진입 차단 (쿨다운). 3회 시 당일 매매 종료. 손절 누적 패턴은 시장과 안 맞는 신호.",
        "confidence": 0.7,
        "times_validated": 1,
        "evidence": "trade_review 2026-05-07",
    },
    {
        "category": "entry_timing",
        "content": "갭업 +5% 이상 종목은 시가 추격 금지. 09:30 이후 첫 눌림 확인 후 진입. 갭업돌파 신호는 15분 대기 규칙 적용.",
        "confidence": 0.75,
        "times_validated": 2,
        "evidence": "trade_review 2026-05-07 (삼성전자), 2026-05-11 (분할매수 5분 룰)",
    },
    {
        "category": "macro",
        "content": "KOSPI +3% 이상이나 KOSDAQ 0% 미만 혼조장에서는 KOSDAQ 종목 진입 금지. 시장 괴리는 KOSDAQ 추격 매수의 가장 큰 함정.",
        "confidence": 0.7,
        "times_validated": 1,
        "evidence": "trade_review 2026-05-11",
    },
    {
        "category": "macro",
        "content": "KOSDAQ 지수 -0.5% 이하 날에는 KOSDAQ 종목 진입 차단 또는 점수 임계값 +10pt 상향. 에코프로 손실 재발 방지.",
        "confidence": 0.65,
        "times_validated": 1,
        "evidence": "trade_review 2026-05-07 (에코프로)",
    },
    {
        "category": "pattern",
        "content": "진입 후 5분 내 +0.3% 미달 종목은 조기 청산 — 평균 보유 14분 중 초반 방향성이 결정적. 손절 트리거 -1.5%로 강화 가능.",
        "confidence": 0.6,
        "times_validated": 1,
        "evidence": "trade_review 2026-05-11",
    },
    {
        "category": "sector",
        "content": "강세 섹터(외인 매수 + 평균 +0.5% 이상) 종목만 진입. 섹터 ETF 또는 동종 3종목 평균이 음(-)이면 종목 단독 진입 금지.",
        "confidence": 0.65,
        "times_validated": 1,
        "evidence": "trade_review 2026-05-11",
    },
    {
        "category": "risk",
        "content": "보유 시간 90분 초과 시 강제 time_cut. 단타 전략에서 90분 넘긴 종목은 의도 이탈로 분류. 14:50보다 빨라도 청산.",
        "confidence": 0.6,
        "times_validated": 1,
        "evidence": "trade_review 2026-04-30",
    },
    {
        "category": "macro",
        "content": "시장 점수 -0.3 이하 또는 KOSPI -1.0% 초과 하락 시 breakout/momentum 신호 차단. pullback_rebound/opening_plunge_rebound 신호만 허용.",
        "confidence": 0.65,
        "times_validated": 1,
        "evidence": "trade_review 2026-04-30",
    },
    {
        "category": "sector",
        "content": "KOSPI +3% 이상 강세장에서는 증권·금융·대형주 가중치 +10점, 자동차부품·중소형 -10점. 강세장은 시총 큰 주가 끌고 간다.",
        "confidence": 0.55,
        "times_validated": 1,
        "evidence": "trade_review 2026-05-06",
    },
]


def seed_learnings() -> int:
    """초기 lessons INSERT. 이미 동일 content 있으면 스킵.
    Returns: 실제 INSERT된 개수.
    """
    today = date.today().isoformat()
    inserted = 0
    for ls in _SEED_LESSONS:
        # 중복 체크 (content 앞 30자 기준)
        head = ls["content"][:30]
        exist = fetch_one(
            "SELECT id FROM learnings WHERE content LIKE ? LIMIT 1",
            (head + "%",),
        )
        if exist:
            logger.debug(f"[seed] 이미 존재 — 스킵: {head}...")
            continue
        try:
            execute(
                """
                INSERT INTO learnings (
                    discovered_at, category, content, evidence,
                    confidence, times_validated, times_failed, status
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 'active')
                """,
                (
                    today,
                    ls["category"],
                    ls["content"],
                    json.dumps(
                        [{"date": today, "observation": ls.get("evidence", "")}],
                        ensure_ascii=False,
                    ),
                    float(ls["confidence"]),
                    int(ls.get("times_validated", 0)),
                ),
            )
            inserted += 1
            logger.info(f"[seed] INSERT — [{ls['category']}] {head}...")
        except Exception as e:
            logger.warning(f"[seed] INSERT 실패: {e}")
    return inserted


if __name__ == "__main__":
    n = seed_learnings()
    total = fetch_one("SELECT COUNT(*) AS cnt FROM learnings")["cnt"]
    print(f"INSERT: {n}건 / 전체 learnings: {total}건")
