"""
pick_scorer.py — 종목 평가 정량 점수화 (2026-05-18)

[배경]
  morning_brief의 confidence (1~5)는 Claude 주관 평가에 의존 → 매일 기준이 흔들림.
  4가지 정량 요소(거래대금 모멘텀/외인기관 수급/기술적 신호/섹터 동조성)를
  각 0~25점, 합계 0~100점으로 산출하고 confidence에 매핑.

[데이터 소스]
  - daily_top_value: 최근 7일치 거래대금/수급/등락
  - technical_analysis.analyze_ticker(): RSI/MA/MACD (FDR 일봉 60일)
  - 동일 sector 종목들의 어제 등락 (섹터 동조성)

[핵심 함수]
  score_pick(ticker, current_price, recent_top, sector_peers, market_regime) -> dict
"""

from __future__ import annotations

from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


# ── 구성 ─────────────────────────────────────────────────────
_MAX_COMPONENT = 25            # 각 요소 만점
_MAX_TOTAL = 100               # 합계 만점

# confidence 매핑 (점수 → 별 개수)
_CONFIDENCE_BANDS = [
    (85, 5),
    (70, 4),
    (55, 3),
    (40, 2),
    (0,  1),
]


def _score_to_confidence(score: float) -> int:
    for threshold, conf in _CONFIDENCE_BANDS:
        if score >= threshold:
            return conf
    return 1


# ── A. 거래대금 모멘텀 (0~25) ──────────────────────────────────
def _score_volume_momentum(ticker: str, recent_top: list[dict]) -> tuple[int, str]:
    """어제 거래대금 / 5일 평균 거래대금 비율 기반 점수.

    Returns: (점수, 설명문)
    """
    rows = sorted(
        (r for r in recent_top if r.get("ticker") == ticker and r.get("trading_value")),
        key=lambda x: str(x.get("date")),
        reverse=True,
    )
    if not rows:
        return 0, "거래대금 데이터 없음"

    latest_value = float(rows[0]["trading_value"] or 0)
    if latest_value <= 0:
        return 0, "최신 거래대금 0"

    prior = rows[1:6]    # 어제 제외 직전 5거래일
    if len(prior) < 2:
        # 데이터 부족 — 중립 점수
        return 12, f"비교 표본 부족 ({len(prior)}일), 중립 12점"

    avg_prior = sum(float(r["trading_value"] or 0) for r in prior) / len(prior)
    if avg_prior <= 0:
        return 12, "직전 평균 0, 중립 12점"

    ratio = latest_value / avg_prior

    if ratio >= 1.5:
        return 25, f"거래대금 {ratio:.2f}배 (5일 평균 대비 폭증)"
    if ratio >= 1.2:
        return 20, f"거래대금 {ratio:.2f}배 (5일 평균 대비 증가)"
    if ratio >= 1.0:
        return 15, f"거래대금 {ratio:.2f}배 (5일 평균 수준)"
    if ratio >= 0.8:
        return 10, f"거래대금 {ratio:.2f}배 (5일 평균 미만)"
    return 5, f"거래대금 {ratio:.2f}배 (급감)"


# ── B. 외인/기관 수급 (0~25) ──────────────────────────────────
def _score_capital_flow(ticker: str, recent_top: list[dict]) -> tuple[int, str]:
    """외인/기관 누적 수급 + 연속 매수 + 동시 매수 가산."""
    rows = sorted(
        (r for r in recent_top if r.get("ticker") == ticker),
        key=lambda x: str(x.get("date")),
        reverse=True,
    )[:5]
    if not rows:
        return 0, "수급 데이터 없음"

    foreign_nets = [float(r.get("foreign_net_buy") or 0) for r in rows]
    inst_nets = [float(r.get("inst_net_buy") or 0) for r in rows]

    foreign_sum = sum(foreign_nets)
    inst_sum = sum(inst_nets)

    # 어제 거래대금 대비 외인 5일 누적 절대값 비율 (%)
    latest_value = float(rows[0].get("trading_value") or 0)
    if latest_value <= 0:
        return 0, "거래대금 0"

    # foreign_net_buy 단위는 daily_top_value의 KIS 원시값(억) 가정 (실데이터는 매우 작아 거의 0)
    # 절대값/거래대금 비율을 0~10점으로 환산 (스케일 영향 적은 부분 점수)
    foreign_ratio = abs(foreign_sum) / latest_value * 100 if latest_value > 0 else 0.0

    base = 0
    parts: list[str] = []

    # 1) 외인 5일 연속 매수 → +10
    foreign_streak = 0
    for v in foreign_nets:
        if v > 0:
            foreign_streak += 1
        else:
            break
    if foreign_streak >= 3:
        base += 10
        parts.append(f"외인 {foreign_streak}일 연속 매수 +10")
    elif foreign_streak >= 1 and foreign_sum > 0:
        base += 5
        parts.append(f"외인 5일 누적 매수 +5")

    # 2) 외인 + 기관 동시 매수 (5일 합) → +10
    if foreign_sum > 0 and inst_sum > 0:
        base += 10
        parts.append("외인+기관 동시 매수 +10")
    elif foreign_sum > 0 or inst_sum > 0:
        base += 3
        parts.append("외인/기관 한쪽 매수 +3")

    # 3) 순매수 절대값 비율 → 최대 +5
    if foreign_ratio >= 1.0:
        base += 5
        parts.append(f"외인 누적/거래대금 {foreign_ratio:.2f}% +5")
    elif foreign_ratio >= 0.3:
        base += 3
        parts.append(f"외인 누적/거래대금 {foreign_ratio:.2f}% +3")

    # 매도 우세 시 감점 (음수 합)
    if foreign_sum < 0 and inst_sum < 0:
        base = max(0, base - 5)
        parts.append("외인+기관 동시 매도 -5")

    score = min(_MAX_COMPONENT, base)
    if not parts:
        parts.append("수급 중립 (변화 미미)")
    return score, " · ".join(parts)


# ── C. 기술적 신호 (0~25) ─────────────────────────────────────
def _score_technical(ta: Optional[dict]) -> tuple[int, str]:
    """RSI 50~70, MA 단기 정배열, MACD 히스토그램 양수.

    ta: technical_analysis.analyze_ticker() 결과 dict (없으면 0점)
    """
    if not ta:
        return 0, "기술적 분석 데이터 없음"

    parts: list[str] = []
    score = 0

    # 1) RSI 50~70 (건강한 추세) +10
    rsi = float(ta.get("rsi_14") or 50.0)
    if 50.0 <= rsi <= 70.0:
        score += 10
        parts.append(f"RSI {rsi:.1f} (건강한 추세) +10")
    elif 45.0 <= rsi < 50.0 or 70.0 < rsi <= 75.0:
        score += 6
        parts.append(f"RSI {rsi:.1f} (수용 가능) +6")
    elif rsi > 75.0:
        score += 2
        parts.append(f"RSI {rsi:.1f} (과열) +2")
    else:
        parts.append(f"RSI {rsi:.1f} (약세)")

    # 2) 5일선 > 20일선 (단기 정배열) +8
    ma = ta.get("ma") or {}
    ma5 = float(ma.get("ma5") or 0)
    ma20 = float(ma.get("ma20") or 0)
    if ma5 > 0 and ma20 > 0:
        if ma5 > ma20:
            score += 8
            parts.append(f"MA5({int(ma5):,})>MA20({int(ma20):,}) 정배열 +8")
        else:
            parts.append(f"MA5<MA20 역배열")

    # 3) MACD 히스토그램 > 0 +7
    macd = ta.get("macd") or {}
    hist = float(macd.get("hist") or 0)
    if hist > 0:
        score += 7
        parts.append(f"MACD hist {hist:+.2f} +7")
    elif hist > -0.5:
        score += 3
        parts.append(f"MACD hist {hist:+.2f} 중립 +3")
    else:
        parts.append(f"MACD hist {hist:+.2f} 약세")

    return min(_MAX_COMPONENT, score), " · ".join(parts)


# ── D. 섹터 동조성 (0~25) ─────────────────────────────────────
def _score_sector_sync(ticker: str, sector: str | None,
                       sector_peers: list[dict]) -> tuple[int, str]:
    """같은 sector peers 중 강세(어제 +0.5% 초과) 비율 기반 점수."""
    if not sector:
        return 12, "섹터 정보 없음, 중립 12점"
    peers = [p for p in sector_peers
             if p.get("sector") == sector and p.get("ticker") != ticker]
    if not peers:
        return 12, f"섹터 '{sector}' peer 없음, 중립 12점"

    # 가장 강세인 종목 5개만 (TOP 100 안에서 이미 추렸으므로 충분)
    sample = peers[:5]
    up_count = sum(1 for p in sample if float(p.get("chg_pct") or 0) > 0.5)
    n = len(sample)
    ratio = up_count / n

    # 5/5 → 25, 4/5 → 20, 3/5 → 15, 2/5 → 10, 0~1/5 → 5
    if ratio >= 0.95:
        score = 25
    elif ratio >= 0.75:
        score = 20
    elif ratio >= 0.55:
        score = 15
    elif ratio >= 0.35:
        score = 10
    else:
        score = 5

    avg_chg = sum(float(p.get("chg_pct") or 0) for p in sample) / n
    note = (
        f"섹터 '{sector}' peer {n}종목 중 강세 {up_count}개 "
        f"(평균 {avg_chg:+.2f}%) → {score}점"
    )
    return score, note


# ── 메인 API ─────────────────────────────────────────────────
def score_pick(
    ticker: str,
    current_price: float,
    recent_top: list[dict],
    sector_peers: list[dict],
    market_regime: str = "sideways",
    ta_result: Optional[dict] = None,
) -> dict:
    """4개 요소 정량 합산 → 0~100점 → confidence 1~5 매핑.

    Args:
        ticker:        6자리 종목 코드
        current_price: 현재가 / 어제 종가 (참조용, 미사용이어도 인터페이스 통일)
        recent_top:    daily_top_value 최근 7일치 전체 (해당 ticker 시계열 필터링)
        sector_peers:  어제 daily_top_value (해당 sector peers 비교용) — 동일 데이터로 충분
        market_regime: 시장 국면 — 점수 자체엔 영향 없음 (보조 컨텍스트)
        ta_result:     technical_analysis.analyze_ticker() 결과 dict (선택)

    Returns:
        {
          'score':      78,
          'confidence': 4,
          'components': {
              'volume_momentum': 22,
              'capital_flow':    20,
              'technical':       18,
              'sector_sync':     18,
          },
          'rationale': "거래대금 1.52배 · 외인 3일 연속 매수 +10 · RSI 65 +10 · 섹터 강세 4/5"
        }
    """
    # ticker가 속한 sector 추출 (recent_top 최신 row)
    sector = None
    for r in sorted(recent_top, key=lambda x: str(x.get("date")), reverse=True):
        if r.get("ticker") == ticker:
            sector = r.get("sector")
            break

    # 각 요소 산출
    v_score, v_note = _score_volume_momentum(ticker, recent_top)
    f_score, f_note = _score_capital_flow(ticker, recent_top)
    t_score, t_note = _score_technical(ta_result)
    s_score, s_note = _score_sector_sync(ticker, sector, sector_peers)

    total = v_score + f_score + t_score + s_score
    total = max(0, min(_MAX_TOTAL, total))
    confidence = _score_to_confidence(total)

    rationale = " | ".join([
        f"거래대금: {v_note}",
        f"수급: {f_note}",
        f"기술: {t_note}",
        f"섹터: {s_note}",
    ])

    return {
        "score":      total,
        "confidence": confidence,
        "components": {
            "volume_momentum": v_score,
            "capital_flow":    f_score,
            "technical":       t_score,
            "sector_sync":     s_score,
        },
        "rationale":  rationale,
        "sector":     sector or "",
        "market_regime_context": market_regime,
    }


# ── CLI 디버그 ────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from src.infra.database import fetch_all

    target = sys.argv[1] if len(sys.argv) >= 2 else "005380"
    rows = fetch_all(
        """
        SELECT date, ticker, name, sector, chg_pct, trading_value,
               close_price, foreign_net_buy, inst_net_buy
        FROM daily_top_value
        WHERE date >= date('now', '-14 days', 'localtime')
        ORDER BY date DESC, rank ASC
        """
    )
    recent = [dict(r) for r in rows]

    ta_res = None
    try:
        from src.teams.research.technical_analysis import analyze_ticker
        # 어제 종가 추출
        for r in recent:
            if r["ticker"] == target and r.get("close_price"):
                ta_res = analyze_ticker(target, float(r["close_price"]))
                break
    except Exception as e:
        print(f"TA 분석 실패: {e}")

    result = score_pick(
        ticker=target,
        current_price=0.0,
        recent_top=recent,
        sector_peers=recent,
        market_regime="sideways",
        ta_result=ta_res,
    )
    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))
