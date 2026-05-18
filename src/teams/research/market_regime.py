"""
market_regime.py — 시장 국면 자동 분류 (2026-05-18)

[배경]
  morning_brief의 market_regime은 Claude가 매번 다른 기준으로 분류 → 일관성 부족.
  KOSPI 이격도/VIX/외인 5일/거래대금 추이를 정량 지표로 묶어
  strong | sideways | weak | reversal | volatile 5개 국면으로 결정론적 분류.

[데이터 소스]
  - us_market_daily: VIX 절대값 + 변화
  - market_condition: 한국 시장 종합 점수, 외인/기관 순매수 (어제)
  - kosdaq_condition: KOSDAQ 등락률 + 외인 (백업)
  - daily_top_value: TOP 100 거래대금 합 = KOSPI/KOSDAQ 활동량 프록시
  - KOSPI 지수: FDR로 직접 조회 (옵션 — 실패해도 진행)

[분류 규칙]
  strong:   KOSPI 20일선 위 + VIX < 18 + 외인 5일 순매수
  weak:     KOSPI 20일선 -3%↓ + 외인 5일 -1,000억↓
  reversal: 어제 KOSPI 등락 ±2.5% 초과 + 거래대금 1.5배+
  volatile: VIX > 25 또는 KOSPI 5일 표준편차 > 2.0%
  sideways: 위 어느 것도 아님

[핵심 함수]
  classify_regime() -> dict
"""

from __future__ import annotations

from datetime import date, timedelta
from statistics import mean, pstdev

from src.infra.database import fetch_all, fetch_one
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ── 임계값 ────────────────────────────────────────────────────
_VIX_LOW = 18.0
_VIX_HIGH = 25.0
_KOSPI_MA20_WEAK_PCT = -3.0
_KOSPI_BIG_MOVE_PCT = 2.5
_VOLUME_SPIKE_RATIO = 1.5
_FOREIGN_WEAK_THRESHOLD = -1000.0    # 외인 5일 누적 -1000억 이하 → 약세
_KOSPI_VOLATILE_STD_PCT = 2.0


def _fetch_us_latest() -> dict | None:
    row = fetch_one(
        "SELECT * FROM us_market_daily ORDER BY date DESC LIMIT 1"
    )
    return dict(row) if row else None


def _fetch_market_5d() -> list[dict]:
    """최근 5일 market_condition (어제부터 거꾸로)."""
    rows = fetch_all(
        """
        SELECT market_score, market_direction,
               foreign_net_buy_bn, institutional_net_buy_bn, created_at
        FROM market_condition
        ORDER BY created_at DESC LIMIT 5
        """
    )
    return [dict(r) for r in rows] if rows else []


def _fetch_kospi_series() -> list[dict]:
    """FDR로 KOSPI 지수 일봉 30일 조회. 실패 시 빈 리스트."""
    try:
        import FinanceDataReader as fdr  # noqa: WPS433
    except Exception:
        logger.warning("[market_regime] FDR 미설치 — KOSPI 이격도 폴백")
        return []

    try:
        end = date.today()
        start = end - timedelta(days=50)
        df = fdr.DataReader("KS11", start, end)
        if df is None or df.empty:
            return []
        return [
            {
                "date":  idx.strftime("%Y-%m-%d"),
                "close": float(row["Close"]),
                "chg_pct": (
                    float(row["Change"]) * 100 if "Change" in row else 0.0
                ),
            }
            for idx, row in df.tail(30).iterrows()
        ]
    except Exception as e:
        logger.warning(f"[market_regime] KOSPI 조회 실패: {e}")
        return []


def _fetch_top_value_volume() -> dict:
    """TOP 100 거래대금 합 — 어제 vs 직전 5일 평균 비율 (= 시장 활동성)."""
    rows = fetch_all(
        """
        SELECT date, SUM(trading_value) AS total_value
        FROM daily_top_value
        WHERE date >= date('now', '-14 days', 'localtime')
        GROUP BY date
        ORDER BY date DESC
        LIMIT 6
        """
    )
    data = [dict(r) for r in rows] if rows else []
    if len(data) < 2:
        return {"latest_eok": 0.0, "ratio": 1.0, "days": len(data)}

    latest = float(data[0]["total_value"] or 0)
    prior = data[1:6]
    if not prior:
        return {"latest_eok": latest / 1e8, "ratio": 1.0, "days": len(data)}
    avg_prior = mean(float(r["total_value"] or 0) for r in prior)
    ratio = (latest / avg_prior) if avg_prior > 0 else 1.0
    return {
        "latest_eok": round(latest / 1e8, 0),
        "ratio":      round(ratio, 2),
        "days":       len(data),
    }


def _kospi_metrics(series: list[dict]) -> dict:
    """KOSPI 시계열 → MA20 이격도 + 5일 표준편차 + 어제 등락."""
    if not series:
        return {
            "ma20_dev_pct":  None,
            "yesterday_chg": None,
            "std_5d_pct":    None,
            "latest_close":  None,
            "data_available": False,
        }
    closes = [r["close"] for r in series]
    latest = closes[-1]
    yesterday_chg = series[-1]["chg_pct"] if series else 0.0

    # MA20 이격도
    if len(closes) >= 20:
        ma20 = mean(closes[-20:])
        ma20_dev = (latest - ma20) / ma20 * 100
    else:
        ma20_dev = None

    # 5일 일간 등락률 표준편차
    chgs_5d = [r["chg_pct"] for r in series[-5:]]
    std_5d = pstdev(chgs_5d) if len(chgs_5d) >= 3 else None

    return {
        "ma20_dev_pct":   round(ma20_dev, 2) if ma20_dev is not None else None,
        "yesterday_chg":  round(yesterday_chg, 2),
        "std_5d_pct":     round(std_5d, 2) if std_5d is not None else None,
        "latest_close":   round(latest, 2),
        "data_available": True,
    }


def _foreign_5d_sum(market_5d: list[dict]) -> float:
    """market_condition 5일 외인 합 (억원)."""
    return round(sum(float(r.get("foreign_net_buy_bn") or 0) for r in market_5d), 0)


# ── 분류 함수 ────────────────────────────────────────────────
def classify_regime() -> dict:
    """정량 지표 기반 시장 국면 자동 분류.

    Returns:
      {
        'regime': 'reversal',  # strong|sideways|weak|reversal|volatile
        'indicators': {
            'kospi_ma20_dev_pct':  -0.7,
            'kospi_yesterday_chg':  +1.2,
            'kospi_std_5d_pct':      1.85,
            'vix':                  18.4,
            'vix_chg':              +1.17,
            'foreign_5d_eok':    -3500.0,
            'volume_ratio':          0.95,
        },
        'rationale': "KOSPI 20일선 -0.7%, VIX 18.4 중립, 외인 5일 -3,500억..."
      }
    """
    us = _fetch_us_latest() or {}
    market_5d = _fetch_market_5d()
    kospi_series = _fetch_kospi_series()
    kospi = _kospi_metrics(kospi_series)
    volume = _fetch_top_value_volume()

    vix = float(us.get("vix") or 0.0)
    vix_chg = float(us.get("vix_chg") or 0.0)
    foreign_5d_eok = _foreign_5d_sum(market_5d)

    indicators = {
        "kospi_ma20_dev_pct":  kospi["ma20_dev_pct"],
        "kospi_yesterday_chg": kospi["yesterday_chg"],
        "kospi_std_5d_pct":    kospi["std_5d_pct"],
        "vix":                 round(vix, 2),
        "vix_chg":             round(vix_chg, 2),
        "foreign_5d_eok":      foreign_5d_eok,
        "volume_ratio":        volume["ratio"],
        "kospi_data_available": kospi["data_available"],
    }

    reasons: list[str] = []
    regime = "sideways"

    ma20_dev = kospi.get("ma20_dev_pct")
    yesterday_chg = kospi.get("yesterday_chg")
    std_5d = kospi.get("std_5d_pct")

    # 1. volatile — VIX 급등 또는 KOSPI 5일 표준편차 극단
    if vix > _VIX_HIGH:
        regime = "volatile"
        reasons.append(f"VIX {vix:.1f} > {_VIX_HIGH} (변동성 큼)")
    elif std_5d is not None and std_5d > _KOSPI_VOLATILE_STD_PCT:
        regime = "volatile"
        reasons.append(f"KOSPI 5일 표준편차 {std_5d:.2f}% > {_KOSPI_VOLATILE_STD_PCT}")

    # 2. reversal — 어제 KOSPI ±2.5% 초과 + 거래대금 1.5배+
    if regime == "sideways" and yesterday_chg is not None:
        if abs(yesterday_chg) >= _KOSPI_BIG_MOVE_PCT and volume["ratio"] >= _VOLUME_SPIKE_RATIO:
            regime = "reversal"
            direction = "급락 후 반등" if yesterday_chg < 0 else "급등 후 차익"
            reasons.append(
                f"KOSPI 어제 {yesterday_chg:+.2f}% + 거래대금 {volume['ratio']:.2f}배 "
                f"({direction} 가능성)"
            )

    # 3. weak — KOSPI 20일선 -3%↓ + 외인 5일 -1000억↓
    if regime == "sideways":
        weak_kospi = ma20_dev is not None and ma20_dev <= _KOSPI_MA20_WEAK_PCT
        weak_foreign = foreign_5d_eok <= _FOREIGN_WEAK_THRESHOLD
        if weak_kospi and weak_foreign:
            regime = "weak"
            reasons.append(
                f"KOSPI 20일선 {ma20_dev:+.2f}% + 외인 5일 {foreign_5d_eok:+,.0f}억"
            )
        elif weak_kospi:
            # KOSPI만 약하고 외인 데이터가 없으면 weak로 분류 (단, 외인 데이터 누락이 분명한 경우)
            if not market_5d:
                regime = "weak"
                reasons.append(
                    f"KOSPI 20일선 {ma20_dev:+.2f}% (외인 데이터 없음)"
                )

    # 4. strong — KOSPI 20일선 위 + VIX < 18 + 외인 5일 매수
    if regime == "sideways":
        strong_kospi = ma20_dev is not None and ma20_dev > 0
        strong_vix = vix > 0 and vix < _VIX_LOW
        strong_foreign = foreign_5d_eok > 0
        cond_n = sum([strong_kospi, strong_vix, strong_foreign])
        if cond_n >= 3:
            regime = "strong"
            reasons.append(
                f"KOSPI 20일선 {ma20_dev:+.2f}% + VIX {vix:.1f} + 외인 5일 {foreign_5d_eok:+,.0f}억"
            )
        elif cond_n == 2 and strong_kospi:
            regime = "strong"
            reasons.append(
                f"강세 조건 2/3 충족 (KOSPI MA20 {ma20_dev:+.2f}%, "
                f"VIX {vix:.1f}, 외인 {foreign_5d_eok:+,.0f}억)"
            )

    # 5. sideways — 위 조건 모두 X
    if not reasons:
        reasons.append("뚜렷한 지표 변화 없음")
        if ma20_dev is not None:
            reasons.append(f"KOSPI MA20 이격 {ma20_dev:+.2f}%")
        if vix > 0:
            reasons.append(f"VIX {vix:.1f}")
        reasons.append(f"외인 5일 {foreign_5d_eok:+,.0f}억")

    # rationale
    rationale_parts = [
        f"KOSPI 20일선 이격 {indicators['kospi_ma20_dev_pct']:+.2f}%"
        if indicators["kospi_ma20_dev_pct"] is not None else "KOSPI 데이터 없음",
        f"VIX {indicators['vix']:.1f}({indicators['vix_chg']:+.2f}pt)"
        if indicators["vix"] > 0 else "VIX 데이터 없음",
        f"외인 5일 {indicators['foreign_5d_eok']:+,.0f}억",
        f"거래대금 5일 대비 {indicators['volume_ratio']:.2f}배",
    ]
    if kospi.get("yesterday_chg") is not None:
        rationale_parts.insert(0, f"KOSPI 어제 {kospi['yesterday_chg']:+.2f}%")

    rationale = " · ".join(rationale_parts) + " → " + " / ".join(reasons)

    return {
        "regime":     regime,
        "indicators": indicators,
        "rationale":  rationale,
    }


# ── CLI ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    r = classify_regime()
    print(json.dumps(r, ensure_ascii=False, indent=2))
