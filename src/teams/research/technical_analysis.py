"""
technical_analysis.py — 종목별 차트 기반 손절/익절 자동 산출 (2026-05-18)

[배경]
  morning_brief에서 손절/익절을 "가격대 단순 분류 (대형/중형/소형)"로 산출하던 게으른
  로직을 **종목별 일봉 + 보조지표 기반 산출**로 교체.

  > 사용자 비판: "대형/중형/소형으로 나누는 게 아니라, 해당 종목의 데이터와 보조지표를
  >              보고 판단해야지."

[데이터 소스]
  - FDR (FinanceDataReader) — 일봉 60일 (시스템 이미 설치)
  - 폴백: daily_top_value 누적 (현재 4일치만, 60일 도달 시 자동 활용)

[핵심 출력]
  analyze_ticker(ticker, current_price) -> {
      'support_levels':    [...],  # 가까운 순 3개
      'resistance_levels': [...],
      'bollinger':         {'upper', 'middle', 'lower', 'width'},
      'ma':                {'ma5', 'ma20', 'ma60'},
      'rsi_14':            float,
      'macd':              {'macd', 'signal', 'hist'},
      'atr_14':            float,
      'atr_pct':           float,
      'fib_levels':        {'fib_236', ..., 'fib_786'},
      'stop_loss':         {'price', 'pct', 'basis', 'distance_atr'},
      'take_profit':       {'price', 'pct', 'basis', 'distance_atr'},
      'risk_reward_ratio': float,
  }

[손절/익절 우선순위]
  손절:
    1. 가까운 지지선 -0.5% (current 대비 -1.0%~-4.0% 범위)
    2. 20일 이동평균 (-1.0%~-3.5%)
    3. 볼린저 하단 또는 middle - 1σ
    4. ATR 폴백 (-1.5 × atr_pct, max -4%)

  익절:
    1. 가까운 저항선 -0.5% (current 대비 +2.0%~+6.0%)
    2. 볼린저 상단
    3. 피보나치 next-up
    4. ATR 폴백 (+2.5 × atr_pct, max +6%)

[성능]
  - FDR 호출 1회/종목 = ~1초 (네트워크 의존)
  - 모든 지표 계산은 로컬 numpy/pandas — Claude 추가 호출 X
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── 설정 상수 ────────────────────────────────────────────────
_OHLCV_DAYS = 60
_RSI_PERIOD = 14
_ATR_PERIOD = 14
_BB_PERIOD = 20
_BB_STD = 2.0
_MACD_FAST = 12
_MACD_SLOW = 26
_MACD_SIGNAL = 9
_PEAK_WINDOW = 5  # local max/min 식별 윈도우 (±5일)

# 손절/익절 제약 (현재가 대비 %)
_SL_MIN_PCT = -4.0
_SL_MAX_PCT = -1.0   # 최소 -1% (너무 좁으면 노이즈 손절)
_TP_MIN_PCT = 2.0
_TP_MAX_PCT = 6.0


# ── OHLCV 로더 ────────────────────────────────────────────────
def _fetch_ohlcv(ticker: str, days: int = _OHLCV_DAYS) -> Optional[pd.DataFrame]:
    """FDR로 일봉 OHLCV (Open/High/Low/Close/Volume) 조회.

    Returns: DataFrame index=Date, columns=Open/High/Low/Close/Volume, 최신순 마지막.
    """
    try:
        import FinanceDataReader as fdr  # noqa: WPS433
    except ImportError:
        logger.error("[TA] FinanceDataReader 미설치 — pip install finance-datareader")
        return None

    try:
        end = date.today()
        # 주말/휴일 보정 — 거래일 60일 확보 위해 캘린더 90일치 가져옴
        start = end - timedelta(days=int(days * 1.5) + 14)
        df = fdr.DataReader(ticker, start, end)
        if df is None or df.empty:
            logger.warning(f"[TA] {ticker} OHLCV 없음")
            return None
        # 거래일 기준 days만 유지
        df = df.tail(days).copy()
        if len(df) < 20:
            logger.warning(f"[TA] {ticker} 거래일 부족 ({len(df)}일) — 지표 정확도 낮음")
        return df
    except Exception as e:
        logger.warning(f"[TA] {ticker} FDR 조회 실패: {e}")
        return None


# ── 보조지표 ──────────────────────────────────────────────────
def _sma(series: pd.Series, period: int) -> float:
    if len(series) < period:
        return float(series.mean())
    return float(series.tail(period).mean())


def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _rsi(close: pd.Series, period: int = _RSI_PERIOD) -> float:
    if len(close) < period + 1:
        return 50.0
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    if pd.isna(val):
        return 50.0
    return float(val)


def _macd(close: pd.Series) -> dict:
    if len(close) < _MACD_SLOW:
        return {"macd": 0.0, "signal": 0.0, "hist": 0.0}
    ema_fast = _ema(close, _MACD_FAST)
    ema_slow = _ema(close, _MACD_SLOW)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, _MACD_SIGNAL)
    hist = macd_line - signal_line
    return {
        "macd":   float(macd_line.iloc[-1]),
        "signal": float(signal_line.iloc[-1]),
        "hist":   float(hist.iloc[-1]),
    }


def _atr(df: pd.DataFrame, period: int = _ATR_PERIOD) -> tuple[float, float]:
    """Returns (atr_absolute, atr_pct)."""
    if len(df) < 2:
        return 0.0, 0.0
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_val = float(tr.tail(period).mean())
    last_close = float(close.iloc[-1])
    atr_pct = (atr_val / last_close * 100) if last_close > 0 else 0.0
    return atr_val, atr_pct


def _bollinger(close: pd.Series, period: int = _BB_PERIOD, std_mult: float = _BB_STD) -> dict:
    if len(close) < period:
        period = max(5, len(close))
    window = close.tail(period)
    middle = float(window.mean())
    std = float(window.std(ddof=0))
    upper = middle + std_mult * std
    lower = middle - std_mult * std
    width = ((upper - lower) / middle * 100) if middle > 0 else 0.0
    return {
        "upper":  upper,
        "middle": middle,
        "lower":  lower,
        "width":  width,
        "std":    std,
    }


def _fib_levels(df: pd.DataFrame) -> dict:
    """최근 60일 high/low 기준 피보나치 되돌림."""
    if df.empty:
        return {}
    high = float(df["High"].max())
    low = float(df["Low"].min())
    diff = high - low
    if diff <= 0:
        return {}
    return {
        "fib_236": high - diff * 0.236,
        "fib_382": high - diff * 0.382,
        "fib_500": high - diff * 0.500,
        "fib_618": high - diff * 0.618,
        "fib_786": high - diff * 0.786,
        "high":    high,
        "low":     low,
    }


# ── 지지선/저항선 (Pivot Points) ──────────────────────────────
def _find_peaks(values: np.ndarray, window: int = _PEAK_WINDOW, find_max: bool = True) -> list[int]:
    """슬라이딩 윈도우 local maxima/minima 인덱스 — scipy 없이 numpy로 구현.

    Args:
        values: 가격 배열
        window: ±window일 내 최대/최소면 피크
        find_max: True=peak(고점) / False=valley(저점)

    Returns: 피크 인덱스 리스트
    """
    n = len(values)
    if n < 2 * window + 1:
        return []
    peaks: list[int] = []
    for i in range(window, n - window):
        win = values[i - window:i + window + 1]
        if find_max:
            if values[i] == win.max() and values[i] > 0:
                peaks.append(i)
        else:
            if values[i] == win.min() and values[i] > 0:
                peaks.append(i)
    return peaks


def _cluster_levels(prices: list[float], current_price: float,
                    cluster_pct: float = 1.5) -> list[dict]:
    """가까운 가격끼리 묶어 '레벨' 만들기 — 동일 가격대 다중 터치 = 강한 레벨.

    Args:
        cluster_pct: 클러스터 폭 (현재가 대비 %)

    Returns: [{'price': avg, 'touches': N, 'strength': N×weight}, ...]
    """
    if not prices:
        return []
    band = current_price * cluster_pct / 100  # 절대값 폭
    sorted_p = sorted(prices)
    clusters: list[list[float]] = []
    current_cluster = [sorted_p[0]]
    for p in sorted_p[1:]:
        if p - current_cluster[-1] <= band:
            current_cluster.append(p)
        else:
            clusters.append(current_cluster)
            current_cluster = [p]
    clusters.append(current_cluster)

    result = []
    for c in clusters:
        avg = sum(c) / len(c)
        result.append({
            "price":   avg,
            "touches": len(c),
        })
    return result


def _support_resistance(df: pd.DataFrame, current_price: float) -> tuple[list[dict], list[dict]]:
    """지지선/저항선 자동 식별.

    Returns: (supports[], resistances[]) — 각각 current_price에 가까운 순.
        각 원소: {'price', 'touches', 'distance_pct'}
    """
    if len(df) < 11:
        return [], []

    highs = df["High"].values
    lows = df["Low"].values

    # 고점 = 저항선 후보 / 저점 = 지지선 후보
    high_idx = _find_peaks(highs, window=_PEAK_WINDOW, find_max=True)
    low_idx = _find_peaks(lows, window=_PEAK_WINDOW, find_max=False)

    high_prices = [float(highs[i]) for i in high_idx]
    low_prices = [float(lows[i]) for i in low_idx]

    high_clusters = _cluster_levels(high_prices, current_price)
    low_clusters = _cluster_levels(low_prices, current_price)

    # 저항선 = current_price 위에 있는 클러스터
    resistances = [c for c in high_clusters if c["price"] > current_price * 1.001]
    # 지지선 = current_price 아래
    supports = [c for c in low_clusters if c["price"] < current_price * 0.999]

    # 추가 정보 (distance_pct) + 가까운 순 정렬
    for c in resistances:
        c["distance_pct"] = (c["price"] - current_price) / current_price * 100
    for c in supports:
        c["distance_pct"] = (current_price - c["price"]) / current_price * 100

    resistances.sort(key=lambda x: x["distance_pct"])
    supports.sort(key=lambda x: x["distance_pct"])

    return supports[:5], resistances[:5]


# ── 손절/익절 결정 로직 ──────────────────────────────────────
def _determine_stop_loss(
    current_price: float,
    supports: list[dict],
    ma: dict,
    bollinger: dict,
    atr_pct: float,
) -> dict:
    """손절가 결정 — 우선순위:
      1. 가까운 지지선 -0.5% (현재가 대비 -1%~-4% 범위)
      2. 20일 이동평균 (-1%~-3.5%)
      3. 볼린저 하단 또는 middle - 1σ
      4. ATR 폴백 (-1.5 × atr_pct, max -4%)
    """
    # 1. 지지선
    for s in supports:
        sl_price = s["price"] * 0.995  # 지지선 -0.5%
        sl_pct = (sl_price - current_price) / current_price * 100
        if _SL_MIN_PCT <= sl_pct <= _SL_MAX_PCT:
            return {
                "price":         round(sl_price, 0),
                "pct":           round(sl_pct, 2),
                "basis":         f"지지선 {int(s['price']):,}원 -0.5% ({s['touches']}회 터치)",
                "distance_atr":  round(abs(sl_pct) / atr_pct, 2) if atr_pct > 0 else 0.0,
            }

    # 2. 20일 이동평균
    ma20 = ma.get("ma20", 0)
    if ma20 > 0:
        sl_pct = (ma20 - current_price) / current_price * 100
        if -3.5 <= sl_pct <= _SL_MAX_PCT:
            return {
                "price":         round(ma20, 0),
                "pct":           round(sl_pct, 2),
                "basis":         f"20일선 {int(ma20):,}원",
                "distance_atr":  round(abs(sl_pct) / atr_pct, 2) if atr_pct > 0 else 0.0,
            }

    # 3. 볼린저 하단
    bb_lower = bollinger.get("lower", 0)
    bb_mid = bollinger.get("middle", 0)
    bb_std = bollinger.get("std", 0)
    if bb_lower > 0:
        sl_pct = (bb_lower - current_price) / current_price * 100
        if _SL_MIN_PCT <= sl_pct <= _SL_MAX_PCT:
            return {
                "price":         round(bb_lower, 0),
                "pct":           round(sl_pct, 2),
                "basis":         f"볼린저 하단 {int(bb_lower):,}원",
                "distance_atr":  round(abs(sl_pct) / atr_pct, 2) if atr_pct > 0 else 0.0,
            }
        # 볼린저 middle - 1σ 도전
        bb_minus_1sigma = bb_mid - bb_std
        if bb_minus_1sigma > 0:
            sl_pct2 = (bb_minus_1sigma - current_price) / current_price * 100
            if _SL_MIN_PCT <= sl_pct2 <= _SL_MAX_PCT:
                return {
                    "price":         round(bb_minus_1sigma, 0),
                    "pct":           round(sl_pct2, 2),
                    "basis":         f"볼린저 중심-1σ {int(bb_minus_1sigma):,}원",
                    "distance_atr":  round(abs(sl_pct2) / atr_pct, 2) if atr_pct > 0 else 0.0,
                }

    # 4. ATR 폴백
    sl_pct_atr = max(_SL_MIN_PCT, round(-1.5 * atr_pct, 2)) if atr_pct > 0 else -2.5
    sl_price = current_price * (1 + sl_pct_atr / 100)
    return {
        "price":         round(sl_price, 0),
        "pct":           sl_pct_atr,
        "basis":         f"ATR 폴백 ({atr_pct:.2f}% × -1.5)",
        "distance_atr":  1.5,
    }


def _determine_take_profit(
    current_price: float,
    resistances: list[dict],
    bollinger: dict,
    fib: dict,
    atr_pct: float,
) -> dict:
    """익절가 결정 — 우선순위:
      1. 가까운 저항선 -0.5% (+2%~+6%)
      2. 볼린저 상단
      3. 피보나치 next-up
      4. ATR 폴백 (+2.5 × atr_pct, max +6%)
    """
    # 1. 저항선
    for r in resistances:
        tp_price = r["price"] * 0.995
        tp_pct = (tp_price - current_price) / current_price * 100
        if _TP_MIN_PCT <= tp_pct <= _TP_MAX_PCT:
            return {
                "price":         round(tp_price, 0),
                "pct":           round(tp_pct, 2),
                "basis":         f"저항선 {int(r['price']):,}원 -0.5% ({r['touches']}회 터치)",
                "distance_atr":  round(tp_pct / atr_pct, 2) if atr_pct > 0 else 0.0,
            }

    # 2. 볼린저 상단
    bb_upper = bollinger.get("upper", 0)
    if bb_upper > 0:
        tp_pct = (bb_upper - current_price) / current_price * 100
        if _TP_MIN_PCT <= tp_pct <= _TP_MAX_PCT:
            return {
                "price":         round(bb_upper, 0),
                "pct":           round(tp_pct, 2),
                "basis":         f"볼린저 상단 {int(bb_upper):,}원",
                "distance_atr":  round(tp_pct / atr_pct, 2) if atr_pct > 0 else 0.0,
            }

    # 3. 피보나치 next-up
    if fib:
        # current_price 위쪽 피보나치 레벨 중 가장 가까운 것
        candidates = []
        for label, lvl in fib.items():
            if label in ("high", "low"):
                continue
            if lvl > current_price * 1.005:
                candidates.append((label, lvl))
        if candidates:
            candidates.sort(key=lambda x: x[1])
            label, lvl = candidates[0]
            tp_pct = (lvl - current_price) / current_price * 100
            if _TP_MIN_PCT <= tp_pct <= _TP_MAX_PCT:
                return {
                    "price":         round(lvl, 0),
                    "pct":           round(tp_pct, 2),
                    "basis":         f"피보나치 {label.replace('fib_', '')[:2]}.{label.replace('fib_', '')[2:]}% {int(lvl):,}원",
                    "distance_atr":  round(tp_pct / atr_pct, 2) if atr_pct > 0 else 0.0,
                }
        # 60일 고점 도전
        high = fib.get("high", 0)
        if high > 0:
            tp_pct = (high * 0.995 - current_price) / current_price * 100
            if _TP_MIN_PCT <= tp_pct <= _TP_MAX_PCT:
                return {
                    "price":         round(high * 0.995, 0),
                    "pct":           round(tp_pct, 2),
                    "basis":         f"60일 고점 {int(high):,}원 -0.5%",
                    "distance_atr":  round(tp_pct / atr_pct, 2) if atr_pct > 0 else 0.0,
                }

    # 4. ATR 폴백
    tp_pct_atr = min(_TP_MAX_PCT, round(2.5 * atr_pct, 2)) if atr_pct > 0 else 3.5
    tp_price = current_price * (1 + tp_pct_atr / 100)
    return {
        "price":         round(tp_price, 0),
        "pct":           tp_pct_atr,
        "basis":         f"ATR 폴백 ({atr_pct:.2f}% × 2.5)",
        "distance_atr":  2.5,
    }


# ── 메인 API ────────────────────────────────────────────────
def analyze_ticker(ticker: str, current_price: float, ohlcv_days: int = _OHLCV_DAYS) -> dict:
    """종목별 기술적 분석 + 손절/익절 산출.

    Args:
        ticker: 6자리 종목 코드
        current_price: 현재가 (참조점)
        ohlcv_days: 일봉 데이터 조회 일수 (기본 60)

    Returns: 위 모듈 docstring 참조. 분석 실패 시 None.
    """
    if not ticker or current_price <= 0:
        return None

    df = _fetch_ohlcv(ticker, ohlcv_days)
    if df is None or len(df) < 11:
        logger.warning(f"[TA] {ticker} 분석 불가 — 데이터 부족")
        return None

    close = df["Close"]

    # 보조지표 계산
    ma = {
        "ma5":  _sma(close, 5),
        "ma20": _sma(close, 20),
        "ma60": _sma(close, 60),
    }
    bollinger = _bollinger(close)
    rsi = _rsi(close)
    macd = _macd(close)
    atr_val, atr_pct = _atr(df)
    fib = _fib_levels(df)
    supports, resistances = _support_resistance(df, current_price)

    # 손절/익절
    sl = _determine_stop_loss(current_price, supports, ma, bollinger, atr_pct)
    tp = _determine_take_profit(current_price, resistances, bollinger, fib, atr_pct)

    rr_ratio = round(tp["pct"] / abs(sl["pct"]), 2) if sl["pct"] != 0 else 0.0

    return {
        "ticker":            ticker,
        "current_price":     current_price,
        "ohlcv_days":        len(df),
        "support_levels":    supports[:3],
        "resistance_levels": resistances[:3],
        "bollinger":         {
            "upper":  round(bollinger["upper"], 0),
            "middle": round(bollinger["middle"], 0),
            "lower":  round(bollinger["lower"], 0),
            "width":  round(bollinger["width"], 2),
        },
        "ma": {
            "ma5":  round(ma["ma5"], 0),
            "ma20": round(ma["ma20"], 0),
            "ma60": round(ma["ma60"], 0),
        },
        "rsi_14": round(rsi, 2),
        "macd": {
            "macd":   round(macd["macd"], 2),
            "signal": round(macd["signal"], 2),
            "hist":   round(macd["hist"], 2),
        },
        "atr_14":  round(atr_val, 2),
        "atr_pct": round(atr_pct, 2),
        "fib_levels": {k: round(v, 0) for k, v in fib.items()} if fib else {},
        "stop_loss":        sl,
        "take_profit":      tp,
        "risk_reward_ratio": rr_ratio,
    }


# ── CLI 디버그 ────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3:
        tk = sys.argv[1]
        px = float(sys.argv[2])
    else:
        tk = "005380"
        px = 669000.0
    result = analyze_ticker(tk, px)
    if result:
        import json
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"분석 실패: {tk}")
