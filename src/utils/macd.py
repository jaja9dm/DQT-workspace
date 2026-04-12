"""
macd.py — MACD 계산 및 Pre-Cross 예측 신호 유틸

전략 핵심:
  완전 크로스가 이루어진 후 액션하면 이미 늦음.
  히스토그램이 수렴 중인 것을 감지하여 크로스 직전에 선제 대응.

  골든크로스 임박 (BUY_PRE):
    히스토그램이 음수인데 N봉 연속 증가 (절댓값 감소 → 0에 수렴)

  데드크로스 임박 (SELL_PRE):
    히스토그램이 양수인데 N봉 연속 감소 (0에 수렴)

  분봉 집계: 1분봉 원데이터를 3분봉·5분봉으로 집계 후 각각 MACD 계산
"""
from __future__ import annotations

from enum import Enum

import pandas as pd


class MACDSignal(Enum):
    BUY_PRE  = "buy_pre"   # 골든크로스 임박 → 예측 매수
    SELL_PRE = "sell_pre"  # 데드크로스 임박 → 예측 매도
    HOLD     = "hold"      # 신호 없음


def calc_macd(
    closes: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """
    MACD 계산.

    Returns:
        DataFrame with columns: macd, signal, hist
    """
    ema_fast   = closes.ewm(span=fast,   adjust=False).mean()
    ema_slow   = closes.ewm(span=slow,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist        = macd_line - signal_line

    return pd.DataFrame({
        "macd":   macd_line,
        "signal": signal_line,
        "hist":   hist,
    }, index=closes.index)


def get_signal(hist_series: pd.Series, n: int = 2) -> MACDSignal:
    """
    히스토그램 추세로 Pre-Cross 신호 판단.

    n봉 이상의 연속 수렴이 확인되면 신호 발생.
    크로스가 완성되기 전(음수/양수 유지 상태)에만 신호를 냄.

    Args:
        hist_series: MACD 히스토그램 Series (시간순)
        n: 연속 수렴 봉 수 (기본 2)

    Returns:
        MACDSignal 열거값
    """
    if len(hist_series) < n + 1:
        return MACDSignal.HOLD

    # 마지막 n봉 슬라이스
    recent = hist_series.iloc[-(n + 1):]
    last_n = recent.iloc[1:]   # 비교 기준 제외한 마지막 n봉

    # ── 골든크로스 임박: 히스토그램이 음수이면서 연속 증가 (0에 수렴) ──
    if all(v < 0 for v in last_n):
        # 각 봉이 이전 봉보다 크면 = 음수지만 0 방향으로 증가
        converging = all(
            last_n.iloc[i] > last_n.iloc[i - 1]
            for i in range(1, len(last_n))
        )
        if converging:
            return MACDSignal.BUY_PRE

    # ── 데드크로스 임박: 히스토그램이 양수이면서 연속 감소 (0에 수렴) ──
    if all(v > 0 for v in last_n):
        converging = all(
            last_n.iloc[i] < last_n.iloc[i - 1]
            for i in range(1, len(last_n))
        )
        if converging:
            return MACDSignal.SELL_PRE

    return MACDSignal.HOLD


def is_daily_macd_bullish(
    macd_val: float,
    signal_val: float,
    hist_val: float,
    prev_hist_val: float,
) -> bool:
    """
    일봉 MACD 강세 여부 판단.

    조건 (하나라도 충족 → True):
      1. MACD Line > Signal Line (골든크로스 완성)
      2. 히스토그램이 음수이지만 전봉보다 증가 (골든크로스 임박)

    Args:
        macd_val:      마지막 봉 MACD Line
        signal_val:    마지막 봉 Signal Line
        hist_val:      마지막 봉 히스토그램
        prev_hist_val: 직전 봉 히스토그램
    """
    golden_cross = macd_val > signal_val
    converging   = (hist_val < 0) and (hist_val > prev_hist_val)
    return golden_cross or converging


def aggregate_candles(candles_1m: list[dict], period: int) -> list[dict]:
    """
    1분봉 리스트를 period분봉으로 집계.

    candles_1m: [{"time": "HHmmss", "open": .., "high": .., "low": .., "close": .., "volume": ..}, ...]
                KIS API는 최신순으로 반환하므로 역전 후 집계.

    Returns:
        period분봉 리스트 (시간순, 오래된 것 먼저)
    """
    if not candles_1m:
        return []

    # KIS API 반환은 최신순 → 역전하여 시간순으로
    ordered = list(reversed(candles_1m))

    aggregated: list[dict] = []
    bucket: list[dict] = []

    for candle in ordered:
        bucket.append(candle)
        if len(bucket) == period:
            agg = {
                "time":   bucket[0]["time"],
                "open":   bucket[0]["open"],
                "high":   max(c["high"] for c in bucket),
                "low":    min(c["low"]  for c in bucket),
                "close":  bucket[-1]["close"],
                "volume": sum(c["volume"] for c in bucket),
            }
            aggregated.append(agg)
            bucket = []

    # 남은 봉 (미완성 현재 봉)도 포함
    if bucket:
        agg = {
            "time":   bucket[0]["time"],
            "open":   bucket[0]["open"],
            "high":   max(c["high"] for c in bucket),
            "low":    min(c["low"]  for c in bucket),
            "close":  bucket[-1]["close"],
            "volume": sum(c["volume"] for c in bucket),
        }
        aggregated.append(agg)

    return aggregated


def macd_from_candles(
    candles: list[dict],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """
    분봉 리스트에서 MACD 계산.

    Returns:
        DataFrame with columns: macd, signal, hist (봉 수만큼 rows)
        데이터 부족 시 빈 DataFrame 반환
    """
    if len(candles) < slow + signal:
        return pd.DataFrame(columns=["macd", "signal", "hist"])

    closes = pd.Series([c["close"] for c in candles], dtype=float)
    return calc_macd(closes, fast=fast, slow=slow, signal=signal)
