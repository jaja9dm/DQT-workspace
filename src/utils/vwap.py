"""
vwap.py — 당일 VWAP(거래량가중평균가격) 계산.

intraday_candles(ticker, bar_time HHmmss, saved_at) DB 기반.
saved_at >= 오늘 날짜 필터로 전일 데이터 오염 차단.

공유 모듈: trading, position_monitor 등 여러 팀에서 import해 사용.
"""
from __future__ import annotations

from src.infra.database import fetch_all


def _fetch_today_candles(ticker: str) -> list:
    return fetch_all(
        """
        SELECT high, low, close, volume
        FROM intraday_candles
        WHERE ticker = ?
          AND bar_time >= '090000'
          AND saved_at >= date('now', 'localtime')
        ORDER BY bar_time ASC
        """,
        (ticker,),
    )


def get_vwap(ticker: str) -> float:
    """당일 VWAP. 데이터 부족(< 3봉) 시 0.0."""
    rows = _fetch_today_candles(ticker)
    if len(rows) < 3:
        return 0.0
    cum_pv = cum_vol = 0.0
    for r in rows:
        typical = (float(r["high"]) + float(r["low"]) + float(r["close"])) / 3
        vol = float(r["volume"])
        cum_pv += typical * vol
        cum_vol += vol
    return (cum_pv / cum_vol) if cum_vol > 0 else 0.0


def get_vwap_and_price(ticker: str) -> tuple[float, float]:
    """당일 VWAP + 현재가(마지막 봉 close). 데이터 부족 시 (0.0, 0.0)."""
    rows = _fetch_today_candles(ticker)
    if len(rows) < 3:
        return 0.0, 0.0
    cum_pv = cum_vol = 0.0
    for r in rows:
        typical = (float(r["high"]) + float(r["low"]) + float(r["close"])) / 3
        vol = float(r["volume"])
        cum_pv += typical * vol
        cum_vol += vol
    if cum_vol == 0:
        return 0.0, 0.0
    return cum_pv / cum_vol, float(rows[-1]["close"])
