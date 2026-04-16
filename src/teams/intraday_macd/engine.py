"""
engine.py — 장중 MACD 모니터링팀

역할:
  Hot List 종목의 1분봉 데이터를 3분봉·5분봉으로 집계하여
  MACD Pre-Cross 신호를 감지하고 intraday_macd_signal 테이블에 기록한다.

  - 매매팀(TradingEngine): buy_pre 신호 발생 시 재진입 판단
  - 포지션 감시(PositionMonitorEngine): sell_pre 신호 발생 시 조기 손절 판단

실행 주기: 3분 (180초)

신호 판단 로직:
  1. KIS API에서 1분봉 30개 조회 (최신순)
  2. 3분봉·5분봉으로 집계
  3. 각 타임프레임별 MACD 계산
  4. Pre-Cross 감지 (settings.MACD_HIST_CONV_BARS봉 연속 수렴)
  5. 결과 DB 기록

  buy_pre  : 3분봉 BUY_PRE AND 5분봉 BUY_PRE → 양쪽 모두 골든크로스 임박
  sell_pre : 3분봉 SELL_PRE OR  5분봉 SELL_PRE → 어느 하나라도 데드크로스 임박
  hold     : 그 외
"""

from __future__ import annotations

import threading
import time
from datetime import datetime

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, Priority
from src.utils.logger import get_logger
from src.utils.macd import MACDSignal, aggregate_candles, get_signal, macd_from_candles

logger = get_logger(__name__)

_INTERVAL_SEC = 180   # 3분 주기


class IntradayMACDEngine:
    """장중 MACD 모니터링 엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="intraday-macd-engine",
        )

    def start(self) -> None:
        logger.info("장중 MACD 모니터링 엔진 시작")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=15)
        logger.info("장중 MACD 모니터링 엔진 종료")

    # ──────────────────────────────────────────
    # 메인 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"장중 MACD 모니터링 오류: {e}", exc_info=True)
            self._stop_event.wait(timeout=_INTERVAL_SEC)

    def run_once(self) -> list[dict]:
        """
        1회 실행: Hot List 조회 → 분봉 수집 → MACD 신호 기록.

        Returns:
            기록된 신호 목록
        """
        tickers = _load_watch_tickers()
        if not tickers:
            return []

        gw = KISGateway()
        results: list[dict] = []
        n = settings.MACD_HIST_CONV_BARS

        for ticker in tickers:
            try:
                # 1분봉 조회 (최신순 30봉)
                candles_1m = gw.get_minute_candles(ticker, priority=Priority.DATA_COLLECTION)
                if len(candles_1m) < 15:
                    continue  # 데이터 부족

                # 3분봉·5분봉 집계 (aggregate_candles는 시간순으로 반환)
                candles_3m = aggregate_candles(candles_1m, period=3)
                candles_5m = aggregate_candles(candles_1m, period=5)

                # MACD 계산
                df_3m = macd_from_candles(candles_3m)
                df_5m = macd_from_candles(candles_5m)

                if df_3m.empty or df_5m.empty:
                    continue

                # Pre-Cross 신호 감지
                sig_3m = get_signal(df_3m["hist"], n=n)
                sig_5m = get_signal(df_5m["hist"], n=n)

                # 최종 신호 결합
                if sig_3m == MACDSignal.BUY_PRE and sig_5m == MACDSignal.BUY_PRE:
                    final_signal = "buy_pre"
                elif sig_3m == MACDSignal.SELL_PRE or sig_5m == MACDSignal.SELL_PRE:
                    final_signal = "sell_pre"
                else:
                    final_signal = "hold"

                # DB 기록
                execute(
                    """
                    INSERT INTO intraday_macd_signal
                        (ticker, signal, hist_3m, hist_5m,
                         macd_3m, signal_3m, macd_5m, signal_5m)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ticker,
                        final_signal,
                        round(float(df_3m["hist"].iloc[-1]), 6),
                        round(float(df_5m["hist"].iloc[-1]), 6),
                        round(float(df_3m["macd"].iloc[-1]), 6),
                        round(float(df_3m["signal"].iloc[-1]), 6),
                        round(float(df_5m["macd"].iloc[-1]), 6),
                        round(float(df_5m["signal"].iloc[-1]), 6),
                    ),
                )

                if final_signal != "hold":
                    logger.info(
                        f"[MACD 신호] {ticker} → {final_signal.upper()} "
                        f"| 3분봉hist {df_3m['hist'].iloc[-1]:+.4f} "
                        f"| 5분봉hist {df_5m['hist'].iloc[-1]:+.4f}"
                    )

                results.append({"ticker": ticker, "signal": final_signal})

            except Exception as e:
                logger.debug(f"분봉 MACD 처리 실패 [{ticker}]: {e}")
                continue

        return results


# ──────────────────────────────────────────────
# 감시 대상 종목 조회
# ──────────────────────────────────────────────

def _load_watch_tickers() -> list[str]:
    """
    현재 감시 대상 종목 = Hot List (최근 30분) + 보유 포지션.
    두 집합의 합집합을 반환.
    """
    tickers: set[str] = set()

    # 1. 최근 Hot List
    rows = fetch_all(
        """
        SELECT DISTINCT ticker FROM hot_list
        WHERE created_at >= datetime('now', '-30 minutes')
        """
    )
    tickers.update(r["ticker"] for r in rows)

    # 2. 현재 보유 포지션 (trailing_stop 테이블 기준)
    rows2 = fetch_all("SELECT ticker FROM trailing_stop")
    tickers.update(r["ticker"] for r in rows2)

    return list(tickers)


# ──────────────────────────────────────────────
# 외부 조회 헬퍼 (position_monitor, trading 팀용)
# ──────────────────────────────────────────────

def get_latest_macd_signal(ticker: str, max_age_minutes: int = 5) -> str:
    """
    해당 종목의 최신 MACD 신호 조회.

    Args:
        ticker: 종목 코드
        max_age_minutes: 이 분 이내 신호만 유효 (기본 5분)

    Returns:
        "buy_pre" | "sell_pre" | "hold" (데이터 없으면 "hold")
    """
    row = fetch_one(
        """
        SELECT signal FROM intraday_macd_signal
        WHERE ticker = ?
          AND created_at >= datetime('now', ?)
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (ticker, f"-{max_age_minutes} minutes"),
    )
    return row["signal"] if row else "hold"


def get_macd_details(ticker: str, max_age_minutes: int = 6) -> dict:
    """
    최신 MACD 신호 + 히스토그램 값 조회 (동적 스캘핑 판단용).

    Args:
        ticker: 종목 코드
        max_age_minutes: 이 분 이내 신호만 유효

    Returns:
        {
            "signal": "buy_pre" | "sell_pre" | "hold",
            "hist_3m": float | None,   # 3분봉 MACD 히스토그램
            "hist_5m": float | None,   # 5분봉 MACD 히스토그램
        }
    """
    row = fetch_one(
        """
        SELECT signal, hist_3m, hist_5m FROM intraday_macd_signal
        WHERE ticker = ?
          AND created_at >= datetime('now', ?)
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (ticker, f"-{max_age_minutes} minutes"),
    )
    if row:
        return {
            "signal": row["signal"],
            "hist_3m": row["hist_3m"],
            "hist_5m": row["hist_5m"],
        }
    return {"signal": "hold", "hist_3m": None, "hist_5m": None}
