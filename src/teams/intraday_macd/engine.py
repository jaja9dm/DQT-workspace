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

import requests as _requests

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, Priority
from src.utils.logger import get_logger
from src.utils.macd import MACDSignal, aggregate_candles, get_signal, macd_from_candles

logger = get_logger(__name__)

_INTERVAL_SEC = 180   # 3분 주기
_CANDLE_SLEEP = 5.0   # 분봉 API 종목간 간격 — KIS 모의투자 엄격 제한 (직접 requests 사용)


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

        results: list[dict] = []
        n = settings.MACD_HIST_CONV_BARS

        # 분봉 API 토큰: KIS Gateway에서 가져오되 직접 requests 사용 (큐 우회 → 간격 보장)
        gw = KISGateway()
        try:
            token = gw._get_token()
        except Exception as e:
            logger.warning(f"MACD 토큰 조회 실패: {e}")
            return []

        for idx, ticker in enumerate(tickers):
            if idx > 0:
                time.sleep(_CANDLE_SLEEP)  # 분봉 API rate limit: 5초 간격 보장
            try:
                # KIS Gateway 큐 우회 — 직접 HTTP로 5초 간격 보장
                candles_1m = _fetch_minute_candles_direct(ticker, token, gw._base_url, gw._app_key, gw._app_secret)
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
                #
                # buy_pre  : 3분봉 OR 5분봉 중 하나만 BUY_PRE여도 진입 신호
                #            (AND → OR: 히스토그램 저점 반등 초기에 더 빠르게 포착)
                # sell_pre : 3분봉 AND 5분봉 모두 SELL_PRE일 때만 청산 신호
                #            (OR → AND: 어느 하나만 약해도 섣불리 청산하는 오류 방지)
                #            → 첫 번째 신호 = 스캘핑 부분 익절 트리거
                #              두 번째 이상 연속 = 전량 청산 트리거 (연속 횟수로 판단)
                raw_sig_3m = sig_3m.value   # "buy_pre"|"sell_pre"|"hold"
                raw_sig_5m = sig_5m.value

                if sig_3m == MACDSignal.BUY_PRE or sig_5m == MACDSignal.BUY_PRE:
                    final_signal = "buy_pre"
                elif sig_3m == MACDSignal.SELL_PRE and sig_5m == MACDSignal.SELL_PRE:
                    final_signal = "sell_pre"
                else:
                    final_signal = "hold"

                # 분봉 캔들 저장 (ATR·거래량 압력 계산용 — 종목별 최근 30봉 유지)
                _save_candles(ticker, candles_1m[:30])

                # DB 기록 (sig_3m/sig_5m: 타임프레임별 개별 신호 저장)
                execute(
                    """
                    INSERT INTO intraday_macd_signal
                        (ticker, signal, hist_3m, hist_5m,
                         macd_3m, signal_3m, macd_5m, signal_5m,
                         sig_3m, sig_5m)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        raw_sig_3m,
                        raw_sig_5m,
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

def _fetch_minute_candles_direct(
    ticker: str,
    token: str,
    base_url: str,
    app_key: str,
    app_secret: str,
) -> list[dict]:
    """
    KIS 분봉 API를 Gateway 큐 우회 직접 requests로 호출.

    호출 전 반드시 5초 간격을 보장해야 함 (KIS 모의투자 rate limit).

    Returns:
        [{"time": "HHmmss", "open": .., "high": .., "low": .., "close": .., "volume": ..}, ...]
        최신순(내림차순) 반환 — aggregate_candles 전달 전 그대로 사용 가능
    """
    now_str = datetime.now().strftime("%H%M%S")
    r = _requests.get(
        f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
        headers={
            "Authorization": f"Bearer {token}",
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": "FHKST03010200",
            "custtype": "P",
        },
        params={
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_HOUR_1": now_str,
            "FID_PW_DATA_INCU_YN": "N",
        },
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()

    rt_cd = data.get("rt_cd", "-1")
    msg = data.get("msg1", "")
    if rt_cd != "0":
        raise RuntimeError(f"KIS 분봉 API 오류 [{rt_cd}]: {msg}")

    candles: list[dict] = []
    for item in data.get("output2", []):
        try:
            candles.append({
                "time":   item.get("stck_cntg_hour", ""),
                "open":   float(item.get("stck_oprc", 0) or 0),
                "high":   float(item.get("stck_hgpr", 0) or 0),
                "low":    float(item.get("stck_lwpr", 0) or 0),
                "close":  float(item.get("stck_prpr", 0) or 0),
                "volume": int(item.get("cntg_vol", 0) or 0),
            })
        except (ValueError, TypeError):
            continue
    return candles  # 최신순 반환


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

def get_consecutive_sell_pre(ticker: str, max_age_minutes: int = 20) -> int:
    """
    최근 max_age_minutes 이내에서 가장 최근부터 연속으로 sell_pre가 몇 번 나왔는지 반환.

    예: [sell_pre, sell_pre, hold, sell_pre] → 2 (가장 최신 연속만 카운트)

    position_monitor가 "파란 바 누적" 판단에 사용:
      - 1회: 스캘핑 부분 익절 소진도 가산
      - 2회: 본격 청산 검토
      - 3회+: 전량 청산 트리거 가능
    """
    rows = fetch_all(
        """
        SELECT signal FROM intraday_macd_signal
        WHERE ticker = ?
          AND created_at >= datetime('now', ?)
        ORDER BY created_at DESC
        LIMIT 6
        """,
        (ticker, f"-{max_age_minutes} minutes"),
    )
    count = 0
    for r in rows:
        if r["signal"] == "sell_pre":
            count += 1
        else:
            break  # 연속이 끊기면 종료
    return count


def get_macd_dual_confirm(ticker: str, max_age_minutes: int = 6) -> bool:
    """
    3분봉 AND 5분봉 모두 buy_pre인지 확인.
    사용자 방식: 두 타임프레임이 같은 방향이어야 진입.
    opening_plunge_rebound 등 고확신 진입 시 사용.
    """
    row = fetch_one(
        """
        SELECT sig_3m, sig_5m FROM intraday_macd_signal
        WHERE ticker = ?
          AND created_at >= datetime('now', ?)
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (ticker, f"-{max_age_minutes} minutes"),
    )
    if not row:
        return False
    return row["sig_3m"] == "buy_pre" and row["sig_5m"] == "buy_pre"


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


# ──────────────────────────────────────────────
# 분봉 캔들 저장 (ATR·거래량 계산용)
# ──────────────────────────────────────────────

def _save_candles(ticker: str, candles: list[dict]) -> None:
    """
    1분봉 캔들을 intraday_candles 테이블에 저장.
    종목별 최근 30봉만 유지 (오래된 것 자동 삭제).
    """
    if not candles:
        return
    try:
        for c in candles:
            execute(
                """
                INSERT OR REPLACE INTO intraday_candles
                    (ticker, bar_time, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ticker, c["time"], c["open"], c["high"], c["low"], c["close"], c["volume"]),
            )
        # 30봉 초과분 삭제
        execute(
            """
            DELETE FROM intraday_candles
            WHERE ticker = ?
              AND bar_time NOT IN (
                  SELECT bar_time FROM intraday_candles
                  WHERE ticker = ?
                  ORDER BY bar_time DESC
                  LIMIT 30
              )
            """,
            (ticker, ticker),
        )
    except Exception as e:
        logger.debug(f"캔들 저장 오류 [{ticker}]: {e}")
