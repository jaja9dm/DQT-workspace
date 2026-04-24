"""
short_selling.py — KRX 공매도 비율 일별 수집 & 캐시

KRX 정보데이터시스템 공개 API에서 전 종목 공매도 비중을 하루 1회 수집.
장 시작 전(08:50) 또는 첫 조회 시 자동 fetch, 당일 메모리 캐시 유지.

제공 함수:
    get_short_ratio(ticker)   -> float   # 0.0 ~ 100.0 (%)
    get_short_squeeze_rank()  -> list    # 공매도 비중 TOP 종목 (스퀴즈 후보)
    refresh()                 -> None    # 강제 갱신
"""

from __future__ import annotations

import threading
from datetime import datetime, date
from typing import Optional

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

# KRX 정보데이터시스템 공매도 현황 엔드포인트
_KRX_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
_KRX_BLD = "dbms/MDC/STAT/standard/MDCSTAT30101"
_TIMEOUT = 15
_HEADERS = {
    "Referer": "http://data.krx.co.kr/",
    "User-Agent": "Mozilla/5.0 (compatible; DQT/1.0)",
}


class ShortSellingCache:
    """일별 공매도 비중 싱글턴 캐시."""

    _instance: Optional["ShortSellingCache"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "ShortSellingCache":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._data: dict[str, float] = {}   # ticker → 공매도 비중 (%)
        self._fetched_date: Optional[date] = None
        self._data_lock = threading.RLock()

    # ── 공개 API ──────────────────────────────────────────────

    def get_short_ratio(self, ticker: str) -> float:
        """종목 공매도 비중 반환 (%). 데이터 없으면 0.0."""
        self._ensure_fresh()
        with self._data_lock:
            return self._data.get(ticker, 0.0)

    def get_short_squeeze_rank(self, top_n: int = 30) -> list[dict]:
        """
        공매도 비중 상위 종목 반환 (쇼트 스퀴즈 후보군).

        Returns:
            [{"ticker": str, "short_ratio": float}, ...] 내림차순
        """
        self._ensure_fresh()
        with self._data_lock:
            ranked = sorted(self._data.items(), key=lambda x: x[1], reverse=True)
        return [{"ticker": t, "short_ratio": r} for t, r in ranked[:top_n]]

    def refresh(self) -> None:
        """강제 갱신 (당일 데이터 재수집)."""
        self._fetch(_force=True)

    # ── 내부 로직 ──────────────────────────────────────────────

    def _ensure_fresh(self) -> None:
        today = date.today()
        with self._data_lock:
            already_fetched = self._fetched_date == today
        if not already_fetched:
            self._fetch()

    def _fetch(self, _force: bool = False) -> None:
        today = date.today()
        today_str = today.strftime("%Y%m%d")

        # 토요일/일요일은 직전 금요일 데이터 사용
        dow = today.weekday()
        if dow == 5:    # 토
            from datetime import timedelta
            today_str = (today - timedelta(days=1)).strftime("%Y%m%d")
        elif dow == 6:  # 일
            from datetime import timedelta
            today_str = (today - timedelta(days=2)).strftime("%Y%m%d")

        try:
            resp = requests.post(
                _KRX_URL,
                data={
                    "bld": _KRX_BLD,
                    "trdDd": today_str,
                    "mktId": "ALL",
                    "share": "1",
                    "money": "1",
                    "csvxls_isNo": "false",
                },
                headers=_HEADERS,
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            payload = resp.json()

            new_data: dict[str, float] = {}
            for item in payload.get("output", []):
                ticker = str(item.get("ISU_SRT_CD", "")).strip()
                ratio_raw = item.get("SHRT_SELL_WGHT", "0") or "0"
                # KRX returns "-" for stocks with no short selling
                try:
                    ratio = float(str(ratio_raw).replace(",", "").replace("-", "0"))
                except (ValueError, TypeError):
                    ratio = 0.0
                if ticker:
                    new_data[ticker] = ratio

            with self._data_lock:
                self._data = new_data
                self._fetched_date = date.today()

            logger.info(f"공매도 데이터 수집 완료: {len(new_data)}종목 ({today_str})")

        except Exception as e:
            logger.warning(f"공매도 데이터 수집 실패: {e}")
            # 실패해도 기존 캐시 유지 (당일 날짜 갱신 안 해서 재시도 안 함)


# ── 모듈 레벨 편의 함수 ────────────────────────────────────────

def get_short_ratio(ticker: str) -> float:
    """종목 공매도 비중 (%). 0.0 = 데이터 없음."""
    return ShortSellingCache().get_short_ratio(ticker)


def get_short_squeeze_candidates(top_n: int = 30) -> list[dict]:
    """공매도 비중 상위 종목 목록 (스퀴즈 후보)."""
    return ShortSellingCache().get_short_squeeze_rank(top_n=top_n)


def prefetch_short_data() -> None:
    """장 시작 전 공매도 데이터 선제 수집 (main.py에서 호출)."""
    t = threading.Thread(
        target=ShortSellingCache().refresh,
        daemon=True,
        name="short-selling-prefetch",
    )
    t.start()
