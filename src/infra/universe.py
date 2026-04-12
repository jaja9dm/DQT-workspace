"""
universe.py — 공통 인프라 0-3: 종목 유니버스 관리

역할:
  매일 장 전 1회 유니버스를 확정하고 DB에 저장.
  장 중에는 KIND RSS 공시 발생 종목을 실시간으로 추가.

유니버스 구성:
  - KOSPI 200       (시가총액 상위 — FinanceDataReader)
  - KOSDAQ 150      (코스닥 대표 — FinanceDataReader)
  - 거래량 Top 100  (전일 거래대금 TOP 100 — FinanceDataReader)
  - 공시 발생 종목  (KIND RSS 실시간 — 당일 편입)
  → 총 ~450종목 (중복 제거)

목적:
  스캔 대상을 사전 제한하여 KIS API 호출 폭발과 Claude 비용 낭비 방지.
"""

from __future__ import annotations

import re
import threading
import time
from datetime import date, datetime
from urllib.request import Request, urlopen

import FinanceDataReader as fdr
import pandas as pd

from src.infra.database import execute, fetch_all, get_conn
from src.utils.logger import get_logger
from src.utils.retry import retry_call

logger = get_logger(__name__)

# KIND 공시 검색 API (JSON)
_KIND_DISCLOSURE_URL = (
    "https://kind.krx.co.kr/disclosureinfo/todaydisclosure/main.do"
    "?method=searchTodayDisclosureInfo&currentPage=1&rowsPerPage=30"
    "&orderMode=0&orderStat=D&forward=todaydisclosure_main"
)

# 종목 코드 6자리 추출용 정규식
_TICKER_PATTERN = re.compile(r"\b(\d{6})\b")

# 브라우저 헤더 (KIND 서버 User-Agent 차단 우회)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json,*/*",
    "Referer": "https://kind.krx.co.kr/",
}


class UniverseManager:
    """
    종목 유니버스 관리 싱글턴.

    Usage:
        um = UniverseManager()
        um.rebuild()                    # 장 전 전체 재구성
        tickers = um.get_today()        # 오늘 유니버스 조회
        um.start_disclosure_watcher()   # 공시 실시간 감시 시작
    """

    _instance: "UniverseManager | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "UniverseManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._watcher_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._seen_disclosures: set[str] = set()  # 이미 처리한 공시 URL 중복 방지

    # ──────────────────────────────────────────
    # 공개 API
    # ──────────────────────────────────────────

    def rebuild(self) -> int:
        """
        유니버스 전체 재구성 (장 전 1회 호출).

        1. 오늘 날짜 기존 유니버스 삭제
        2. KOSPI 200 / KOSDAQ 150 / 거래량 Top 100 수집
        3. DB에 저장

        Returns:
            저장된 종목 수
        """
        today = date.today()
        logger.info(f"종목 유니버스 재구성 시작 — {today}")

        # 오늘 기존 데이터 초기화
        with get_conn() as conn:
            conn.execute("DELETE FROM universe WHERE active_date = ?", (str(today),))

        rows: list[tuple] = []

        # KOSPI 200
        rows += _fetch_kospi200(today)
        logger.info(f"KOSPI 200 수집 완료: {len([r for r in rows if r[3] == 'kospi200'])}종목")

        # KOSDAQ 150
        kosdaq_rows = _fetch_kosdaq150(today)
        rows += kosdaq_rows
        logger.info(f"KOSDAQ 150 수집 완료: {len(kosdaq_rows)}종목")

        # 거래량 Top 100 (중복 제거 후 추가)
        existing_tickers = {r[0] for r in rows}
        top_rows = _fetch_volume_top100(today, exclude=existing_tickers)
        rows += top_rows
        logger.info(f"거래량 Top 100 추가: {len(top_rows)}종목 (신규)")

        # DB 저장
        with get_conn() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO universe (ticker, name, market, reason, active_date) VALUES (?,?,?,?,?)",
                rows,
            )

        total = len(rows)
        logger.info(f"유니버스 확정: 총 {total}종목 ({today})")
        return total

    def get_today(self) -> list[str]:
        """오늘 유니버스 종목 코드 목록 반환."""
        today = str(date.today())
        rows = fetch_all(
            "SELECT ticker FROM universe WHERE active_date = ? ORDER BY ticker",
            (today,),
        )
        return [row["ticker"] for row in rows]

    def get_today_count(self) -> int:
        """오늘 유니버스 종목 수 반환."""
        return len(self.get_today())

    def add_disclosure_ticker(self, ticker: str, name: str = "") -> bool:
        """
        공시 발생 종목을 유니버스에 즉시 추가.

        Returns:
            True if 새로 추가됨, False if 이미 존재
        """
        today = str(date.today())
        try:
            with get_conn() as conn:
                cur = conn.execute(
                    "SELECT ticker FROM universe WHERE ticker=? AND active_date=?",
                    (ticker, today),
                )
                if cur.fetchone():
                    return False

                conn.execute(
                    "INSERT OR IGNORE INTO universe (ticker, name, market, reason, active_date) VALUES (?,?,?,?,?)",
                    (ticker, name, "UNKNOWN", "disclosure", today),
                )
            logger.info(f"공시 종목 유니버스 편입: {ticker} {name}")
            return True
        except Exception as e:
            logger.error(f"공시 종목 추가 오류 [{ticker}]: {e}")
            return False

    def is_in_universe(self, ticker: str) -> bool:
        """종목이 오늘 유니버스에 포함되어 있는지 확인."""
        today = str(date.today())
        row = fetch_all(
            "SELECT 1 FROM universe WHERE ticker=? AND active_date=? LIMIT 1",
            (ticker, today),
        )
        return len(row) > 0

    # ──────────────────────────────────────────
    # KIND RSS 공시 실시간 감시
    # ──────────────────────────────────────────

    def start_disclosure_watcher(self, interval_sec: int = 120) -> None:
        """KIND RSS 공시 감시 스레드 시작 (2분 주기)."""
        if self._watcher_thread and self._watcher_thread.is_alive():
            return
        self._stop_event.clear()
        self._watcher_thread = threading.Thread(
            target=self._watch_disclosures,
            args=(interval_sec,),
            daemon=True,
            name="universe-disclosure-watcher",
        )
        self._watcher_thread.start()
        logger.info("KIND RSS 공시 감시 시작")

    def stop_disclosure_watcher(self) -> None:
        self._stop_event.set()

    def _watch_disclosures(self, interval_sec: int) -> None:
        while not self._stop_event.is_set():
            try:
                self._fetch_and_add_disclosures()
            except Exception as e:
                logger.warning(f"KIND RSS 수집 오류: {e}")
            self._stop_event.wait(timeout=interval_sec)

    def _fetch_and_add_disclosures(self) -> None:
        """KIND 공시 페이지에서 최신 공시를 파싱해 유니버스에 추가."""
        try:
            from html.parser import HTMLParser

            req = Request(_KIND_DISCLOSURE_URL, headers=_HEADERS)
            with urlopen(req, timeout=15) as resp:
                raw = resp.read()
            # KIND 응답은 EUC-KR 인코딩
            html = raw.decode("euc-kr", errors="replace")
        except Exception as e:
            logger.warning(f"KIND 공시 페이지 요청 실패: {e}")
            return

        # HTML에서 종목 코드(6자리 숫자) + 종목명 추출
        # KIND 페이지 패턴: isuCd=000000 또는 stockCd=000000
        code_patterns = [
            re.compile(r'isuCd=(\d{6})'),
            re.compile(r'stockCd=(\d{6})'),
            re.compile(r'종목코드[^0-9]*(\d{6})'),
        ]
        # 공시 항목 식별을 위한 고유 키: (ticker, title_hash)
        entries_found = 0
        for pattern in code_patterns:
            for match in pattern.finditer(html):
                ticker = match.group(1)
                key = ticker
                if key in self._seen_disclosures:
                    continue
                self._seen_disclosures.add(key)
                self.add_disclosure_ticker(ticker)
                entries_found += 1

        if entries_found:
            logger.debug(f"KIND 공시 {entries_found}건 신규 종목 추가")


# ──────────────────────────────────────────────
# 데이터 수집 헬퍼
# ──────────────────────────────────────────────

def _fetch_kospi200(today: date) -> list[tuple]:
    """FinanceDataReader로 KOSPI 시가총액 상위 200 종목 수집 (재시도 포함)."""
    try:
        df = retry_call(fdr.StockListing, "KOSPI", max_attempts=3, base_delay=3.0)
        # Marcap(시가총액) 내림차순 정렬 후 상위 200
        sort_col = "Marcap" if "Marcap" in df.columns else ("MktCap" if "MktCap" in df.columns else None)
        if sort_col:
            df = df.sort_values(sort_col, ascending=False)
        df = df.head(200)
        return [
            (str(row["Code"]).zfill(6), row.get("Name", ""), "KOSPI", "kospi200", str(today))
            for _, row in df.iterrows()
            if row.get("Code")
        ]
    except Exception as e:
        logger.error(f"KOSPI 200 수집 오류: {e}")
        return []


def _fetch_kosdaq150(today: date) -> list[tuple]:
    """FinanceDataReader로 KOSDAQ 시가총액 상위 150 종목 수집 (재시도 포함)."""
    try:
        df = retry_call(fdr.StockListing, "KOSDAQ", max_attempts=3, base_delay=3.0)
        sort_col = "Marcap" if "Marcap" in df.columns else ("MktCap" if "MktCap" in df.columns else None)
        if sort_col:
            df = df.sort_values(sort_col, ascending=False)
        df = df.head(150)
        return [
            (str(row["Code"]).zfill(6), row.get("Name", ""), "KOSDAQ", "kosdaq150", str(today))
            for _, row in df.iterrows()
            if row.get("Code")
        ]
    except Exception as e:
        logger.error(f"KOSDAQ 150 수집 오류: {e}")
        return []


def _fetch_volume_top100(today: date, exclude: set[str]) -> list[tuple]:
    """
    FinanceDataReader로 전체 KOSPI·KOSDAQ 종목 중
    전일 거래대금 상위 100종목 수집 (유니버스에 없는 종목만).
    """
    try:
        rows = []
        for market in ("KOSPI", "KOSDAQ"):
            df = retry_call(fdr.StockListing, market, max_attempts=3, base_delay=3.0)
            if "Amount" not in df.columns:
                # 거래대금 컬럼 없을 경우 Volume 대체
                sort_col = "Volume" if "Volume" in df.columns else None
            else:
                sort_col = "Amount"

            if sort_col:
                df = df.sort_values(sort_col, ascending=False)

            for _, row in df.iterrows():
                ticker = str(row.get("Code", "")).zfill(6)
                if not ticker or ticker in exclude:
                    continue
                rows.append(
                    (ticker, row.get("Name", ""), market, "volume_top", str(today))
                )
                if len(rows) >= 100:
                    break
            if len(rows) >= 100:
                break

        return rows[:100]
    except Exception as e:
        logger.error(f"거래량 Top 100 수집 오류: {e}")
        return []


