"""
collector.py — 글로벌 시황팀 데이터 수집 모듈

수집 대상:
  - 미국 3대 지수 (S&P500, NASDAQ, Dow)
  - VIX 공포지수
  - WTI 원유, 금
  - 환율 (USD/KRW, JPY/KRW, EUR/KRW)
  - 미국 10년물 국채 금리
  - 주요 미국 기술주 (NVDA, AAPL, TSM, MSFT, GOOGL)
  - 미국 경제지표 발표 일정 (FRED API)

외부 의존: yfinance, fredapi
KIS API 불필요 — 독립 실행 가능.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import yfinance as yf

from src.config.settings import settings
from src.utils.logger import get_logger
from src.utils.retry import with_retry

logger = get_logger(__name__)


# ── 수집 대상 심볼 정의 ─────────────────────────────────────

_US_INDEX = {
    "sp500": "^GSPC",
    "nasdaq": "^IXIC",
    "dow": "^DJI",
}

_VOLATILITY = {
    "vix": "^VIX",
}

_COMMODITIES = {
    "wti_oil": "CL=F",
    "gold": "GC=F",
}

_FX = {
    "usd_krw": "KRW=X",
    "jpy_krw": "JPYKRW=X",
    "eur_krw": "EURKRW=X",
}

_RATES = {
    "us_10y": "^TNX",
}

_US_TECH = {
    "NVDA": "NVDA",
    "AAPL": "AAPL",
    "MSFT": "MSFT",
    "GOOGL": "GOOGL",
    "TSM": "TSM",
    "META": "META",
}


# ── 데이터 클래스 ────────────────────────────────────────────

@dataclass
class GlobalMarketData:
    """글로벌 시황팀 수집 결과."""
    timestamp: str = ""

    # 미국 지수
    sp500_price: float = 0.0
    sp500_change: float = 0.0       # 등락률 (%)
    nasdaq_price: float = 0.0
    nasdaq_change: float = 0.0
    dow_price: float = 0.0
    dow_change: float = 0.0

    # 공포지수
    vix: float = 0.0

    # 원자재
    wti_oil: float = 0.0
    gold: float = 0.0

    # 환율
    usd_krw: float = 0.0
    jpy_krw: float = 0.0
    eur_krw: float = 0.0

    # 금리
    us_10y_yield: float = 0.0

    # 주요 미국 기술주
    us_tech: dict[str, float] = field(default_factory=dict)  # {ticker: change_pct}

    # 경제지표 이벤트 (오늘 + 내일)
    upcoming_events: list[str] = field(default_factory=list)

    # 수집 오류 종목
    errors: list[str] = field(default_factory=list)


# ── 수집 함수 ────────────────────────────────────────────────

def _fetch_change_pct(ticker: str) -> tuple[float, float]:
    """
    yfinance로 현재가와 전일 대비 등락률(%) 반환.
    네트워크 오류 시 최대 3회 재시도.

    Returns:
        (current_price, change_pct)
    """
    @with_retry(max_attempts=3, base_delay=3.0, max_delay=15.0)
    def _inner() -> tuple[float, float]:
        t = yf.Ticker(ticker)
        info = t.fast_info
        price = float(info.last_price or 0)
        prev_close = float(info.previous_close or price)
        change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0.0
        return round(price, 4), round(change_pct, 3)

    try:
        return _inner()
    except Exception as e:
        logger.warning(f"yfinance 최종 실패 [{ticker}]: {e}")
        return 0.0, 0.0


def _fetch_upcoming_events() -> list[str]:
    """
    FRED API에서 향후 2일 이내 미국 주요 경제지표 발표일 조회.
    FRED_API_KEY 없으면 빈 리스트 반환.
    """
    if not settings.FRED_API_KEY:
        return []

    try:
        from fredapi import Fred
        fred = Fred(api_key=settings.FRED_API_KEY)
        today = datetime.now().date()
        end = today + timedelta(days=2)

        # FOMC, CPI, 고용지표 등 주요 시리즈
        watched_series = {
            "FEDFUNDS": "FOMC 기준금리",
            "CPIAUCSL": "미국 CPI",
            "UNRATE": "미국 실업률",
            "PAYEMS": "비농업 고용지표",
            "T10YIE": "미국 10년 기대 인플레이션",
        }

        events = []
        for series_id, name in watched_series.items():
            try:
                releases = fred.get_series_latest_release(series_id)
                if releases is not None:
                    latest_date = releases.index[-1].date() if hasattr(releases.index[-1], 'date') else today
                    if today <= latest_date <= end:
                        events.append(f"{name} 발표 ({latest_date})")
            except Exception:
                pass

        return events
    except ImportError:
        logger.debug("fredapi 미설치 — 경제지표 일정 스킵")
        return []
    except Exception as e:
        logger.warning(f"FRED API 오류: {e}")
        return []


def collect() -> GlobalMarketData:
    """
    글로벌 시황 데이터 전체 수집.

    Returns:
        GlobalMarketData 인스턴스
    """
    data = GlobalMarketData(timestamp=datetime.now().isoformat(timespec="seconds"))
    logger.info("글로벌 시황 데이터 수집 시작")

    # 미국 3대 지수
    data.sp500_price, data.sp500_change = _fetch_change_pct(_US_INDEX["sp500"])
    data.nasdaq_price, data.nasdaq_change = _fetch_change_pct(_US_INDEX["nasdaq"])
    data.dow_price, data.dow_change = _fetch_change_pct(_US_INDEX["dow"])

    # VIX
    data.vix, _ = _fetch_change_pct(_VOLATILITY["vix"])

    # 원자재
    data.wti_oil, _ = _fetch_change_pct(_COMMODITIES["wti_oil"])
    data.gold, _ = _fetch_change_pct(_COMMODITIES["gold"])

    # 환율
    data.usd_krw, _ = _fetch_change_pct(_FX["usd_krw"])
    data.jpy_krw, _ = _fetch_change_pct(_FX["jpy_krw"])
    data.eur_krw, _ = _fetch_change_pct(_FX["eur_krw"])

    # 금리
    data.us_10y_yield, _ = _fetch_change_pct(_RATES["us_10y"])

    # 미국 기술주 등락률
    for name, symbol in _US_TECH.items():
        _, chg = _fetch_change_pct(symbol)
        data.us_tech[name] = chg

    # 경제지표 일정
    data.upcoming_events = _fetch_upcoming_events()

    logger.info(
        f"글로벌 수집 완료 — S&P500 {data.sp500_change:+.2f}% "
        f"| VIX {data.vix:.1f} | USD/KRW {data.usd_krw:.0f}"
    )
    return data
