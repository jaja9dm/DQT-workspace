"""
us_market.py — 미국 시장 일일 스냅샷 수집 인프라

어시스턴트 모델 전환 (2026-05-12) Phase 3-1.

역할:
  매일 아침 한국장 시작 전(08:30~08:40 권장) 미국 야간 종가 데이터를 한 번에 수집해
  us_market_daily 테이블에 적재한다. morning_brief.py가 이를 읽어 텔레그램 브리핑에 활용.

수집 항목:
  - 3대 지수: S&P500, NASDAQ, Dow Jones (종가/등락률)
  - VIX 공포지수 + 변화량
  - US10Y 국채 금리
  - 주요 ETF: SOXX(반도체), LIT(2차전지)
  - 거래량 상위 미국 종목 (yfinance most_actives)
  - 핵심 종목 8개: NVDA, TSM, AAPL, MSFT, GOOGL, TSLA, AMD, META

외부 의존: yfinance
KIS API 불필요 — 독립 실행 가능.

폴백 전략:
  - 개별 심볼 실패 → 0.0 / None 으로 채우고 errors 로깅
  - DB 저장 실패 → 이전 행 유지 (덮어쓰기 X)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime

import yfinance as yf

from src.infra.database import execute, fetch_one
from src.utils.logger import get_logger
from src.utils.retry import with_retry

logger = get_logger(__name__)


# ── 수집 대상 심볼 ────────────────────────────────────────────

_INDEX_SYMBOLS = {
    "sp500":  "^GSPC",
    "nasdaq": "^IXIC",
    "dow":    "^DJI",
}

_VIX_SYMBOL = "^VIX"
_US10Y_SYMBOL = "^TNX"

_ETF_SYMBOLS = {
    "soxx": "SOXX",   # 반도체
    "lit":  "LIT",    # 리튬/2차전지
}

# 핵심 미국 주요 종목 (한국 시장에 가장 큰 영향)
_KEY_STOCKS: dict[str, str] = {
    "NVDA":  "엔비디아",
    "TSM":   "TSMC",
    "AAPL":  "애플",
    "MSFT":  "마이크로소프트",
    "GOOGL": "구글",
    "TSLA":  "테슬라",
    "AMD":   "AMD",
    "META":  "메타",
}

# 거래량 상위 후보군 (yfinance가 most_actives 정식 API를 제공하지 않으므로
# 시가총액·관심도 높은 종목 풀에서 거래량 상위 N개 추출).
_VOLUME_POOL: dict[str, str] = {
    "NVDA":  "엔비디아",
    "TSLA":  "테슬라",
    "AAPL":  "애플",
    "AMD":   "AMD",
    "MSFT":  "마이크로소프트",
    "GOOGL": "구글",
    "META":  "메타",
    "AMZN":  "아마존",
    "TSM":   "TSMC",
    "NFLX":  "넷플릭스",
    "INTC":  "인텔",
    "PLTR":  "팔란티어",
    "AVGO":  "브로드컴",
    "MU":    "마이크론",
    "COIN":  "코인베이스",
    "SMCI":  "수퍼마이크로",
    "ARM":   "ARM",
    "QQQ":   "나스닥100 ETF",
    "SPY":   "S&P500 ETF",
}


# ── 데이터 클래스 ─────────────────────────────────────────────

@dataclass
class USMarketSnapshot:
    """미국 시장 일일 스냅샷."""
    date: str = ""             # YYYY-MM-DD (한국시간 기준 — 미국 야간 종가 적재일)

    # 3대 지수
    sp500_close: float = 0.0
    sp500_chg_pct: float = 0.0
    nasdaq_close: float = 0.0
    nasdaq_chg_pct: float = 0.0
    dow_close: float = 0.0
    dow_chg_pct: float = 0.0

    # VIX
    vix: float = 0.0
    vix_chg: float = 0.0       # 전일 대비 절대 변화량 (포인트)

    # US10Y (yfinance ^TNX는 yield × 10 형태 — 4.50% → 45.0)
    us10y_yield: float = 0.0   # %로 변환된 값

    # ETF
    soxx: float = 0.0
    soxx_chg_pct: float = 0.0
    lit: float = 0.0
    lit_chg_pct: float = 0.0

    # 거래량 상위 [{ticker, name_kr, volume, chg_pct}, ...]
    top_volume_tickers: list[dict] = field(default_factory=list)

    # 핵심 주요 종목 {ticker: {name_kr, close, chg_pct}}
    key_stocks: dict[str, dict] = field(default_factory=dict)

    # 수집 오류 누적
    errors: list[str] = field(default_factory=list)


# ── 내부 헬퍼 ────────────────────────────────────────────────

@with_retry(max_attempts=3, base_delay=2.0, max_delay=10.0)
def _fast_quote(symbol: str) -> tuple[float, float, int]:
    """
    yfinance fast_info로 (현재가, 전일대비 등락률%, 거래량) 반환.
    실패 시 (0.0, 0.0, 0) 폴백.
    """
    t = yf.Ticker(symbol)
    info = t.fast_info
    price = float(info.last_price or 0)
    prev_close = float(info.previous_close or price)
    chg_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0.0
    volume = int(info.last_volume or 0)
    return round(price, 4), round(chg_pct, 3), volume


def _safe_quote(symbol: str, errors: list[str]) -> tuple[float, float, int]:
    """예외를 흡수해 errors 리스트에 누적. (0,0,0) 폴백."""
    try:
        return _fast_quote(symbol)
    except Exception as e:
        msg = f"{symbol}: {e}"
        errors.append(msg)
        logger.warning(f"yfinance 조회 실패 — {msg}")
        return 0.0, 0.0, 0


# ── 공개 API ─────────────────────────────────────────────────

def fetch_us_market_data() -> USMarketSnapshot:
    """
    미국 시장 종합 데이터를 한 번에 수집해 USMarketSnapshot 반환.

    호출 예시:
        snap = fetch_us_market_data()
        save_us_market_snapshot(snap)
    """
    snap = USMarketSnapshot(date=str(date.today()))
    logger.info("미국 시장 데이터 수집 시작")

    # 1. 3대 지수
    sp_p, sp_c, _ = _safe_quote(_INDEX_SYMBOLS["sp500"], snap.errors)
    nd_p, nd_c, _ = _safe_quote(_INDEX_SYMBOLS["nasdaq"], snap.errors)
    dw_p, dw_c, _ = _safe_quote(_INDEX_SYMBOLS["dow"], snap.errors)
    snap.sp500_close,  snap.sp500_chg_pct  = sp_p, sp_c
    snap.nasdaq_close, snap.nasdaq_chg_pct = nd_p, nd_c
    snap.dow_close,    snap.dow_chg_pct    = dw_p, dw_c

    # 2. VIX (변화량은 절대 포인트)
    vix_p, vix_pct, _ = _safe_quote(_VIX_SYMBOL, snap.errors)
    snap.vix = vix_p
    # 전일 종가 역산: pct = (cur - prev)/prev * 100 → prev = cur / (1 + pct/100)
    if vix_p > 0 and vix_pct != 0:
        prev = vix_p / (1 + vix_pct / 100.0)
        snap.vix_chg = round(vix_p - prev, 3)
    else:
        snap.vix_chg = 0.0

    # 3. US10Y (yfinance ^TNX는 이미 yield × 10 형태가 아니라 yield × 1로 들어옴, 4.5%면 4.5)
    y10_p, _, _ = _safe_quote(_US10Y_SYMBOL, snap.errors)
    snap.us10y_yield = y10_p

    # 4. ETF
    soxx_p, soxx_c, _ = _safe_quote(_ETF_SYMBOLS["soxx"], snap.errors)
    lit_p,  lit_c,  _ = _safe_quote(_ETF_SYMBOLS["lit"],  snap.errors)
    snap.soxx, snap.soxx_chg_pct = soxx_p, soxx_c
    snap.lit,  snap.lit_chg_pct  = lit_p,  lit_c

    # 5. 핵심 종목 8개
    for ticker, name_kr in _KEY_STOCKS.items():
        price, chg, _ = _safe_quote(ticker, snap.errors)
        snap.key_stocks[ticker] = {
            "name_kr": name_kr,
            "close": price,
            "chg_pct": chg,
        }

    # 6. 거래량 상위 N
    snap.top_volume_tickers = fetch_us_volume_top(n=10)

    err_cnt = len(snap.errors)
    logger.info(
        f"미국 시장 수집 완료 — S&P {snap.sp500_chg_pct:+.2f}% | "
        f"NASDAQ {snap.nasdaq_chg_pct:+.2f}% | VIX {snap.vix:.1f} | "
        f"오류 {err_cnt}건"
    )
    return snap


def fetch_us_volume_top(n: int = 10) -> list[dict]:
    """
    yfinance 후보군에서 거래량 상위 N 종목 반환.

    Returns:
        [{ticker, name_kr, volume, chg_pct}, ...] — 거래량 내림차순
    """
    results: list[dict] = []
    for ticker, name_kr in _VOLUME_POOL.items():
        try:
            price, chg, volume = _fast_quote(ticker)
            if volume <= 0:
                continue
            results.append({
                "ticker":  ticker,
                "name_kr": name_kr,
                "volume":  volume,
                "chg_pct": chg,
                "close":   price,
            })
        except Exception as e:
            logger.debug(f"volume_top 조회 실패 [{ticker}]: {e}")
            continue

    results.sort(key=lambda x: x["volume"], reverse=True)
    return results[:n]


def save_us_market_snapshot(snap: USMarketSnapshot) -> None:
    """
    us_market_daily 테이블에 INSERT OR REPLACE.
    snap.date가 비어 있으면 오늘 날짜로 채움.
    """
    if not snap.date:
        snap.date = str(date.today())

    # errors가 너무 많으면(절반 이상) 덮어쓰기 금지 — 직전 데이터 보호
    total_attempts = (
        3                # 지수
        + 1              # VIX
        + 1              # US10Y
        + 2              # ETF
        + len(_KEY_STOCKS)
    )
    if len(snap.errors) >= total_attempts * 0.5:
        logger.warning(
            f"미국 시장 수집 오류 과다({len(snap.errors)}/{total_attempts}) "
            f"— 이전 행 유지, DB 저장 스킵"
        )
        return

    try:
        execute(
            """
            INSERT OR REPLACE INTO us_market_daily (
                date,
                sp500_close, sp500_chg_pct,
                nasdaq_close, nasdaq_chg_pct,
                dow_close, dow_chg_pct,
                vix, vix_chg,
                us10y_yield,
                soxx, soxx_chg_pct,
                lit,  lit_chg_pct,
                top_volume_tickers,
                key_stocks
            ) VALUES (
                :date,
                :sp500_close, :sp500_chg_pct,
                :nasdaq_close, :nasdaq_chg_pct,
                :dow_close, :dow_chg_pct,
                :vix, :vix_chg,
                :us10y_yield,
                :soxx, :soxx_chg_pct,
                :lit,  :lit_chg_pct,
                :top_volume_tickers,
                :key_stocks
            )
            """,
            (
                snap.date,
                snap.sp500_close, snap.sp500_chg_pct,
                snap.nasdaq_close, snap.nasdaq_chg_pct,
                snap.dow_close, snap.dow_chg_pct,
                snap.vix, snap.vix_chg,
                snap.us10y_yield,
                snap.soxx, snap.soxx_chg_pct,
                snap.lit,  snap.lit_chg_pct,
                json.dumps(snap.top_volume_tickers, ensure_ascii=False),
                json.dumps(snap.key_stocks, ensure_ascii=False),
            ),
        )
        logger.info(f"us_market_daily 저장 완료 [{snap.date}]")
    except Exception as e:
        logger.error(f"us_market_daily 저장 실패: {e}", exc_info=True)


def get_latest_us_snapshot() -> dict | None:
    """가장 최근 us_market_daily 행 반환 (조회 헬퍼)."""
    row = fetch_one(
        "SELECT * FROM us_market_daily ORDER BY date DESC LIMIT 1"
    )
    return dict(row) if row else None


# ── CLI 진입점 ────────────────────────────────────────────────

if __name__ == "__main__":
    snap = fetch_us_market_data()
    save_us_market_snapshot(snap)
    print(f"수집 완료: {snap.date}")
    print(f"  S&P500: {snap.sp500_close:,.2f} ({snap.sp500_chg_pct:+.2f}%)")
    print(f"  NASDAQ: {snap.nasdaq_close:,.2f} ({snap.nasdaq_chg_pct:+.2f}%)")
    print(f"  VIX: {snap.vix:.2f} ({snap.vix_chg:+.2f})")
    print(f"  US10Y: {snap.us10y_yield:.3f}%")
    print(f"  거래량 TOP: {[t['ticker'] for t in snap.top_volume_tickers]}")
    if snap.errors:
        print(f"  오류 {len(snap.errors)}건: {snap.errors[:3]}")
