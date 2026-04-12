"""
collector.py — 국내 시황팀 데이터 수집 모듈

수집 대상:
  - KIS API: KOSPI / KOSDAQ 지수 현재가·등락률
  - KIS API: 투자자별 매매동향 (외국인·기관 순매수)
  - FinanceDataReader: KOSPI / KOSDAQ 과거 지수 (이동평균·추세)
  - 네이버금융: 국내 증시 주요 뉴스 (감성 분석 원문 제공)

KIS API는 반드시 KISGateway 경유.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

import FinanceDataReader as fdr
import pandas as pd

from src.infra.kis_gateway import KISGateway, RequestPriority
from src.utils.retry import retry_call, with_retry
from src.utils.logger import get_logger

logger = get_logger(__name__)

_NAVER_MARKET_NEWS_URL = (
    "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://finance.naver.com/",
}

# KIS API 경로 (투자자별 매매동향)
_KIS_INVESTOR_PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"
# KIS API 경로 (지수 현재가)
_KIS_INDEX_PATH = "/uapi/domestic-stock/v1/quotations/inquire-index-price"


# ── 데이터 클래스 ─────────────────────────────────────────────

@dataclass
class IndexData:
    """지수 단일 데이터."""
    name: str = ""
    current: float = 0.0
    change: float = 0.0       # 등락 포인트
    change_pct: float = 0.0   # 등락률 (%)
    volume: int = 0           # 거래량


@dataclass
class InvestorFlow:
    """투자자별 매매동향 (단위: 억원)."""
    foreign_net: float = 0.0      # 외국인 순매수
    institutional_net: float = 0.0  # 기관 순매수
    individual_net: float = 0.0   # 개인 순매수


@dataclass
class TrendData:
    """이동평균·추세 데이터 (FinanceDataReader 기반)."""
    ma5: float = 0.0
    ma20: float = 0.0
    ma60: float = 0.0
    above_ma20: bool = False     # 현재 지수 > 20일선
    trend_direction: str = "neutral"  # up | down | neutral


@dataclass
class MarketNews:
    """뉴스 단건."""
    title: str = ""
    url: str = ""
    summary: str = ""


@dataclass
class DomesticMarketData:
    """국내 시황팀 수집 결과."""
    timestamp: str = ""

    # 지수
    kospi: IndexData = field(default_factory=IndexData)
    kosdaq: IndexData = field(default_factory=IndexData)

    # 투자자별 매매동향
    kospi_flow: InvestorFlow = field(default_factory=InvestorFlow)
    kosdaq_flow: InvestorFlow = field(default_factory=InvestorFlow)

    # 기술적 지표
    kospi_trend: TrendData = field(default_factory=TrendData)
    kosdaq_trend: TrendData = field(default_factory=TrendData)

    # 뉴스 (최대 10건)
    news: list[MarketNews] = field(default_factory=list)

    # 수집 오류
    errors: list[str] = field(default_factory=list)


# ── KIS API 수집 ──────────────────────────────────────────────

def _fetch_index_from_kis(market: str) -> IndexData:
    """
    KIS API로 KOSPI/KOSDAQ 지수 현재가 조회.

    market: 'KOSPI' | 'KOSDAQ'
    """
    gw = KISGateway()
    # KIS 지수 코드: KOSPI=0001, KOSDAQ=1001
    iscd = "0001" if market == "KOSPI" else "1001"
    name = market

    try:
        resp = gw.request(
            method="GET",
            path=_KIS_INDEX_PATH,
            params={"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": iscd},
            tr_id="FHPUP02100000",
            priority=RequestPriority.DATA_COLLECTION,
        )
        output = resp.get("output", {})
        current = float(output.get("bstp_nmix_prpr", 0) or 0)
        change = float(output.get("bstp_nmix_prdy_vrss", 0) or 0)
        change_pct = float(output.get("bstp_nmix_prdy_ctrt", 0) or 0)
        volume = int(output.get("acml_vol", 0) or 0)
        return IndexData(
            name=name,
            current=current,
            change=change,
            change_pct=change_pct,
            volume=volume,
        )
    except Exception as e:
        logger.warning(f"KIS 지수 조회 실패 [{market}]: {e}")
        return _fetch_index_fallback(market)


def _fetch_index_fallback(market: str) -> IndexData:
    """KIS API 실패 시 FinanceDataReader로 전일 종가 대체 (재시도 포함)."""
    try:
        symbol = "KS11" if market == "KOSPI" else "KQ11"
        df = retry_call(
            fdr.DataReader, symbol, datetime.now().date() - timedelta(days=5),
            max_attempts=3, base_delay=3.0,
        )
        if df.empty:
            return IndexData(name=market)
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else df.iloc[-1]
        current = float(last["Close"])
        prev_close = float(prev["Close"])
        change = current - prev_close
        change_pct = change / prev_close * 100 if prev_close else 0.0
        return IndexData(
            name=market,
            current=round(current, 2),
            change=round(change, 2),
            change_pct=round(change_pct, 3),
            volume=int(last.get("Volume", 0)),
        )
    except Exception as e:
        logger.warning(f"FDR 지수 폴백 실패 [{market}]: {e}")
        return IndexData(name=market)


def _fetch_investor_flow(market: str) -> InvestorFlow:
    """
    KIS API로 투자자별 매매동향 조회.
    market: 'KOSPI' | 'KOSDAQ'
    """
    gw = KISGateway()
    mktdiv = "J" if market == "KOSPI" else "Q"  # KIS 시장 구분

    try:
        resp = gw.request(
            method="GET",
            path=_KIS_INVESTOR_PATH,
            params={
                "FID_COND_MRKT_DIV_CODE": mktdiv,
                "FID_INPUT_ISCD": "0000",  # 전체 시장
            },
            tr_id="FHKST01010900",
            priority=RequestPriority.DATA_COLLECTION,
        )
        output = resp.get("output", {})

        def _to_bn(val: str) -> float:
            """단위 변환: KIS는 백만원 단위 → 억원으로 반환"""
            try:
                return round(float(val or 0) / 100, 1)
            except (ValueError, TypeError):
                return 0.0

        return InvestorFlow(
            foreign_net=_to_bn(output.get("frgn_ntby_qty", 0)),
            institutional_net=_to_bn(output.get("orgn_ntby_qty", 0)),
            individual_net=_to_bn(output.get("indv_ntby_qty", 0)),
        )
    except Exception as e:
        logger.warning(f"KIS 투자자 매매동향 조회 실패 [{market}]: {e}")
        return InvestorFlow()


# ── FinanceDataReader 기술적 지표 ─────────────────────────────

def _fetch_trend(market: str, current_price: float) -> TrendData:
    """
    FinanceDataReader로 60일 지수 데이터 조회 → 이동평균·추세 계산 (재시도 포함).
    market: 'KOSPI' | 'KOSDAQ'
    """
    symbol = "KS11" if market == "KOSPI" else "KQ11"
    try:
        end = datetime.now().date()
        start = end - timedelta(days=100)  # 60일 MA 계산용 여유분
        df = retry_call(fdr.DataReader, symbol, start, end, max_attempts=3, base_delay=3.0)
        if df.empty or len(df) < 5:
            return TrendData()

        closes = df["Close"].astype(float)
        ma5 = float(closes.rolling(5).mean().iloc[-1]) if len(closes) >= 5 else 0.0
        ma20 = float(closes.rolling(20).mean().iloc[-1]) if len(closes) >= 20 else 0.0
        ma60 = float(closes.rolling(60).mean().iloc[-1]) if len(closes) >= 60 else 0.0

        ref = current_price if current_price > 0 else float(closes.iloc[-1])
        above_ma20 = ref > ma20 if ma20 > 0 else False

        # 추세: 5일선 > 20일선 → up, 반대 → down
        if ma5 > 0 and ma20 > 0:
            trend = "up" if ma5 > ma20 else "down"
        else:
            trend = "neutral"

        return TrendData(
            ma5=round(ma5, 2),
            ma20=round(ma20, 2),
            ma60=round(ma60, 2),
            above_ma20=above_ma20,
            trend_direction=trend,
        )
    except Exception as e:
        logger.warning(f"FDR 이동평균 계산 실패 [{market}]: {e}")
        return TrendData()


# ── 네이버금융 뉴스 수집 ──────────────────────────────────────

def _fetch_naver_market_news(max_items: int = 10) -> list[MarketNews]:
    """네이버금융 국내 증시 뉴스 최신 목록 수집 (BeautifulSoup 없이 regex). 재시도 포함."""
    @with_retry(max_attempts=3, base_delay=3.0, max_delay=15.0)
    def _download() -> bytes:
        req = Request(_NAVER_MARKET_NEWS_URL, headers=_HEADERS)
        with urlopen(req, timeout=10) as resp:
            return resp.read()

    try:
        raw = _download()
        html = raw.decode("euc-kr", errors="replace")

        # 뉴스 링크·제목 추출 패턴
        # 네이버금융 뉴스 항목: <dt class="..."><a href="...">제목</a></dt>
        pattern = re.compile(
            r'<dt[^>]*>\s*<a[^>]+href=["\']([^"\']+news[^"\']+)["\'][^>]*>\s*([^<]+)\s*</a>',
            re.IGNORECASE,
        )
        items = pattern.findall(html)
        news = []
        seen_urls: set[str] = set()
        for href, title in items:
            title = title.strip()
            # 절대 URL 변환
            if href.startswith("http"):
                url = href
            else:
                url = "https://finance.naver.com" + href
            if url in seen_urls or not title:
                continue
            seen_urls.add(url)
            news.append(MarketNews(title=title, url=url))
            if len(news) >= max_items:
                break

        logger.debug(f"네이버금융 뉴스 {len(news)}건 수집")
        return news
    except Exception as e:
        logger.warning(f"네이버금융 뉴스 수집 실패: {e}")
        return []


# ── 통합 수집 ─────────────────────────────────────────────────

def collect() -> DomesticMarketData:
    """
    국내 시황 데이터 전체 수집.

    Returns:
        DomesticMarketData 인스턴스
    """
    data = DomesticMarketData(timestamp=datetime.now().isoformat(timespec="seconds"))
    logger.info("국내 시황 데이터 수집 시작")

    # 1. KIS API — 지수 현재가
    data.kospi = _fetch_index_from_kis("KOSPI")
    data.kosdaq = _fetch_index_from_kis("KOSDAQ")

    # 2. KIS API — 투자자별 매매동향
    data.kospi_flow = _fetch_investor_flow("KOSPI")
    data.kosdaq_flow = _fetch_investor_flow("KOSDAQ")

    # 3. FDR — 이동평균·추세
    data.kospi_trend = _fetch_trend("KOSPI", data.kospi.current)
    data.kosdaq_trend = _fetch_trend("KOSDAQ", data.kosdaq.current)

    # 4. 뉴스
    data.news = _fetch_naver_market_news()

    logger.info(
        f"국내 시황 수집 완료 — "
        f"KOSPI {data.kospi.current:,.2f} ({data.kospi.change_pct:+.2f}%) | "
        f"KOSDAQ {data.kosdaq.current:,.2f} ({data.kosdaq.change_pct:+.2f}%) | "
        f"외국인 {data.kospi_flow.foreign_net:+.0f}억 | "
        f"기관 {data.kospi_flow.institutional_net:+.0f}억"
    )
    return data
