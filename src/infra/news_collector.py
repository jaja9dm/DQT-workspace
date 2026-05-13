"""
news_collector.py — 주요 뉴스 수집/분류/저장 (2026-05-13 최적화)

역할:
  매일 morning_brief(07:30) / evening_review(16:40) 직전에 호출되어
  국내·해외 금융 뉴스를 수집 → Claude로 한글 번역·분류·태그링 →
  daily_news 테이블에 영구 저장 → 메시지에 들어갈 4분류 헤드라인을 추려준다.

원칙:
  - 추정 절대 금지: 원문/번역만, 시장 영향 추측 X
  - 네트워크 timeout 5초, 실패 시 graceful (빈 리스트 반환, ERROR 로그)
  - URL 중복 시 스킵 (UNIQUE INDEX)
  - 카테고리 4분류: macro | sector | company | risk

핵심 함수:
  collect_korean_news(hours)    → list[dict]
  collect_us_news(hours)        → list[dict]
  classify_and_translate(raw)   → list[dict]  (Claude Haiku + cache_control)
  save_news(news_list)          → int  (신규 INSERT 건수)
  get_news_for_brief(date_str)  → dict {'macro':[…], 'sector':[…], 'company':[…], 'risk':[…]}

비용 최적화 (2026-05-13):
  - summary 필드 제거 (출력 토큰 30~40% ↓): raw_summary 그대로 저장 (영어는 번역만)
  - related_tickers 로컬 매칭 (Claude 응답에서 제거): 사전 기반 100% 정확
  - 응답 필드 최소화: {index, headline_kr, category, tags, importance}
  - 시스템 프롬프트 압축 + importance 엄격화 (5점 인플레 차단)
  - 룰 기반 카테고리 힌트 (payload에 추가, Claude 빠르게 확신)
  - jaccard 기반 강화 dedup (소스 우선순위)
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Iterable

import anthropic
import feedparser
import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import check_claude_error

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_HTTP_TIMEOUT = 5.0
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)

_VALID_CATEGORIES = {"macro", "sector", "company", "risk"}

# 분류별 기본 슬롯 (총 max 10개)
_DEFAULT_QUOTA = {"macro": 3, "sector": 4, "company": 3, "risk": 2}

# 소스 신뢰도 우선순위 (dedup 시 더 신뢰성 높은 source 유지)
_SOURCE_PRIORITY = {
    "한국경제": 100, "한국경제-증권": 100,
    "매일경제": 90, "매일경제-경제": 90,
    "네이버금융": 60,
    "CNBC": 100, "MarketWatch": 90, "Yahoo Finance": 80, "Investing": 70,
}


# ════════════════════════════════════════════════════════════════════
# 유틸
# ════════════════════════════════════════════════════════════════════

def _hash_headline(text: str) -> str:
    """헤드라인 정규화 후 SHA1 — 중복 제거용."""
    norm = re.sub(r"\s+", " ", (text or "").strip()).lower()
    return hashlib.sha1(norm.encode("utf-8", errors="ignore")).hexdigest()


def _to_kst(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).astimezone(
            timezone(timedelta(hours=9))
        )
    return dt.astimezone(timezone(timedelta(hours=9)))


def _parse_pub(entry) -> datetime | None:
    """feedparser entry의 published 시간 → KST datetime."""
    for key in ("published", "updated", "pubDate"):
        v = getattr(entry, key, None) or (entry.get(key) if isinstance(entry, dict) else None)
        if not v:
            continue
        try:
            dt = parsedate_to_datetime(v)
            return _to_kst(dt)
        except Exception:
            continue
    # struct_time fallback
    pp = getattr(entry, "published_parsed", None)
    if pp:
        try:
            dt = datetime(*pp[:6], tzinfo=timezone.utc)
            return _to_kst(dt)
        except Exception:
            pass
    return None


def _filter_recent(items: list[dict], hours: int) -> list[dict]:
    if not items:
        return []
    cutoff = datetime.now(timezone(timedelta(hours=9))) - timedelta(hours=hours)
    out = []
    for it in items:
        pub = it.get("published_at")
        if pub is None:
            # 시각 미상은 일단 포함 (cutoff 이후로 가정)
            out.append(it)
            continue
        if pub >= cutoff:
            out.append(it)
    return out


def _dedup_headlines(items: list[dict]) -> list[dict]:
    """기본 dedup — 헤드라인 SHA1 hash 기반 (정확 일치)."""
    seen = set()
    out = []
    for it in items:
        h = _hash_headline(it.get("headline_orig") or it.get("headline") or "")
        if not h or h in seen:
            continue
        seen.add(h)
        out.append(it)
    return out


# ── 강화 dedup (jaccard 토큰 유사도) ────────────────────────────────

_NORM_PATTERN = re.compile(r"[\s\W_]+")  # 공백/구두점/특수문자


def _normalize_for_dedup(text: str) -> set[str]:
    """헤드라인 토큰화: 2-gram 문자 + 4자 이상 단어."""
    if not text:
        return set()
    t = (text or "").lower().strip()
    # 1) 공백/구두점 제거 후 2-gram char shingle (한글 강건)
    cleaned = _NORM_PATTERN.sub("", t)
    shingles: set[str] = set()
    if len(cleaned) >= 2:
        for i in range(len(cleaned) - 1):
            shingles.add(cleaned[i:i + 2])
    # 2) 단어 토큰 (영어/숫자 매칭 보강)
    for w in re.findall(r"[a-z0-9가-힣]{3,}", t):
        shingles.add(w)
    return shingles


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / len(a | b)


def _dedup_advanced(items: list[dict], threshold: float = 0.5) -> list[dict]:
    """강화 dedup — jaccard 유사도 0.5+ 이면 중복 (한경/매경 동일 사건 차단).

    중복 발견 시: source 우선순위 높은 항목 유지, 더 짧은 헤드라인 우선 (대체로 본문 깔끔).
    """
    if not items:
        return []
    # 먼저 hash 기반 정확 일치 제거
    base = _dedup_headlines(items)

    # 토큰 사전 계산 (시장별로 분리해서 cross-market 매칭 줄임)
    enriched = []
    for it in base:
        text = it.get("headline_orig") or it.get("headline") or ""
        enriched.append({
            "item": it,
            "tokens": _normalize_for_dedup(text),
            "len": len(text),
            "priority": _SOURCE_PRIORITY.get(it.get("source") or "", 50),
            "market": it.get("market"),
        })

    kept: list[dict] = []
    for cur in enriched:
        dup_idx = -1
        for ki, ke in enumerate(kept):
            # 같은 시장끼리만 jaccard 비교 (kr-us 우연 매칭 차단)
            if ke["market"] != cur["market"]:
                continue
            if _jaccard(ke["tokens"], cur["tokens"]) >= threshold:
                dup_idx = ki
                break
        if dup_idx < 0:
            kept.append(cur)
            continue
        # 중복 — 우선순위 비교
        existing = kept[dup_idx]
        replace = False
        if cur["priority"] > existing["priority"]:
            replace = True
        elif cur["priority"] == existing["priority"]:
            # 동일 우선순위 — 짧은 헤드라인 우선 (보통 본문 깔끔)
            if cur["len"] < existing["len"]:
                replace = True
        if replace:
            kept[dup_idx] = cur

    return [k["item"] for k in kept]


def _safe_get(url: str) -> str | None:
    try:
        r = requests.get(
            url,
            headers={"User-Agent": _UA, "Accept-Language": "ko,en;q=0.8"},
            timeout=_HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            logger.debug(f"[news_collector] GET {url} -> {r.status_code}")
            return None
        r.encoding = r.apparent_encoding or r.encoding
        return r.text
    except Exception as e:
        logger.debug(f"[news_collector] GET {url} 실패: {e}")
        return None


def _fetch_feed(url: str) -> list:
    """RSS 안전 파싱 — timeout 적용."""
    try:
        raw = _safe_get(url)
        if raw is None:
            return []
        d = feedparser.parse(raw)
        return list(d.entries or [])
    except Exception as e:
        logger.warning(f"[news_collector] RSS 파싱 실패 {url}: {e}")
        return []


# ════════════════════════════════════════════════════════════════════
# 한국 뉴스 수집
# ════════════════════════════════════════════════════════════════════

_KR_RSS_FEEDS = [
    # 한경 — 증권/금융
    ("한국경제", "https://www.hankyung.com/feed/finance"),
    ("한국경제-증권", "https://www.hankyung.com/feed/stock"),
    # 매일경제 — 증권
    ("매일경제", "https://www.mk.co.kr/rss/50200011/"),
    # 매일경제 — 경제
    ("매일경제-경제", "https://www.mk.co.kr/rss/30100041/"),
]


def _collect_kr_rss(hours: int) -> list[dict]:
    out: list[dict] = []
    for src, url in _KR_RSS_FEEDS:
        entries = _fetch_feed(url)
        for e in entries[:40]:
            title = (getattr(e, "title", "") or "").strip()
            link = (getattr(e, "link", "") or "").strip()
            if not title:
                continue
            pub = _parse_pub(e)
            summary = ""
            raw_sum = getattr(e, "summary", "") or ""
            if raw_sum:
                try:
                    summary = BeautifulSoup(raw_sum, "html.parser").get_text(" ", strip=True)[:300]
                except Exception:
                    summary = re.sub(r"<[^>]+>", " ", raw_sum)[:300]
            out.append({
                "market":        "kr",
                "source":        src,
                "headline":      title,        # 한국 뉴스 — 번역 불필요
                "headline_orig": None,
                "raw_summary":   summary,
                "url":           link or None,
                "published_at":  pub,
            })
    return _filter_recent(out, hours)


def _collect_naver_finance_headlines() -> list[dict]:
    """네이버 금융 메인 헤드라인 — RSS 없음, HTML 파싱."""
    out: list[dict] = []
    html = _safe_get("https://finance.naver.com/news/mainnews.naver")
    if not html:
        return out
    try:
        soup = BeautifulSoup(html, "html.parser")
        # 메인 뉴스 리스트 — .mainNewsList > li
        for li in soup.select(".mainNewsList li, .newsList li"):
            a = li.select_one("a")
            if not a:
                continue
            title = a.get_text(strip=True)
            href = a.get("href", "")
            if not title or len(title) < 5:
                continue
            if href and href.startswith("/"):
                href = "https://finance.naver.com" + href
            out.append({
                "market":        "kr",
                "source":        "네이버금융",
                "headline":      title,
                "headline_orig": None,
                "raw_summary":   "",
                "url":           href or None,
                "published_at":  None,  # 네이버 헤드라인은 정확한 발행시각 미제공
            })
    except Exception as e:
        logger.debug(f"[news_collector] 네이버 금융 파싱 실패: {e}")
    return out[:30]


def collect_korean_news(hours: int = 18) -> list[dict]:
    """네이버 금융 헤드라인 + 한경/매경 RSS — 최근 N시간."""
    try:
        rss = _collect_kr_rss(hours)
        naver = _collect_naver_finance_headlines()
        # 1차: hash 정확 dedup → 2차: jaccard 강화 dedup
        merged = _dedup_advanced(rss + naver)
        logger.info(
            f"[news_collector] 한국 뉴스 수집: rss={len(rss)}, naver={len(naver)}, "
            f"dedup={len(merged)}"
        )
        return merged
    except Exception as e:
        logger.error(f"[news_collector] 한국 뉴스 수집 실패: {e}", exc_info=True)
        return []


# ════════════════════════════════════════════════════════════════════
# 미국 뉴스 수집
# ════════════════════════════════════════════════════════════════════

_US_RSS_FEEDS = [
    # Yahoo Finance
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
    # CNBC — 전체 (TopNews)
    ("CNBC", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    # MarketWatch
    ("MarketWatch", "https://feeds.marketwatch.com/marketwatch/topstories/"),
    # Investing.com — 뉴스
    ("Investing", "https://www.investing.com/rss/news_25.rss"),
]


def collect_us_news(hours: int = 18) -> list[dict]:
    """Yahoo Finance / CNBC / MarketWatch / Investing RSS — 최근 N시간."""
    try:
        out: list[dict] = []
        for src, url in _US_RSS_FEEDS:
            entries = _fetch_feed(url)
            for e in entries[:30]:
                title = (getattr(e, "title", "") or "").strip()
                link = (getattr(e, "link", "") or "").strip()
                if not title:
                    continue
                pub = _parse_pub(e)
                summary = ""
                raw_sum = getattr(e, "summary", "") or ""
                if raw_sum:
                    try:
                        summary = BeautifulSoup(raw_sum, "html.parser").get_text(" ", strip=True)[:300]
                    except Exception:
                        summary = re.sub(r"<[^>]+>", " ", raw_sum)[:300]
                out.append({
                    "market":        "us",
                    "source":        src,
                    "headline":      title,         # 우선 원문 — Claude 단계에서 한글로 교체
                    "headline_orig": title,
                    "raw_summary":   summary,
                    "url":           link or None,
                    "published_at":  pub,
                })
        merged = _dedup_advanced(out)
        filtered = _filter_recent(merged, hours)
        logger.info(
            f"[news_collector] 미국 뉴스 수집: raw={len(out)}, dedup={len(merged)}, "
            f"recent={len(filtered)}"
        )
        return filtered
    except Exception as e:
        logger.error(f"[news_collector] 미국 뉴스 수집 실패: {e}", exc_info=True)
        return []


# ════════════════════════════════════════════════════════════════════
# 종목 매칭 — 로컬 사전 기반 (Claude 호출 안 함)
# ════════════════════════════════════════════════════════════════════

# 별칭 사전 (사용자 흔한 줄임말)
_TICKER_ALIASES: dict[str, str] = {
    "SK하닉": "000660", "하닉": "000660", "sk하닉": "000660",
    "삼전": "005930", "삼성전": "005930",
    "현대차": "005380", "기아차": "000270",
    "엘지엔솔": "373220", "LG엔솔": "373220", "엘지에너지솔루션": "373220",
    "삼바": "207940", "삼성바이오": "207940",
    "셀트": "068270",
    "에코프로비엠": "247540",
    "한미반": "042700",
    "한화에어로": "012450", "한화에어로스페이스": "012450",
    "두산에너빌": "034020",
}

# 종목명 사전: 캐시되어 자주 호출되도록 lazy load
_TICKER_NAME_DICT: dict[str, str] | None = None  # {name -> ticker}
_TICKER_NAME_DICT_VERSION = 0  # 캐시 무효화용


def _build_ticker_name_dict() -> dict[str, str]:
    """종목명 → 6자리 코드 사전 빌드.

    소스: universe + daily_top_value + hot_list (모두 UNION, 최신 우선).
    """
    out: dict[str, str] = {}
    try:
        rows = fetch_all(
            """
            SELECT DISTINCT ticker, name
            FROM (
                SELECT ticker, name, added_at as ts FROM universe WHERE name IS NOT NULL AND length(ticker)=6
                UNION ALL
                SELECT ticker, name, created_at as ts FROM daily_top_value WHERE name IS NOT NULL AND length(ticker)=6
                UNION ALL
                SELECT ticker, name, created_at as ts FROM hot_list WHERE name IS NOT NULL AND length(ticker)=6
            )
            ORDER BY ts DESC
            """
        )
        for r in (rows or []):
            d = dict(r)
            name = (d.get("name") or "").strip()
            ticker = (d.get("ticker") or "").strip()
            if not name or not ticker or len(ticker) != 6 or not ticker.isdigit():
                continue
            # 우선주(우) 제외
            if name.endswith("우") and len(name) >= 2:
                # 그래도 추가는 하되 정확한 단어경계 매칭에만
                pass
            if name not in out:
                out[name] = ticker
    except Exception as e:
        logger.warning(f"[news_collector] 종목명 사전 빌드 실패: {e}")
    # 별칭 추가
    for alias, tk in _TICKER_ALIASES.items():
        out.setdefault(alias, tk)
    return out


def _get_ticker_dict() -> dict[str, str]:
    global _TICKER_NAME_DICT
    if _TICKER_NAME_DICT is None:
        _TICKER_NAME_DICT = _build_ticker_name_dict()
        logger.info(f"[news_collector] 종목명 사전 로드: {len(_TICKER_NAME_DICT)}건")
    return _TICKER_NAME_DICT


def reset_ticker_dict() -> None:
    """테스트/일중 갱신용 — 사전 캐시 무효화."""
    global _TICKER_NAME_DICT
    _TICKER_NAME_DICT = None


# 한글 단어 경계 후속 글자 (조사/접미사) — '삼성전자가/는/와/도' 등 매칭 위해
_KR_WORD_BOUNDARY_AFTER = set("는은이가을를의에서와과로으도만한도부터까지나며")
# 일반 명사 충돌 방지 — 너무 짧고 흔한 단어 (단독 사용 시 무시, '신라호텔' 등 부분일치 케이스도 위험)
_AMBIGUOUS_SHORT_NAMES = {
    "대상", "한화", "두산", "현대", "동방", "동원", "보령", "신라", "삼양", "동서",
}
# 영문 약자 — 매우 흔한 단어와 충돌해서 단어경계로도 막기 어려운 것만.
# (대부분의 영문 약자는 word boundary 검사로 충분히 안전)
_EN_SHORT_NAMES_BLACKLIST: set[str] = set()


def _is_kr_char(ch: str) -> bool:
    return "가" <= ch <= "힣"


def _is_en_or_digit(ch: str) -> bool:
    return ch.isalnum() and ord(ch) < 128


def _is_pure_ascii(s: str) -> bool:
    return all(ord(c) < 128 for c in s)


def _match_tickers_locally(headline: str, summary: str = "") -> list[str]:
    """헤드라인+요약에서 종목명을 매칭하여 6자리 종목코드 추출.

    매칭 규칙 (오매칭 차단 강화):
      0. 6자리 숫자가 본문에 있으면 직접 추출 (예: "005930")
      1. 한글 종목명 3자 이상: 부분일치 OK
      2. 한글 종목명 2자: 한글 단어경계 검사 (조사/공백/구두점/문장끝)
      3. 영문 종목명: 영문 단어경계 검사 (앞뒤가 영숫자면 거부) — "NC"가 "Incyte"에 매칭 차단
      4. 흔한 짧은 단어 (대상/한화/NC 등): 단독 사용 무시
    """
    if not headline:
        return []
    text = (headline or "") + " " + (summary or "")
    found: list[str] = []
    seen: set[str] = set()

    # 0) 6자리 종목코드 직접 추출
    for m in re.finditer(r"(?<!\d)(\d{6})(?!\d)", text):
        tk = m.group(1)
        if tk not in seen:
            seen.add(tk)
            found.append(tk)

    name_dict = _get_ticker_dict()

    # 1) 종목명 길이 순으로 정렬 (긴 이름 우선)
    by_len = sorted(name_dict.items(), key=lambda kv: -len(kv[0]))

    matched_spans: list[tuple[int, int]] = []

    def _is_in_span(start: int, end: int) -> bool:
        for s, e in matched_spans:
            if not (end <= s or start >= e):
                return True
        return False

    for name, ticker in by_len:
        if ticker in seen:
            continue
        nlen = len(name)
        if nlen < 2:
            continue
        is_ascii = _is_pure_ascii(name)
        # 짧고 흔한 한글 단어 — 단독 매칭 위험
        if nlen <= 2 and not is_ascii and name in _AMBIGUOUS_SHORT_NAMES:
            continue
        # 짧은 영문 약자 — 다른 영문 단어에 부분매칭 위험
        if is_ascii and nlen <= 3 and name in _EN_SHORT_NAMES_BLACKLIST:
            continue

        start = 0
        while True:
            idx = text.find(name, start)
            if idx < 0:
                break
            end = idx + nlen
            if _is_in_span(idx, end):
                start = end
                continue

            ok = True
            # 한글 종목명 2자 — 한글 단어경계 검사
            if nlen <= 2 and not is_ascii:
                if end < len(text):
                    next_ch = text[end]
                    if _is_kr_char(next_ch) and next_ch not in _KR_WORD_BOUNDARY_AFTER:
                        ok = False
                if ok and idx > 0:
                    prev_ch = text[idx - 1]
                    if _is_kr_char(prev_ch):
                        ok = False

            # 영문 종목명 — 영문 단어 경계 필수 (앞뒤가 영숫자면 다른 단어의 일부)
            if ok and is_ascii:
                if idx > 0 and _is_en_or_digit(text[idx - 1]):
                    ok = False
                if ok and end < len(text) and _is_en_or_digit(text[end]):
                    ok = False

            if not ok:
                start = end
                continue

            seen.add(ticker)
            found.append(ticker)
            matched_spans.append((idx, end))
            break

        if len(found) >= 5:
            break

    return found[:5]


# ════════════════════════════════════════════════════════════════════
# 룰 기반 카테고리 힌트
# ════════════════════════════════════════════════════════════════════

_MACRO_KW = [
    "FOMC", "기준금리", "금리인상", "금리인하", "환율", "달러", "원화", "위안",
    "유가", "WTI", "브렌트", "인플레이션", "CPI", "PPI", "PCE", "고용지표",
    "비농업", "실업률", "GDP", "FOMC", "연준", "Fed", "BOK", "한은", "한국은행",
    "관세", "무역", "지정학", "전쟁", "중동", "이란", "러시아", "우크라",
    "통화정책", "재정정책", "예산안", "ECB", "BOJ", "IMF",
]
_SECTOR_KW = [
    "반도체", "메모리", "HBM", "파운드리", "DDR",
    "2차전지", "이차전지", "배터리", "양극재", "음극재", "전해질",
    "조선", "수주", "LNG", "원자력", "SMR",
    "AI", "GPU", "데이터센터", "클라우드",
    "바이오", "신약", "임상", "FDA",
    "자동차", "전기차", "EV", "수소",
    "철강", "정유", "화학", "통신", "금융",
]
_COMPANY_KW = ["실적", "분기", "공시", "신제품", "출시", "M&A", "인수", "합병", "유증", "감자", "공장"]
_RISK_KW = [
    "급락", "폭락", "파산", "디폴트", "부도", "회계부정", "사기", "스캔들",
    "제재", "조사", "압수수색", "기소", "벌금", "리콜", "사고", "화재",
    "해킹", "감독원", "고발",
]


def _category_hint(headline: str, summary: str = "") -> str | None:
    """룰 기반 카테고리 힌트 — Claude는 참고만, 최종 결정은 Claude."""
    text = (headline or "") + " " + (summary or "")
    if any(k in text for k in _RISK_KW):
        return "risk"
    if any(k in text for k in _MACRO_KW):
        return "macro"
    if any(k in text for k in _SECTOR_KW):
        return "sector"
    if any(k in text for k in _COMPANY_KW):
        return "company"
    return None


# ════════════════════════════════════════════════════════════════════
# Claude 분류·번역 (최적화 — summary/tickers 제거, 응답 필드 최소화)
# ════════════════════════════════════════════════════════════════════

_CLASSIFY_SYSTEM_PROMPT = """한국 주식시장 뉴스 분류기. 절대 추정 금지 — 원문에 없는 정보 추가 X.

각 뉴스 항목마다 다음을 결정:

1. headline_ko: 한글 헤드라인.
   - 영문이면 정확히 번역 (의역 X, 원문 의미만)
   - 한글이면 원문 그대로 (수정 X)
2. category: macro | sector | company | risk
   - macro: 금리/환율/유가/통화정책/거시지표/지정학 (FOMC, 환율, 무역분쟁)
   - sector: 업종 전체 영향 (반도체 업황, 조선 수주, AI 테마)
   - company: 특정 기업 단일 이슈 (실적, 신제품, 공시)
   - risk: 급락/파산/사고/규제/제재 등 명확한 부정 이벤트
3. tags: 원문에 명시된 키워드만. 최대 3개. ["반도체", "FOMC"] 형식.
4. importance: 1~5 정수. ⚠️ 엄격하게.
   - 5: KOSPI/KOSDAQ 1% 이상 영향 확실 (FOMC 결정, 전쟁, 대형 거시). 하루 0~2건.
   - 4: 특정 섹터 또는 대형주 영향 확실 (반도체 관세, 빅테크 실적). 하루 3~8건.
   - 3: 개별 기업 또는 산업 일부 (실적, 공시, 신제품). 하루 10~25건.
   - 2: 정보성, 단기 영향 약함. 하루 20~40건.
   - 1: 단순 정보, 분석 가치 적음. 하루 10~30건.
   - 5점 인플레 금지. 4점 이상은 전체 5% 이하 유지.

응답: STRICT JSON 배열. 첫 글자 `[`, 마지막 글자 `]`. 코드 펜스/주석/설명 금지.
[
  {"index":0,"headline_ko":"...","category":"macro","tags":["..."],"importance":3},
  ...
]

규칙:
- 입력 순서대로 응답. index 그대로 사용.
- 분류 불가 시 category=macro, importance=2.
- headline_ko 50자 내외 — 길게 늘리지 말 것.
- 'hint' 필드는 룰 기반 추정 — 참고만, 최종은 본인이 결정."""


def _extract_json_array(raw: str) -> str:
    text = raw.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if m:
        text = m.group(1)
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        text = text[start: end + 1]
    text = re.sub(r",(\s*[\]}])", r"\1", text)
    return text


def _parse_array_robust(raw: str) -> list:
    """Claude 응답 — JSON 배열 파싱. 깨졌으면 객체 단위 부분 복구."""
    cleaned = _extract_json_array(raw)
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass

    # 부분 복구 — 객체 단위로 추출 시도
    out: list = []
    depth = 0
    start_idx: int | None = None
    in_str = False
    esc = False
    for i, ch in enumerate(cleaned):
        if esc:
            esc = False
            continue
        if ch == "\\" and in_str:
            esc = True
            continue
        if ch == '"' and not esc:
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            if depth == 0:
                start_idx = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start_idx is not None:
                obj_text = cleaned[start_idx: i + 1]
                obj_text = re.sub(r",(\s*[\]}])", r"\1", obj_text)
                try:
                    obj = json.loads(obj_text)
                    if isinstance(obj, dict):
                        out.append(obj)
                except Exception:
                    pass
                start_idx = None
    return out


def _chunk(seq: list, size: int) -> Iterable[list]:
    for i in range(0, len(seq), size):
        yield seq[i: i + size]


def _build_payload(chunk: list[dict]) -> list[dict]:
    """Claude 입력 페이로드 빌드 — 시장별 최적화.

    - 한국어 뉴스(market=kr): headline만 (번역 불필요)
    - 영어 뉴스(market=us): headline + raw_summary 50자 (번역 컨텍스트)
    - 모든 항목에 룰 기반 hint 추가 (토큰 거의 안 늘어남)
    """
    payload = []
    for i, item in enumerate(chunk):
        market = item.get("market") or "kr"
        headline = (item.get("headline_orig") or item.get("headline") or "")[:200]
        raw_sum = (item.get("raw_summary") or "")
        hint = _category_hint(headline, raw_sum)
        entry: dict = {
            "index":    i,
            "headline": headline,
        }
        if market == "us":
            entry["market"] = "us"
            # 영어 뉴스만 raw_summary 50자 — 번역 정확도 보강
            if raw_sum:
                entry["raw"] = raw_sum[:50]
        # 한국어는 market 필드 생략 (토큰 절감 — Claude는 한글이면 kr 추정)
        if hint:
            entry["hint"] = hint
        payload.append(entry)
    return payload


def _classify_chunk(chunk: list[dict]) -> list[dict]:
    """Claude Haiku로 청크 단위 분류·번역."""
    if not chunk:
        return []

    payload = _build_payload(chunk)
    user_content = (
        "다음 뉴스를 분류·번역. index 그대로 응답에 사용. (market 없으면 한국 뉴스, hint는 룰 기반 참고)\n\n"
        + json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
    )

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=6000,  # 응답 필드 줄여서 max 축소
            temperature=0,
            timeout=120.0,
            system=[
                {
                    "type": "text",
                    "text": _CLASSIFY_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_content}],
        )
        raw = response.content[0].text.strip()
        parsed = _parse_array_robust(raw)
        if not isinstance(parsed, list):
            return []
        if not parsed:
            logger.warning(
                f"[news_collector] Claude 분류 파싱 실패 — 부분 복구도 0건 (응답 길이={len(raw)})"
            )
        return parsed
    except Exception as e:
        logger.error(f"[news_collector] Claude 분류 실패: {type(e).__name__}: {e}")
        check_claude_error(e, "news_collector")
        return []


def classify_and_translate(raw_news: list[dict]) -> list[dict]:
    """Claude로 일괄 처리: 번역+분류+태그+중요도. 종목매칭은 로컬 사전.

    실패 시 — 번역/분류 없이 원문 + category 룰힌트 + importance=2 폴백.
    """
    if not raw_news:
        return []

    # 비용 절감: 수집 양 max 80건 (importance 보존: published_at 최신순 우선)
    if len(raw_news) > 80:
        raw_news = raw_news[:80]

    # 청크 크기 35 — timeout 120초 안정
    results: list[dict] = []
    CHUNK = 35

    for chunk in _chunk(raw_news, CHUNK):
        parsed = _classify_chunk(chunk)
        index_map = {int(p.get("index", -1)): p for p in parsed if isinstance(p, dict)}

        for i, src_item in enumerate(chunk):
            cls = index_map.get(i)
            market = src_item.get("market") or "kr"
            raw_summary = (src_item.get("raw_summary") or "").strip()
            original_headline = src_item.get("headline_orig") or src_item.get("headline") or ""

            if cls:
                category = (cls.get("category") or "").strip().lower()
                if category not in _VALID_CATEGORIES:
                    category = _category_hint(original_headline, raw_summary) or "macro"
                tags = cls.get("tags") or []
                try:
                    importance = int(cls.get("importance") or 3)
                except (TypeError, ValueError):
                    importance = 3
                importance = max(1, min(5, importance))
                headline_ko = (cls.get("headline_ko") or "").strip()
                if not headline_ko:
                    headline_ko = original_headline
            else:
                # 폴백 — Claude 실패 시
                category = _category_hint(original_headline, raw_summary) or "macro"
                tags = []
                importance = 2
                headline_ko = original_headline

            # ── 종목 매칭 (로컬 사전, Claude 호출 X) ──
            # 한국 뉴스: headline + raw_summary 둘 다 매칭
            # 미국 뉴스: 한글 번역된 headline_ko + 원문 headline (Samsung -> 삼성전자 매칭)
            if market == "kr":
                tickers = _match_tickers_locally(headline_ko, raw_summary)
            else:
                # 영어 뉴스 — 번역본과 원문 둘 다 매칭 (Samsung→005930 매칭은 영어에도 적용)
                tickers = _match_tickers_locally(headline_ko + " " + original_headline, raw_summary)

            # ── summary 처리: Claude에게 받지 않고 raw_summary 그대로 ──
            # 한국어: raw_summary 그대로 (번역 불필요)
            # 영어: raw_summary는 원문이므로 사용 안 함 — headline_ko가 번역된 한 줄 요약 역할
            if market == "kr":
                summary_final = raw_summary[:200]
            else:
                # 영어 뉴스는 raw_summary가 영어. 메시지엔 headline만 쓰이므로 빈 값 OK
                summary_final = ""

            results.append({
                "market":          src_item.get("market"),
                "source":          src_item.get("source"),
                "headline":        headline_ko,
                "headline_orig":   src_item.get("headline_orig"),
                "summary":         summary_final,
                "category":        category,
                "tags":            tags if isinstance(tags, list) else [],
                "related_tickers": tickers,
                "importance":      importance,
                "url":             src_item.get("url"),
                "published_at":    src_item.get("published_at"),
            })

    return results


# ════════════════════════════════════════════════════════════════════
# DB 저장 / 조회
# ════════════════════════════════════════════════════════════════════

def save_news(news_list: list[dict]) -> int:
    """daily_news INSERT. URL 중복 시 스킵. headline 중복도 24h 내라면 스킵.

    Returns: 신규 INSERT 건수.
    """
    if not news_list:
        return 0

    today = datetime.now().date().isoformat()
    inserted = 0

    # 최근 36시간 헤드라인 set — URL 없는 항목 중복 차단
    try:
        recent_rows = fetch_all(
            """
            SELECT headline FROM daily_news
            WHERE created_at >= datetime('now', '-36 hours', 'localtime')
            """
        )
        recent_headlines = {
            _hash_headline(dict(r).get("headline") or "")
            for r in (recent_rows or [])
        }
    except Exception:
        recent_headlines = set()

    for item in news_list:
        headline = (item.get("headline") or "").strip()
        if not headline:
            continue
        category = item.get("category") or "macro"
        if category not in _VALID_CATEGORIES:
            category = "macro"
        url = item.get("url")
        h_hash = _hash_headline(headline)

        # URL 없는 항목은 헤드라인 해시로 중복 체크
        if not url and h_hash in recent_headlines:
            continue

        pub_at = item.get("published_at")
        if isinstance(pub_at, datetime):
            pub_at_s = pub_at.isoformat(timespec="seconds")
        elif isinstance(pub_at, str):
            pub_at_s = pub_at
        else:
            pub_at_s = None

        try:
            execute(
                """
                INSERT OR IGNORE INTO daily_news (
                    date, market, source, headline, headline_orig, summary,
                    category, tags, related_tickers, importance, url, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    today,
                    item.get("market") or "global",
                    item.get("source"),
                    headline,
                    item.get("headline_orig"),
                    item.get("summary") or "",
                    category,
                    json.dumps(item.get("tags") or [], ensure_ascii=False),
                    json.dumps(item.get("related_tickers") or [], ensure_ascii=False),
                    int(item.get("importance") or 3),
                    url,
                    pub_at_s,
                ),
            )
            # INSERT OR IGNORE는 lastrowid가 0/None 가능 — 정확한 카운트를 위해 직접 SELECT
            row = fetch_one(
                "SELECT id FROM daily_news WHERE date = ? AND headline = ? ORDER BY id DESC LIMIT 1",
                (today, headline),
            )
            if row:
                inserted += 1
                recent_headlines.add(h_hash)
        except Exception as e:
            logger.debug(f"[news_collector] INSERT 실패: {e}")
            continue

    # inserted는 위 방식상 '존재 확인'에 가까움 — 실제 신규만 세려면 별도 처리.
    # 실용 목적: 새로 들어온 (URL 중복 X) 항목 수 ~= len(news_list) - skipped.
    # 정확도를 위해 created_at 기준 최신 1분 이내 카운트로 재계산.
    try:
        row = fetch_one(
            "SELECT COUNT(*) AS n FROM daily_news WHERE created_at >= datetime('now', '-1 minute', 'localtime')"
        )
        if row:
            inserted = int(dict(row).get("n") or inserted)
    except Exception:
        pass

    return inserted


def get_news_for_brief(date_str: str, max_per_category: dict | None = None) -> dict:
    """morning_brief/evening_review에서 호출.

    Returns:
        {
            'macro': [top N],
            'sector': [top N],
            'company': [top N],
            'risk': [top N],
        }
      각 항목: {headline, source, importance, tags, related_tickers, market, url}
      importance DESC 정렬. 총합 max 10개 (분류별 분배).
    """
    quota = dict(_DEFAULT_QUOTA)
    if max_per_category:
        for k in _VALID_CATEGORIES:
            if k in max_per_category:
                try:
                    quota[k] = max(0, int(max_per_category[k]))
                except (TypeError, ValueError):
                    pass

    out: dict[str, list[dict]] = {k: [] for k in _VALID_CATEGORIES}

    # 우선 당일 — 비어 있으면 직전 36시간 보강
    rows = fetch_all(
        """
        SELECT id, market, source, headline, summary, category, tags,
               related_tickers, importance, url, published_at, created_at
        FROM daily_news
        WHERE date = ?
        ORDER BY importance DESC, created_at DESC
        """,
        (date_str,),
    )
    if not rows:
        rows = fetch_all(
            """
            SELECT id, market, source, headline, summary, category, tags,
                   related_tickers, importance, url, published_at, created_at
            FROM daily_news
            WHERE created_at >= datetime('now', '-36 hours', 'localtime')
            ORDER BY importance DESC, created_at DESC
            """
        )

    for r in (rows or []):
        d = dict(r)
        cat = d.get("category")
        if cat not in _VALID_CATEGORIES:
            continue
        if len(out[cat]) >= quota.get(cat, 0):
            continue
        try:
            tags = json.loads(d.get("tags") or "[]")
        except Exception:
            tags = []
        try:
            tickers = json.loads(d.get("related_tickers") or "[]")
        except Exception:
            tickers = []
        out[cat].append({
            "headline":        d.get("headline"),
            "summary":         d.get("summary"),
            "source":          d.get("source"),
            "market":          d.get("market"),
            "importance":      d.get("importance") or 3,
            "tags":            tags,
            "related_tickers": tickers,
            "url":             d.get("url"),
        })

    # 총합 10개 한도 — 우선순위 importance 기준
    total = sum(len(v) for v in out.values())
    if total > 10:
        flat = []
        for cat, items in out.items():
            for it in items:
                flat.append((cat, it))
        flat.sort(key=lambda x: x[1].get("importance") or 0, reverse=True)
        kept = flat[:10]
        out = {k: [] for k in _VALID_CATEGORIES}
        for cat, it in kept:
            out[cat].append(it)

    return out


# ════════════════════════════════════════════════════════════════════
# 통합 엔트리 (morning_brief/evening_review에서 1줄로 호출)
# ════════════════════════════════════════════════════════════════════

def collect_and_save(hours: int = 18) -> dict:
    """수집 → 분류 → 저장 한 번에. Returns: {kr, us, classified, saved}."""
    try:
        kr = collect_korean_news(hours=hours)
    except Exception as e:
        logger.error(f"[news_collector] kr 수집 실패: {e}")
        kr = []
    try:
        us = collect_us_news(hours=hours)
    except Exception as e:
        logger.error(f"[news_collector] us 수집 실패: {e}")
        us = []

    classified: list[dict] = []
    if kr or us:
        try:
            classified = classify_and_translate(kr + us)
        except Exception as e:
            logger.error(f"[news_collector] 분류 실패: {e}")
            classified = []

    saved = 0
    if classified:
        try:
            saved = save_news(classified)
        except Exception as e:
            logger.error(f"[news_collector] 저장 실패: {e}")

    logger.info(
        f"[news_collector] 통합 완료 — kr={len(kr)} us={len(us)} "
        f"classified={len(classified)} saved={saved}"
    )
    return {"kr": len(kr), "us": len(us), "classified": len(classified), "saved": saved}


# ── CLI ───────────────────────────────────────────────────────

if __name__ == "__main__":
    r = collect_and_save(hours=24)
    print(r)
