"""
news_collector.py — 주요 뉴스 수집/분류/저장 (2026-05-13)

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
    seen = set()
    out = []
    for it in items:
        h = _hash_headline(it.get("headline_orig") or it.get("headline") or "")
        if not h or h in seen:
            continue
        seen.add(h)
        out.append(it)
    return out


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
        merged = _dedup_headlines(rss + naver)
        logger.info(f"[news_collector] 한국 뉴스 수집: rss={len(rss)}, naver={len(naver)}, dedup={len(merged)}")
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
        merged = _dedup_headlines(out)
        filtered = _filter_recent(merged, hours)
        logger.info(f"[news_collector] 미국 뉴스 수집: raw={len(out)}, dedup={len(merged)}, recent={len(filtered)}")
        return filtered
    except Exception as e:
        logger.error(f"[news_collector] 미국 뉴스 수집 실패: {e}", exc_info=True)
        return []


# ════════════════════════════════════════════════════════════════════
# Claude 분류·번역
# ════════════════════════════════════════════════════════════════════

_CLASSIFY_SYSTEM_PROMPT = """당신은 한국 주식시장 매매 어시스턴트의 뉴스 분류기입니다.
절대 추정 금지 — 원문 헤드라인에 없는 정보를 추가하지 마세요.

작업 (각 뉴스 항목마다):
1. headline_ko: 한글 헤드라인. 영문이면 자연스럽고 정확하게 번역. 한글이면 그대로 또는 가벼운 정리.
2. summary: 1~2줄 한국어 요약. 원문(headline + raw_summary) 기반. 영향 추측·예측 금지.
3. category: 'macro' | 'sector' | 'company' | 'risk' 중 하나.
   - macro:   금리/환율/유가/통화정책/거시지표/지정학 (예: FOMC, 미·중 협상, USD/KRW)
   - sector:  업종 전체에 영향 (예: 반도체 업황, 조선 수주, AI 테마)
   - company: 특정 기업 단일 이슈 (예: 삼성전자 실적, 엔비디아 신제품)
   - risk:    급락/사고/파산/규제/제재 등 명확한 부정적 리스크
4. tags: 원문에 명시된 키워드만. 예: ["반도체", "FOMC", "엔비디아"]. 최대 5개.
5. related_tickers: 헤드라인/요약에 명시된 한국 종목코드(6자리)만. 추정 X. 없으면 빈 배열 [].
   허용 예시: 삼성전자=005930, SK하이닉스=000660, 현대차=005380, 기아=000270, 네이버=035420, 카카오=035720, 셀트리온=068270, HMM=011200, 한화에어로=012450, LIG넥스원=079550, 한미반도체=042700.
6. importance: 1~5 정수. 5=KOSPI 1%↑ 영향 가능, 4=섹터 강한 영향, 3=일반, 2=정보성, 1=루머/약영향.
7. dedup_key: headline_ko를 정규화한 짧은 키 (중복 후처리용).

응답: STRICT JSON 배열 (객체 하나가 입력 항목 하나에 대응. 입력 순서 유지).
[
  {
    "index": 0,
    "headline_ko": "...",
    "summary": "...",
    "category": "macro",
    "tags": ["..."],
    "related_tickers": [],
    "importance": 3
  },
  ...
]

규칙:
- 응답은 첫 글자 `[` 마지막 글자 `]`. 그 외 문자 없음.
- 코드 펜스/주석/설명문/trailing comma 금지.
- 분류 불가 시 category='macro', importance=2.
- 한 항목당 100자 내외 — 길게 풀어쓰지 말 것.
"""


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


def _classify_chunk(chunk: list[dict]) -> list[dict]:
    """Claude Haiku로 청크 단위 분류·번역."""
    if not chunk:
        return []

    # 입력 콤팩트 직렬화
    payload = []
    for i, item in enumerate(chunk):
        payload.append({
            "index":       i,
            "market":      item.get("market"),
            "source":      item.get("source"),
            "headline":    (item.get("headline_orig") or item.get("headline") or "")[:300],
            "raw_summary": (item.get("raw_summary") or "")[:300],
        })

    user_content = (
        "다음 뉴스 항목 배열을 분류·번역하세요. 각 항목의 index를 응답에 그대로 사용하세요.\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=4000,
            temperature=0,
            timeout=45.0,
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
    """Claude로 일괄 처리: 번역+분류+태그+종목매칭+중요도+요약.

    실패 시 — 번역/분류 없이 원문 + category='macro' + importance=2 폴백.
    """
    if not raw_news:
        return []

    # 청크 크기 15 — Claude max_tokens 안에서 JSON이 잘리지 않도록 안전 마진
    results: list[dict] = []
    CHUNK = 15

    for chunk in _chunk(raw_news, CHUNK):
        parsed = _classify_chunk(chunk)
        index_map = {int(p.get("index", -1)): p for p in parsed if isinstance(p, dict)}

        for i, src_item in enumerate(chunk):
            cls = index_map.get(i)
            if cls:
                category = (cls.get("category") or "").strip().lower()
                if category not in _VALID_CATEGORIES:
                    category = "macro"
                tags = cls.get("tags") or []
                tickers = cls.get("related_tickers") or []
                try:
                    importance = int(cls.get("importance") or 3)
                except (TypeError, ValueError):
                    importance = 3
                importance = max(1, min(5, importance))
                headline_ko = (cls.get("headline_ko") or "").strip()
                summary = (cls.get("summary") or "").strip()
                if not headline_ko:
                    headline_ko = src_item.get("headline") or src_item.get("headline_orig") or ""
            else:
                # 폴백 — Claude 실패 시 원문 저장
                category = "macro"
                tags = []
                tickers = []
                importance = 2
                headline_ko = src_item.get("headline") or src_item.get("headline_orig") or ""
                summary = (src_item.get("raw_summary") or "")[:160]

            results.append({
                "market":          src_item.get("market"),
                "source":          src_item.get("source"),
                "headline":        headline_ko,
                "headline_orig":   src_item.get("headline_orig"),
                "summary":         summary,
                "category":        category,
                "tags":            tags if isinstance(tags, list) else [],
                "related_tickers": tickers if isinstance(tickers, list) else [],
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
