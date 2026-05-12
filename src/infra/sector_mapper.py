"""
sector_mapper.py — KRX 종목 → 섹터/테마 매핑 인프라

어시스턴트 모델 전환 (2026-05-12) Phase 3-2.

역할:
  morning_brief / evening_review가 종목별 섹터·테마를 빠르게 조회하기 위한 공통 인프라.

3가지 데이터 소스 통합:
  1. 섹터 (sector): KRX 공식 업종 분류 — FinanceDataReader StockListing
     기존 sector_rotation.SectorRotationCache 재사용 (이미 KRX 매핑 로드)
  2. 테마 (themes): ticker_themes DB 테이블에 누적 저장 (source: krx/news/manual/naver)
  3. 뉴스 기반 테마 자동 추출: sentiment_cache 최근 항목을 Claude Haiku로 묶어
     "AI반도체", "조선", "바이오" 같은 테마 라벨을 생성 → ticker_themes 적재

공개 함수:
  - get_sector(ticker) -> str
  - get_themes(ticker, source=None) -> list[str]
  - upsert_theme(ticker, theme, source='manual', weight=1.0) -> None
  - refresh_themes_from_news(target_date=None) -> int  # 적재 건수
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.sector_rotation import SectorRotationCache
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_THEME_EXTRACTION_SYSTEM = """당신은 한국 주식 시장 테마 분석 전문가입니다.
제공된 뉴스 제목·요약 묶음에서 투자자 관점의 '테마 키워드'를 추출합니다.

규칙:
- 테마는 4~10자 한국어 (예: "AI반도체", "2차전지", "조선", "원자력", "바이오시밀러")
- 일반 명사·시황 단어는 제외 (예: "증시", "급등", "외인매수" 같은 단어 금지)
- 종목 코드 외에 종목명도 함께 제시되는 경우에만 매핑
- 확신이 낮은 매핑은 절대 추측하지 말고 제외

JSON 출력만 (다른 텍스트 금지):
{
  "themes": [
    {
      "theme": "테마이름",
      "tickers": ["005930", "..."],
      "confidence": 0.0~1.0
    }
  ]
}"""


# ── 섹터 조회 ──────────────────────────────────────────────────

def get_sector(ticker: str) -> str:
    """
    종목 코드 → KRX 공식 업종명.

    SectorRotationCache가 이미 KRX 전종목 매핑을 캐시하므로 재사용.
    미매핑이면 '기타' 반환.
    """
    return SectorRotationCache().get_sector(ticker)


# ── 테마 조회·적재 ───────────────────────────────────────────

def get_themes(ticker: str, source: str | None = None) -> list[str]:
    """
    종목의 테마 목록 반환 (가중치 내림차순).

    Args:
        ticker: 종목 코드
        source: 'krx' | 'news' | 'manual' | 'naver' (None=전체)
    """
    if source:
        rows = fetch_all(
            """
            SELECT theme, weight FROM ticker_themes
            WHERE ticker = ? AND source = ?
            ORDER BY weight DESC, updated_at DESC
            """,
            (ticker, source),
        )
    else:
        # 동일 테마가 여러 source에서 잡힐 수 있으니 합산
        rows = fetch_all(
            """
            SELECT theme, SUM(weight) AS w
            FROM ticker_themes
            WHERE ticker = ?
            GROUP BY theme
            ORDER BY w DESC
            """,
            (ticker,),
        )
    return [r["theme"] for r in rows]


def upsert_theme(
    ticker: str,
    theme: str,
    source: str = "manual",
    weight: float = 1.0,
) -> None:
    """ticker_themes에 단일 매핑 적재 (PK: ticker+theme+source)."""
    if not ticker or not theme:
        return
    theme = theme.strip()
    try:
        execute(
            """
            INSERT INTO ticker_themes (ticker, theme, weight, source, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ticker, theme, source) DO UPDATE SET
                weight     = excluded.weight,
                updated_at = excluded.updated_at
            """,
            (ticker, theme, weight, source, datetime.now().isoformat(timespec="seconds")),
        )
    except Exception as e:
        logger.warning(f"테마 적재 실패 [{ticker}/{theme}/{source}]: {e}")


def get_tickers_by_theme(theme: str, limit: int = 20) -> list[str]:
    """특정 테마에 매핑된 종목 코드 리스트."""
    rows = fetch_all(
        """
        SELECT ticker, MAX(weight) AS w
        FROM ticker_themes
        WHERE theme = ?
        GROUP BY ticker
        ORDER BY w DESC
        LIMIT ?
        """,
        (theme, limit),
    )
    return [r["ticker"] for r in rows]


# ── 뉴스 기반 테마 자동 추출 ──────────────────────────────────

def refresh_themes_from_news(target_date: date | str | None = None) -> int:
    """
    최근 sentiment_cache 항목들을 묶어 Claude Haiku로 테마 추출 → ticker_themes 적재.

    Args:
        target_date: 대상 날짜 (기본 오늘). YYYY-MM-DD 문자열 또는 date 객체.

    Returns:
        새로 적재된 (ticker, theme) 매핑 건수.
    """
    if target_date is None:
        target_date = date.today()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, "%Y-%m-%d").date()

    # 1. 어제 자정 ~ target_date 23:59 사이의 종목 관련 뉴스 수집
    day_start = datetime.combine(target_date, datetime.min.time()).isoformat()
    day_end   = datetime.combine(target_date, datetime.max.time()).isoformat()
    rows = fetch_all(
        """
        SELECT url, ticker, score, direction, key_factors, analyzed_at
        FROM sentiment_cache
        WHERE analyzed_at BETWEEN ? AND ?
          AND ticker IS NOT NULL
          AND category = 'stock'
          AND direction != 'neutral'
        ORDER BY analyzed_at DESC
        LIMIT 80
        """,
        (day_start, day_end),
    )

    if not rows:
        logger.info(f"[sector_mapper] 테마 추출 — sentiment_cache {target_date} 데이터 없음")
        return 0

    # 2. ticker별로 key_factors를 묶어 Claude에 전달
    by_ticker: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        try:
            factors = json.loads(r["key_factors"] or "[]")
            if factors:
                by_ticker[r["ticker"]].extend(factors[:2])
        except Exception:
            continue

    if not by_ticker:
        logger.info("[sector_mapper] key_factors 추출 결과 없음")
        return 0

    # 3. 종목명도 함께 전달 (universe / hot_list 어디든 OK)
    ticker_names = _resolve_ticker_names(list(by_ticker.keys()))

    # 4. Claude 호출 — JSON 응답
    payload_lines = []
    for tk, factors in by_ticker.items():
        name = ticker_names.get(tk, "")
        # 동일 factor 중복 제거
        uniq = list(dict.fromkeys(f for f in factors if f))[:5]
        payload_lines.append(f"- {tk} ({name}): {' / '.join(uniq) if uniq else '근거없음'}")

    user_content = (
        f"날짜: {target_date}\n"
        f"종목별 최근 뉴스 핵심 키워드:\n"
        + "\n".join(payload_lines)
        + "\n\n각 테마(theme)별로 매핑되는 종목 코드(tickers)를 묶어 JSON으로 답하세요."
    )

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=1024,
            temperature=settings.CLAUDE_TEMPERATURE,
            system=[
                {
                    "type": "text",
                    "text": _THEME_EXTRACTION_SYSTEM,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_content}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"[sector_mapper] Claude 응답 JSON 파싱 실패: {e}")
        return 0
    except Exception as e:
        logger.error(f"[sector_mapper] Claude 호출 오류: {e}")
        return 0

    # 5. ticker_themes 적재
    inserted = 0
    for item in parsed.get("themes", []):
        theme = (item.get("theme") or "").strip()
        tickers = item.get("tickers") or []
        conf = float(item.get("confidence") or 0.0)
        if not theme or conf < 0.4:
            continue
        for tk in tickers:
            tk = str(tk).strip().zfill(6)
            if not tk or len(tk) != 6 or not tk.isdigit():
                continue
            try:
                upsert_theme(tk, theme, source="news", weight=round(conf, 3))
                inserted += 1
            except Exception:
                continue

    logger.info(
        f"[sector_mapper] 테마 자동 추출 완료 — "
        f"{len(by_ticker)}종목 분석 → {inserted}건 적재 ({target_date})"
    )
    return inserted


# ── 내부 유틸 ────────────────────────────────────────────────

def _resolve_ticker_names(tickers: list[str]) -> dict[str, str]:
    """
    종목 코드 → 종목명 매핑 dict. universe / hot_list 어디든 있으면 추출.
    """
    if not tickers:
        return {}
    placeholders = ",".join("?" * len(tickers))
    result: dict[str, str] = {}

    # 1. universe 우선
    try:
        rows = fetch_all(
            f"SELECT DISTINCT ticker, name FROM universe WHERE ticker IN ({placeholders})",
            tuple(tickers),
        )
        for r in rows:
            if r["name"]:
                result[r["ticker"]] = r["name"]
    except Exception:
        pass

    # 2. 누락분은 hot_list 최근 데이터
    missing = [t for t in tickers if t not in result]
    if missing:
        placeholders2 = ",".join("?" * len(missing))
        try:
            rows = fetch_all(
                f"""
                SELECT ticker, MAX(name) AS name
                FROM hot_list
                WHERE ticker IN ({placeholders2}) AND name IS NOT NULL
                GROUP BY ticker
                """,
                tuple(missing),
            )
            for r in rows:
                if r["name"]:
                    result[r["ticker"]] = r["name"]
        except Exception:
            pass

    return result


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "refresh":
        n = refresh_themes_from_news()
        print(f"테마 적재 완료: {n}건")
    elif len(sys.argv) > 1:
        tk = sys.argv[1].zfill(6)
        print(f"[{tk}] sector = {get_sector(tk)}")
        print(f"[{tk}] themes = {get_themes(tk)}")
    else:
        print("사용법:")
        print("  python -m src.infra.sector_mapper <ticker>   # 조회")
        print("  python -m src.infra.sector_mapper refresh    # 뉴스 기반 테마 적재")
