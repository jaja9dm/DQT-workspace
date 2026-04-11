"""
sentiment_cache.py — 공통 인프라 0-2: 감성 분석 캐시

역할:
  뉴스·공시 URL을 SHA-256 해시로 중복 제거.
  같은 기사를 여러 팀이 각자 Claude에 보내는 낭비를 제거한다.
  수집 즉시 1회만 분석하고, 전 팀이 DB에서 읽는다.

사용법:
  from src.infra.sentiment_cache import SentimentCache

  cache = SentimentCache()

  # 분석 요청 (캐시 히트 시 Claude 호출 없음)
  result = cache.analyze(
      url="https://...",
      title="삼성전자 어닝 서프라이즈",
      content="본문...",
      ticker="005930",     # 종목 관련 뉴스면 명시, 시황 기사면 None
      category="stock",    # stock | market | global
  )
  print(result["score"], result["direction"])

모델: claude-haiku-4-5 (속도·비용 최적화)
만료: 24시간
"""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_one, get_conn
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
_CACHE_TTL_HOURS = 24


class SentimentCache:
    """
    뉴스·공시 감성 분석 캐시 싱글턴.

    내부적으로 sentiment_cache 테이블을 사용한다.
    동일 URL 재요청 시 Claude 호출 없이 DB에서 즉시 반환.
    """

    _instance: "SentimentCache | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "SentimentCache":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    # ──────────────────────────────────────────
    # 공개 API
    # ──────────────────────────────────────────

    def analyze(
        self,
        url: str,
        title: str,
        content: str,
        ticker: str | None = None,
        category: str = "stock",  # stock | market | global
    ) -> dict:
        """
        뉴스·공시 감성 분석 (캐시 우선).

        Returns:
            {
                "score": float,        # -1.0(매우 부정) ~ 1.0(매우 긍정)
                "direction": str,      # bullish | bearish | neutral
                "confidence": float,   # 0.0 ~ 1.0
                "key_factors": list,   # 주요 근거 (최대 3개)
                "from_cache": bool,    # True=캐시 히트
            }
        """
        url_hash = _hash(url)

        # 1. 캐시 조회
        cached = self._get_cache(url_hash)
        if cached:
            logger.debug(f"캐시 히트: {url[:60]}...")
            return {**cached, "from_cache": True}

        # 2. Claude 분석
        logger.info(f"감성 분석 시작: {title[:40]}...")
        result = _call_claude(title, content, ticker, category)

        # 3. DB 저장
        self._save_cache(url_hash, url, ticker, category, result)

        return {**result, "from_cache": False}

    def get_by_ticker(self, ticker: str) -> list[dict]:
        """특정 종목의 최근 감성 분석 결과 목록 반환 (최신 5건)."""
        from src.infra.database import fetch_all
        rows = fetch_all(
            """
            SELECT score, direction, confidence, key_factors, analyzed_at
            FROM sentiment_cache
            WHERE ticker = ? AND expires_at > ?
            ORDER BY analyzed_at DESC LIMIT 5
            """,
            (ticker, datetime.now().isoformat()),
        )
        return [
            {
                "score": row["score"],
                "direction": row["direction"],
                "confidence": row["confidence"],
                "key_factors": json.loads(row["key_factors"] or "[]"),
                "analyzed_at": row["analyzed_at"],
            }
            for row in rows
        ]

    def avg_score_by_ticker(self, ticker: str) -> float | None:
        """종목의 최근 감성 평균 점수 반환. 데이터 없으면 None."""
        results = self.get_by_ticker(ticker)
        if not results:
            return None
        return round(sum(r["score"] for r in results) / len(results), 3)

    def purge_expired(self) -> int:
        """만료된 캐시 항목 삭제. 반환값: 삭제 건수."""
        with get_conn() as conn:
            cur = conn.execute(
                "DELETE FROM sentiment_cache WHERE expires_at <= ?",
                (datetime.now().isoformat(),),
            )
            deleted = cur.rowcount
        if deleted:
            logger.info(f"만료 캐시 {deleted}건 삭제")
        return deleted

    # ──────────────────────────────────────────
    # 내부 구현
    # ──────────────────────────────────────────

    def _get_cache(self, url_hash: str) -> dict | None:
        row = fetch_one(
            """
            SELECT score, direction, confidence, key_factors
            FROM sentiment_cache
            WHERE url_hash = ? AND expires_at > ?
            """,
            (url_hash, datetime.now().isoformat()),
        )
        if not row:
            return None
        return {
            "score": row["score"],
            "direction": row["direction"],
            "confidence": row["confidence"],
            "key_factors": json.loads(row["key_factors"] or "[]"),
        }

    def _save_cache(
        self,
        url_hash: str,
        url: str,
        ticker: str | None,
        category: str,
        result: dict,
    ) -> None:
        expires_at = (datetime.now() + timedelta(hours=_CACHE_TTL_HOURS)).isoformat()
        execute(
            """
            INSERT OR REPLACE INTO sentiment_cache
                (url_hash, url, ticker, category, score, direction,
                 confidence, key_factors, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                url_hash,
                url,
                ticker,
                category,
                result["score"],
                result["direction"],
                result["confidence"],
                json.dumps(result["key_factors"], ensure_ascii=False),
                expires_at,
            ),
        )


# ──────────────────────────────────────────────
# Claude 호출
# ──────────────────────────────────────────────

def _call_claude(
    title: str,
    content: str,
    ticker: str | None,
    category: str,
) -> dict:
    """Claude Haiku로 감성 분석 수행."""
    ticker_context = f"종목 코드: {ticker}\n" if ticker else ""
    content_preview = content[:800] if len(content) > 800 else content

    prompt = f"""뉴스 기사의 투자 감성을 분석하세요.

{ticker_context}카테고리: {category} (stock=개별종목, market=국내시황, global=글로벌)
제목: {title}
본문 요약: {content_preview}

다음 JSON 형식으로만 응답하세요:
{{
  "score": <-1.0~1.0, 소수점 2자리>,
  "direction": "<bullish|bearish|neutral>",
  "confidence": <0.0~1.0, 소수점 2자리>,
  "key_factors": ["<근거1>", "<근거2>"]
}}

판단 기준:
- score 0.3 이상 = bullish, -0.3 이하 = bearish, 그 사이 = neutral
- confidence: 근거가 명확할수록 높게
- key_factors: 판단 근거 최대 2개, 10자 이내로 간결하게"""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=256,
            temperature=settings.CLAUDE_TEMPERATURE,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)

    except json.JSONDecodeError:
        logger.warning("감성 분석 JSON 파싱 실패 — neutral 기본값 반환")
        return {"score": 0.0, "direction": "neutral", "confidence": 0.3, "key_factors": []}
    except Exception as e:
        logger.error(f"Claude Haiku 호출 오류: {e}")
        return {"score": 0.0, "direction": "neutral", "confidence": 0.0, "key_factors": []}


# ──────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────

def _hash(url: str) -> str:
    """URL → SHA-256 해시 (64자 hex)."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()
