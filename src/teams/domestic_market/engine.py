"""
engine.py — 국내 시황팀 메인 엔진

실행 주기: 장 중 30분마다 (09:00 ~ 15:30)
즉시 트리거 조건:
  - KOSPI ±1.5% 이상 급변
  - 외국인 순매수 ±2000억 이상
  - 글로벌 리스크 점수 7 이상 (글로벌 시황팀 신호)

수집 → Claude 분석 → DB 저장 → 트리거 체크 순으로 실행.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime

from src.infra.database import execute, fetch_one
from src.teams.domestic_market.analyzer import analyze
from src.teams.domestic_market.collector import DomesticMarketData, collect
from src.utils.logger import get_logger

logger = get_logger(__name__)

_INTERVAL_SEC = 1800            # 30분
_KOSPI_ALERT_PCT = 1.5          # KOSPI ±1.5% 즉시 트리거
_FOREIGN_ALERT_BN = 2000.0      # 외국인 순매수 ±2000억 즉시 트리거
_GLOBAL_RISK_TRIGGER = 7        # 글로벌 리스크 점수 임계값


class DomesticMarketEngine:
    """국내 시황팀 엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="domestic-market-engine",
        )
        self._last_kospi: float = 0.0
        self._last_foreign_net: float = 0.0

    def start(self, morning_summary: bool = False) -> None:
        logger.info("국내 시황팀 엔진 시작")
        self._morning_summary = morning_summary
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=10)
        logger.info("국내 시황팀 엔진 종료")

    # ──────────────────────────────────────────
    # 메인 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        # 첫 실행: 오버나이트 요약 (전날 15:30 이후 변동 한번에 체크)
        try:
            self.run_once(morning_summary=getattr(self, "_morning_summary", False))
        except Exception as e:
            logger.error(f"국내 시황팀 오류: {e}", exc_info=True)

        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=_INTERVAL_SEC)
            if self._stop_event.is_set():
                break
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"국내 시황팀 오류: {e}", exc_info=True)

    def run_once(self, morning_summary: bool = False) -> dict:
        """
        1회 실행: 수집 → 분석 → DB 저장 → 트리거 체크.

        morning_summary=True: 장 전 오버나이트 요약 모드
        Returns:
            저장된 market_condition 딕셔너리
        """
        # 1. 데이터 수집
        data = collect()

        # 2. 글로벌 리스크 점수 참조 (글로벌 시황팀 DB에서 읽기)
        global_risk_score = _get_global_risk_score()

        # 3. Claude 분석
        analysis = analyze(data, global_risk_score, morning_summary=morning_summary)

        # 4. DB 저장
        row = _save_to_db(data, analysis)

        # 5. 즉시 트리거 체크
        self._check_alerts(data)

        # 6. 감성 분석 — 수집된 뉴스 비동기 처리
        self._submit_news_for_sentiment(data)

        return row

    # ──────────────────────────────────────────
    # 트리거 체크
    # ──────────────────────────────────────────

    def _check_alerts(self, data: DomesticMarketData) -> None:
        alerts = []

        # KOSPI 급변
        if self._last_kospi > 0 and data.kospi.current > 0:
            kospi_change_pct = (
                (data.kospi.current - self._last_kospi) / self._last_kospi * 100
            )
            if abs(kospi_change_pct) >= _KOSPI_ALERT_PCT:
                alerts.append(
                    f"KOSPI {self._last_kospi:,.0f} → {data.kospi.current:,.0f} "
                    f"({kospi_change_pct:+.2f}%) — 지수 급변 경보"
                )
        self._last_kospi = data.kospi.current

        # 외국인 급격한 순매수/매도 전환
        fn = data.kospi_flow.foreign_net
        if abs(fn) >= _FOREIGN_ALERT_BN:
            direction = "대량 매수" if fn > 0 else "대량 매도"
            alerts.append(f"외국인 {fn:+.0f}억 — {direction} 경보")
        self._last_foreign_net = fn

        if alerts:
            for alert in alerts:
                logger.warning(f"[국내시황 경보] {alert}")
            try:
                from src.teams.risk.engine import trigger_emergency
                trigger_emergency()
            except Exception:
                pass

    # ──────────────────────────────────────────
    # 감성 분석 제출
    # ──────────────────────────────────────────

    def _submit_news_for_sentiment(self, data: DomesticMarketData) -> None:
        """수집된 뉴스를 SentimentCache에 비동기 제출."""
        if not data.news:
            return

        def _analyze_all():
            from src.infra.sentiment_cache import SentimentCache
            cache = SentimentCache()
            for news in data.news[:5]:  # 최대 5건만 분석
                try:
                    cache.analyze(
                        url=news.url,
                        title=news.title,
                        content=news.summary or news.title,
                        ticker=None,
                        category="market",
                    )
                except Exception as e:
                    logger.debug(f"뉴스 감성 분석 오류: {e}")

        t = threading.Thread(target=_analyze_all, daemon=True, name="news-sentiment")
        t.start()


# ──────────────────────────────────────────────
# DB 헬퍼
# ──────────────────────────────────────────────

def _get_global_risk_score() -> int:
    """글로벌 시황팀 DB에서 가장 최근 리스크 점수 조회."""
    try:
        row = fetch_one(
            "SELECT global_risk_score FROM global_condition ORDER BY created_at DESC LIMIT 1"
        )
        return int(row["global_risk_score"]) if row else 5
    except Exception:
        return 5  # 데이터 없으면 중간값


def _save_to_db(data: DomesticMarketData, analysis: dict) -> dict:
    """market_condition 테이블에 저장."""
    row = {
        "market_score": analysis.get("market_score", 0.0),
        "market_direction": analysis.get("market_direction", "neutral"),
        "foreign_net_buy_bn": data.kospi_flow.foreign_net,
        "institutional_net_buy_bn": data.kospi_flow.institutional_net,
        "advancing_stocks": None,   # 향후 추가 (KIS 상승종목 수)
        "declining_stocks": None,   # 향후 추가 (KIS 하락종목 수)
        "summary": json.dumps(
            {
                "kospi": data.kospi.change_pct,
                "kosdaq": data.kosdaq.change_pct,
                "analysis": analysis.get("summary", ""),
                "key_reasons": analysis.get("key_reasons", []),
                "leading_force": analysis.get("leading_force", "mixed"),
            },
            ensure_ascii=False,
        ),
    }

    execute(
        """
        INSERT INTO market_condition
            (market_score, market_direction, foreign_net_buy_bn,
             institutional_net_buy_bn, advancing_stocks, declining_stocks, summary)
        VALUES
            (:market_score, :market_direction, :foreign_net_buy_bn,
             :institutional_net_buy_bn, :advancing_stocks, :declining_stocks, :summary)
        """,
        tuple(row.values()),
    )

    logger.info(
        f"DB 저장 완료 — 시장점수={row['market_score']} "
        f"| 방향={row['market_direction']} "
        f"| 외국인={row['foreign_net_buy_bn']:+.0f}억"
    )
    return row


def get_latest() -> dict | None:
    """가장 최근 market_condition 행 반환."""
    row = fetch_one(
        "SELECT * FROM market_condition ORDER BY created_at DESC LIMIT 1"
    )
    return dict(row) if row else None
