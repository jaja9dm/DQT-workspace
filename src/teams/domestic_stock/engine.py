"""
engine.py — 국내 주식팀 메인 엔진

실행 주기: 장 중 5분마다 (09:00 ~ 15:30)
즉시 트리거 조건:
  - 개별 종목 거래량 급등 (평균 대비 5배 이상)
  - 개별 종목 가격 급등 (+5% 이상)
  - 글로벌 리스크 점수 7 이상 (위기 직전 — 스캔 강화)

수집 → Claude Hot List 판단 → DB 저장 → 트리거 체크 순으로 실행.
Hot List에 오른 종목은 hot_list 테이블에 저장 → 매매팀이 읽어 진입 판단.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime

from src.infra.database import execute, fetch_one
from src.teams.domestic_stock.analyzer import analyze
from src.teams.domestic_stock.collector import (
    PRICE_SURGE_PCT,
    VOLUME_SURGE_RATIO,
    StockSnapshot,
    UniverseScan,
    collect,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

_INTERVAL_SEC = 300              # 5분
_IMMEDIATE_VOL_RATIO = 5.0      # 즉시 트리거용 거래량 배율 (5배)
_IMMEDIATE_PRICE_PCT = 5.0      # 즉시 트리거용 가격 급등 (5%)

# 매매팀 Gate 1~3과 동일한 임계값 — 이 조건이면 어차피 진입 불가이므로 Hot List 분석 스킵
_GATE_RISK_LEVEL_MAX = 4        # Gate 1: 리스크 레벨 이 값 이상이면 신규 진입 금지
_GATE_MARKET_SCORE_MIN = -0.3   # Gate 3: 국내 시황 점수 이 값 미만이면 진입 보류


class DomesticStockEngine:
    """국내 주식팀 엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="domestic-stock-engine",
        )

    def start(self) -> None:
        logger.info("국내 주식팀 엔진 시작")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=15)
        logger.info("국내 주식팀 엔진 종료")

    # ──────────────────────────────────────────
    # 메인 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"국내 주식팀 오류: {e}", exc_info=True)

            self._stop_event.wait(timeout=_INTERVAL_SEC)

    def run_once(self) -> list[dict]:
        """
        1회 실행: 수집 → Hot List 판단 → DB 저장 → 트리거 체크.

        Returns:
            저장된 Hot List 딕셔너리 리스트
        """
        # 1. 컨텍스트 조회 (DB에서)
        market_score = _get_market_score()
        global_risk_score = _get_global_risk_score()

        # 2. 유니버스 스캔
        scan = collect()

        # 3. 즉시 트리거 경보 (스캔 직후)
        self._check_immediate_alerts(scan)

        # 4. [Gate 사전 체크] 매매팀 Gate 1~3 차단 조건이면 Claude 호출 생략
        #    어차피 매매팀이 진입을 차단할 시황이면 Hot List 분석 자체가 낭비
        if _is_trading_blocked(market_score):
            logger.info(
                "Hot List 분석 스킵 — 매매팀 게이트 차단 조건 (리스크 레벨/글로벌 시황/국내 시황)"
            )
            return []

        # 5. Claude Hot List 판단
        hot_list = analyze(scan, market_score, global_risk_score)

        # 6. DB 저장
        saved = _save_hot_list(hot_list, scan)

        # 7. 종목별 뉴스 감성 분석 제출 (Hot List 종목 대상)
        self._submit_ticker_sentiment(hot_list, scan)

        logger.info(f"Hot List 확정: {len(saved)}종목")
        return saved

    # ──────────────────────────────────────────
    # 즉시 트리거
    # ──────────────────────────────────────────

    def _check_immediate_alerts(self, scan: UniverseScan) -> None:
        """극단적 급등 종목 즉시 경보."""
        for snap in scan.snapshots:
            alerts = []
            if snap.volume_ratio >= _IMMEDIATE_VOL_RATIO:
                alerts.append(f"거래량 {snap.volume_ratio:.1f}배 급등")
            if snap.change_pct >= _IMMEDIATE_PRICE_PCT:
                alerts.append(f"가격 {snap.change_pct:+.1f}% 급등")

            if alerts:
                logger.warning(
                    f"[주식 경보] {snap.ticker}({snap.name}): "
                    + " / ".join(alerts)
                )
                # TODO: 위기 관리팀 즉시 트리거 (7단계 구현 후 연결)

    # ──────────────────────────────────────────
    # 감성 분석 제출
    # ──────────────────────────────────────────

    def _submit_ticker_sentiment(
        self, hot_list: list[dict], scan: UniverseScan
    ) -> None:
        """Hot List 종목의 뉴스 URL을 SentimentCache에 비동기 제출."""
        if not hot_list:
            return

        hot_tickers = {h["ticker"] for h in hot_list}
        snaps = {s.ticker: s for s in scan.snapshots}

        def _analyze_all():
            from src.infra.sentiment_cache import SentimentCache
            cache = SentimentCache()
            for ticker in hot_tickers:
                snap = snaps.get(ticker)
                if not snap:
                    continue
                try:
                    # 종목 뉴스 URL은 향후 뉴스 수집기 연동 시 추가
                    # 현재는 ticker 이름 기반 더미 분석 건너뜀
                    pass
                except Exception as e:
                    logger.debug(f"감성 분석 제출 오류 [{ticker}]: {e}")

        t = threading.Thread(target=_analyze_all, daemon=True, name="stock-sentiment")
        t.start()


# ──────────────────────────────────────────────
# DB 헬퍼
# ──────────────────────────────────────────────

def _is_trading_blocked(market_score: float) -> bool:
    """
    매매팀 Gate 1~3 차단 조건을 미리 확인.

    True를 반환하면 어차피 매매팀이 진입을 차단하므로
    Claude Hot List 분석 호출 자체를 생략해 API 비용을 절감한다.

    Gate 1 — 리스크 레벨 ≥ 4 (risk_status 테이블)
    Gate 2 — 글로벌 outlook == 'negative' (global_condition 테이블)
    Gate 3 — 국내 시황 점수 < -0.3 (이미 인자로 전달됨)
    """
    # Gate 1: 리스크 레벨
    try:
        row = fetch_one(
            "SELECT risk_level FROM risk_status ORDER BY created_at DESC LIMIT 1"
        )
        if row and int(row["risk_level"]) >= _GATE_RISK_LEVEL_MAX:
            logger.debug(f"Gate 1 차단: 리스크 레벨 {row['risk_level']}")
            return True
    except Exception:
        pass  # 리스크팀 아직 미기동 시 무시하고 진행

    # Gate 2: 글로벌 시황 outlook
    try:
        row = fetch_one(
            "SELECT korea_market_outlook FROM global_condition ORDER BY created_at DESC LIMIT 1"
        )
        if row and row["korea_market_outlook"] == "negative":
            logger.debug("Gate 2 차단: 글로벌 시황 negative")
            return True
    except Exception:
        pass

    # Gate 3: 국내 시황 점수
    if market_score < _GATE_MARKET_SCORE_MIN:
        logger.debug(f"Gate 3 차단: 국내 시황 점수 {market_score:.2f}")
        return True

    return False


def _get_market_score() -> float:
    """국내 시황팀 DB에서 가장 최근 시장 점수 조회."""
    try:
        row = fetch_one(
            "SELECT market_score FROM market_condition ORDER BY created_at DESC LIMIT 1"
        )
        return float(row["market_score"]) if row else 0.0
    except Exception:
        return 0.0


def _get_global_risk_score() -> int:
    """글로벌 시황팀 DB에서 가장 최근 리스크 점수 조회."""
    try:
        row = fetch_one(
            "SELECT global_risk_score FROM global_condition ORDER BY created_at DESC LIMIT 1"
        )
        return int(row["global_risk_score"]) if row else 5
    except Exception:
        return 5


def _save_hot_list(hot_list: list[dict], scan: UniverseScan) -> list[dict]:
    """hot_list 테이블에 저장. 스냅샷에서 추가 지표도 함께 저장."""
    snaps = {s.ticker: s for s in scan.snapshots}
    saved = []

    for item in hot_list:
        ticker = item.get("ticker", "")
        snap = snaps.get(ticker)
        if not snap:
            continue

        execute(
            """
            INSERT INTO hot_list
                (ticker, name, signal_type, volume_ratio,
                 price_change_pct, rsi, sector, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                snap.name,
                item.get("signal_type", "unknown"),
                snap.volume_ratio,
                snap.change_pct,
                snap.rsi,
                None,           # sector: 향후 업종 정보 추가 시 채움
                item.get("reason", ""),
            ),
        )
        saved.append({
            "ticker": ticker,
            "name": snap.name,
            "signal_type": item.get("signal_type"),
            "volume_ratio": snap.volume_ratio,
            "change_pct": snap.change_pct,
            "rsi": snap.rsi,
            "reason": item.get("reason", ""),
        })

    return saved


def get_latest_hot_list(limit: int = 10) -> list[dict]:
    """가장 최근 hot_list 조회 (매매팀이 읽는 공개 API)."""
    from src.infra.database import fetch_all
    rows = fetch_all(
        """
        SELECT ticker, name, signal_type, volume_ratio, price_change_pct, rsi, reason, created_at
        FROM hot_list
        ORDER BY created_at DESC LIMIT ?
        """,
        (limit,),
    )
    return [dict(r) for r in rows]
