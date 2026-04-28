"""
engine.py — 글로벌 시황팀 메인 엔진

실행 주기: 평일 08:35 시작 / 15:35 종료 / 1시간마다
시작 시: 전날 15:30 이후 오버나이트 데이터 한 번에 요약
즉시 트리거 조건:
  - VIX ≥ 25
  - 미국 지수 ±2% 이상
  - USD/KRW ±1% 이상

수집 → Claude 분석 → DB 저장 → 트리거 조건 체크 순으로 실행.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime

from src.infra.database import execute, fetch_one
from src.teams.global_market.analyzer import analyze
from src.teams.global_market.collector import GlobalMarketData, collect
from src.utils.logger import get_logger

logger = get_logger(__name__)

# 즉시 트리거 임계값 (concept.md 기준)
_VIX_ALERT_THRESHOLD = 25.0
_INDEX_CHANGE_THRESHOLD = 2.0   # ±2%
_FX_CHANGE_THRESHOLD = 1.0      # ±1%

_INTERVAL_SEC = 3600  # 1시간


class GlobalMarketEngine:
    """글로벌 시황팀 엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="global-market-engine",
        )
        self._last_usd_krw: float = 0.0  # FX 변화율 계산용

    def start(self, morning_summary: bool = False) -> None:
        logger.info("글로벌 시황팀 엔진 시작")
        self._morning_summary = morning_summary
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=10)
        logger.info("글로벌 시황팀 엔진 종료")

    # ──────────────────────────────────────────
    # 메인 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        # 첫 실행: 오버나이트 요약 (전날 15:30 이후 변동 한번에 체크)
        try:
            self.run_once(morning_summary=getattr(self, "_morning_summary", False))
        except Exception as e:
            logger.error(f"글로벌 시황팀 오류: {e}", exc_info=True)

        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=_INTERVAL_SEC)
            if self._stop_event.is_set():
                break
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"글로벌 시황팀 오류: {e}", exc_info=True)

    def run_once(self, morning_summary: bool = False) -> dict:
        """
        1회 실행: 수집 → 분석 → DB 저장 → 트리거 체크.

        morning_summary=True: 오버나이트(전날 15:30~현재) 요약 모드
        Returns:
            저장된 global_condition 딕셔너리
        """
        # 1. 데이터 수집
        data = collect()

        # 2. Claude 분석 (아침 첫 실행은 오버나이트 요약 포함)
        analysis = analyze(data, morning_summary=morning_summary)

        # 3. DB 저장
        row = _save_to_db(data, analysis)

        # 4. 즉시 트리거 체크
        self._check_alerts(data)

        return row

    # ──────────────────────────────────────────
    # 트리거 체크
    # ──────────────────────────────────────────

    def _check_alerts(self, data: GlobalMarketData) -> None:
        alerts = []

        # VIX 경보
        if data.vix >= _VIX_ALERT_THRESHOLD:
            alerts.append(f"VIX {data.vix:.1f} — 공포지수 경보 임계값({_VIX_ALERT_THRESHOLD}) 초과")

        # 미국 지수 급변
        for name, chg in [
            ("S&P 500", data.sp500_change),
            ("NASDAQ", data.nasdaq_change),
            ("Dow", data.dow_change),
        ]:
            if abs(chg) >= _INDEX_CHANGE_THRESHOLD:
                alerts.append(f"{name} {chg:+.2f}% — 지수 급변 경보")

        # 환율 급변
        if self._last_usd_krw > 0:
            fx_change_pct = abs(data.usd_krw - self._last_usd_krw) / self._last_usd_krw * 100
            if fx_change_pct >= _FX_CHANGE_THRESHOLD:
                alerts.append(
                    f"USD/KRW {self._last_usd_krw:.0f} → {data.usd_krw:.0f} "
                    f"({fx_change_pct:+.2f}%) — 환율 급변 경보"
                )
        self._last_usd_krw = data.usd_krw

        if alerts:
            for alert in alerts:
                logger.warning(f"[글로벌 경보] {alert}")
            try:
                from src.teams.risk.engine import trigger_emergency
                trigger_emergency()
            except Exception:
                pass


# ──────────────────────────────────────────────
# DB 저장 헬퍼
# ──────────────────────────────────────────────

def _save_to_db(data: GlobalMarketData, analysis: dict) -> dict:
    """global_condition 테이블에 저장."""
    row = {
        "global_risk_score": analysis.get("global_risk_score", 0),
        "vix": data.vix,
        "sp500_change": data.sp500_change,
        "nasdaq_change": data.nasdaq_change,
        "usd_krw": data.usd_krw,
        "wti_oil": data.wti_oil,
        "us_10y_yield": data.us_10y_yield,
        "korea_market_outlook": analysis.get("korea_market_outlook", "neutral"),
        "key_events": json.dumps(
            analysis.get("key_risks", []) + data.upcoming_events,
            ensure_ascii=False,
        ),
    }

    execute(
        """
        INSERT INTO global_condition
            (global_risk_score, vix, sp500_change, nasdaq_change,
             usd_krw, wti_oil, us_10y_yield, korea_market_outlook, key_events)
        VALUES
            (:global_risk_score, :vix, :sp500_change, :nasdaq_change,
             :usd_krw, :wti_oil, :us_10y_yield, :korea_market_outlook, :key_events)
        """,
        tuple(row.values()),
    )

    logger.info(
        f"DB 저장 완료 — 리스크 점수={row['global_risk_score']} "
        f"| 전망={row['korea_market_outlook']}"
    )
    return row


def get_latest() -> dict | None:
    """가장 최근 global_condition 행 반환."""
    row = fetch_one(
        "SELECT * FROM global_condition ORDER BY created_at DESC LIMIT 1"
    )
    return dict(row) if row else None
