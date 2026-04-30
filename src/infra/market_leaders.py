"""
market_leaders.py — 시장 주도주 실시간 감지

KIS 거래대금 순위 API로 전체 시장(KOSPI+KOSDAQ) 거래대금 상위 종목을 조회.
5분 캐시 — 매 스캔 사이클에서 갱신.

주도주 판단 기준:
  1. 거래대금 순위 (돈이 몰리는 곳 = 시장이 주목하는 곳)
  2. 등락률 양수 (상승 중)
  3. 외인 or 기관 순매수 (세력 동반)

제공 함수:
  refresh()                  -> None          # KIS API 호출해 갱신
  get_leaders(n)             -> list[dict]    # 거래대금+수급 기준 주도주 상위 n개
  get_leader_tickers()       -> set[str]      # 주도주 ticker 집합
  get_leader_context_str()   -> str           # Claude 프롬프트 삽입용 문자열
"""
from __future__ import annotations

import threading
import time
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)

_CACHE_TTL_SEC = 300   # 5분 캐시

_lock    = threading.Lock()
_leaders: list[dict] = []      # 필터링된 주도주 리스트
_all_top: list[dict] = []      # 원시 거래대금 순위 (필터 전)
_last_refresh: float = 0.0


def _refresh_from_hot_list(top_n: int) -> list[dict]:
    """paper 모드 폴백: hot_list DB에서 거래대금 상위 종목으로 주도주 구성."""
    try:
        from src.infra.database import fetch_all
        rows = fetch_all(
            """
            SELECT ticker, name, price_change_pct, trading_value,
                   frgn_net_buy, inst_net_buy
            FROM hot_list
            WHERE DATE(created_at) = DATE('now', 'localtime')
            ORDER BY trading_value DESC
            """,
        )
        seen: set[str] = set()
        result: list[dict] = []
        for r in rows:
            tk = r["ticker"]
            if tk in seen:
                continue
            seen.add(tk)
            result.append({
                "ticker":        tk,
                "name":          r["name"] or tk,
                "price":         0.0,
                "change_pct":    float(r["price_change_pct"] or 0),
                "trading_value": int(r["trading_value"] or 0),
                "volume":        0,
                "frgn_net_buy":  int(r["frgn_net_buy"] or 0),
                "inst_net_buy":  int(r["inst_net_buy"] or 0),
            })
            if len(result) >= top_n:
                break
        return result
    except Exception as e:
        logger.debug(f"hot_list 폴백 실패: {e}")
        return []


def refresh(top_n: int = 30) -> None:
    """
    KIS 거래대금 순위 API를 호출해 주도주 캐시를 갱신.
    KOSPI + KOSDAQ 합산 후 거래대금 내림차순 정렬.
    paper 모드: KIS ranking 미지원 → hot_list DB 폴백.
    """
    global _leaders, _all_top, _last_refresh
    try:
        from src.infra.kis_gateway import KISGateway, Priority
        gw = KISGateway()

        kospi  = gw.get_trading_value_ranking("J", top_n, Priority.BACKGROUND)
        kosdaq = gw.get_trading_value_ranking("Q", top_n, Priority.BACKGROUND)
        combined = kospi + kosdaq

        # paper 모드에서 KIS ranking 미지원 → hot_list 폴백
        if not combined:
            combined = _refresh_from_hot_list(top_n)

        # 거래대금 내림차순 정렬 후 상위 top_n
        combined.sort(key=lambda x: x["trading_value"], reverse=True)
        raw_top = combined[:top_n]

        # 주도주 필터: 상승 중 AND (외인 or 기관 순매수)
        leaders = [
            item for item in raw_top
            if item["change_pct"] > 0
            and (item["frgn_net_buy"] > 0 or item["inst_net_buy"] > 0)
        ]
        # 필터 통과 없으면 거래대금 상위 5개라도 반환
        if not leaders:
            leaders = [item for item in raw_top if item["change_pct"] > 0][:5]
        if not leaders:
            leaders = raw_top[:5]

        with _lock:
            _all_top   = raw_top
            _leaders   = leaders
            _last_refresh = time.time()

        source = "hot_list폴백" if not (kospi + kosdaq) else "KIS"
        logger.info(
            f"주도주 갱신 완료({source}) — 거래대금 상위 {len(raw_top)}종목 중 "
            f"주도주 {len(leaders)}종목 선별: "
            + ", ".join(f"{l['name']}({l['ticker']})" for l in leaders[:5])
        )
    except Exception as e:
        logger.warning(f"주도주 갱신 실패: {e}")


def _ensure_fresh() -> None:
    """캐시가 5분 이상 지났으면 자동 갱신."""
    with _lock:
        stale = (time.time() - _last_refresh) > _CACHE_TTL_SEC
    if stale:
        refresh()


def get_leaders(n: int = 10) -> list[dict]:
    """거래대금+수급 기준 주도주 상위 n개 반환."""
    _ensure_fresh()
    with _lock:
        return _leaders[:n]


def get_leader_tickers() -> set[str]:
    """주도주 ticker 집합 반환 — O(1) 조회용."""
    _ensure_fresh()
    with _lock:
        return {l["ticker"] for l in _leaders}


def get_all_top_tickers() -> set[str]:
    """거래대금 상위 전체 ticker 집합 (필터 전)."""
    _ensure_fresh()
    with _lock:
        return {l["ticker"] for l in _all_top}


def get_leader_context_str() -> str:
    """
    Claude 프롬프트에 삽입할 주도주 컨텍스트 문자열.

    예:
      오늘 거래대금 상위 주도주 (전체 시장 기준):
      1. 삼성전자(005930) +1.2% | 거래대금 8,230억 | 외인+기관 매수
      2. SK하이닉스(000660) +2.5% | 거래대금 4,100억 | 외인 매수
    """
    _ensure_fresh()
    with _lock:
        leaders = _leaders[:8]

    if not leaders:
        return ""

    lines = ["오늘 거래대금 상위 주도주 (전체 시장 기준):"]
    for i, l in enumerate(leaders, 1):
        supply = []
        if l["frgn_net_buy"] > 0 and l["inst_net_buy"] > 0:
            supply.append("외인+기관 동시매수")
        elif l["frgn_net_buy"] > 0:
            supply.append("외인 매수")
        elif l["inst_net_buy"] > 0:
            supply.append("기관 매수")
        supply_str = " | " + supply[0] if supply else ""
        tv_str = f"{l['trading_value'] / 1e8:.0f}억" if l["trading_value"] > 0 else "-"
        lines.append(
            f"  {i}. {l['name']}({l['ticker']}) "
            f"{l['change_pct']:+.1f}% | 거래대금 {tv_str}{supply_str}"
        )
    return "\n".join(lines)
