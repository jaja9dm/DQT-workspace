"""
evening_selector.py — 전일 저녁 내일 매수 후보 5종목 선정 (방향 1 전략)

매일 장 마감 후 16:30에 실행.
오늘 하루 데이터를 바탕으로 Claude가 내일 시초가에 매수할 종목을 1~5순위로 선정.
09:01에 갭 + 호가 체크 후 가장 높은 순위의 합격 종목을 매수한다.

선정 기준:
  - RSI 45~68 (과열 아님, 상승 추세 중)
  - 거래량비율 1.5배 이상
  - 당일 등락 +0~+8% (적당한 모멘텀)
  - 당일 hot_list 등장 이력
  - 섹터 강세 여부
  - ticker_stats 과거 성과
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, RequestPriority
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_SYSTEM_PROMPT = """너는 국내 주식 퀀트 트레이더야.
오늘 장 데이터를 보고 내일 시초가(09:00)에 매수할 종목을 1~5순위로 선정해줘.
1순위가 최선이고 5순위가 차선책이야. 내일 아침에 갭과 호가 상황을 확인 후 통과하는 가장 높은 순위 종목을 산다.

선정 기준:
1. RSI 45~68: 과열되지 않았지만 상승 모멘텀이 있는 구간
2. 거래량 비율 1.5배 이상: 오늘 평균보다 거래가 활발했음
3. 당일 등락 +1%~+8%: 너무 폭등한 종목은 피함 (내일 급락 위험)
4. 모멘텀 점수 높을수록 유리
5. 외인/기관 순매수 플러스면 가산점
6. ticker_stats 과거 성과 (승률, 평균손익) 참고
7. 같은 섹터 종목은 중복 피할 것 (분산 효과)

응답은 반드시 JSON 배열만 (5개 또는 가능한 만큼):
[
  {"rank": 1, "ticker": "종목코드 6자리", "name": "종목명", "reason": "선정 이유 1~2줄"},
  {"rank": 2, "ticker": "...", "name": "...", "reason": "..."},
  ...
]

후보가 3개 미만이면 가능한 만큼만 반환. 없으면 빈 배열 [].
후보가 아예 없거나 시장이 매우 불안하면: []"""


def run_evening_selection() -> None:
    """장 마감 후 내일 매수 후보 선정. scheduler에서 16:30에 호출."""
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    dow = (date.today() + timedelta(days=1)).weekday()
    if dow >= 5:
        tomorrow = (date.today() + timedelta(days=7 - dow)).isoformat()

    # 이미 선정했으면 스킵
    existing = fetch_one("SELECT ticker FROM tomorrow_pick WHERE pick_date=? AND rank=1", (tomorrow,))
    if existing:
        logger.info(f"[저녁 선점] 이미 선정됨: {existing['ticker']} ({tomorrow})")
        return

    candidates = _build_candidates()
    if not candidates:
        logger.warning("[저녁 선점] 후보 없음 — 선정 건너뜀")
        return

    picks = _ask_claude(candidates)

    if not picks:
        logger.info("[저녁 선점] Claude 선정 없음")
        return

    # 기준가 조회
    gw = KISGateway()
    ref_prices = _fetch_ref_prices(gw, [p["ticker"] for p in picks])

    for pick in picks:
        ticker = pick["ticker"]
        rank = pick["rank"]
        ref_price = ref_prices.get(ticker, 0.0)
        execute(
            """
            INSERT OR REPLACE INTO tomorrow_pick (pick_date, rank, ticker, name, reason, ref_price, status)
            VALUES (?, ?, ?, ?, ?, ?, 'pending')
            """,
            (tomorrow, rank, ticker, pick.get("name", ""), pick.get("reason", ""), ref_price),
        )

    # 1순위 기준으로 알림
    top = picks[0]
    ref_str = f"{ref_prices.get(top['ticker'], 0):,.0f}원" if ref_prices.get(top["ticker"]) else "N/A"
    names_str = " / ".join(f"{p['rank']}. {p.get('name','')}" for p in picks)
    msg = (
        f"📌 <b>[내일 선점 완료]</b> {len(picks)}종목\n"
        f"📅 매수 예정: {tomorrow} 시초가 (갭+호가 통과 시)\n"
        f"🏆 순위: {names_str}\n"
        f"1순위 기준가: {ref_str}\n"
        f"📝 {top.get('reason','')}"
    )
    logger.info(f"[저녁 선점] {len(picks)}종목 선정 완료 → {tomorrow}")
    notify(msg)


def _build_candidates() -> list[dict]:
    """오늘 hot_list + ticker_stats 기반 후보 목록 구성."""
    rows = fetch_all(
        """
        SELECT
            h.ticker, h.name, h.rsi, h.volume_ratio, h.price_change_pct,
            h.momentum_score, h.rs_daily, h.rs_5d,
            h.frgn_net_buy, h.inst_net_buy, h.sector, h.signal_type,
            h.trading_value,
            t.win_rate, t.avg_pnl_pct, t.total_trades
        FROM hot_list h
        LEFT JOIN ticker_stats t ON h.ticker = t.ticker
        WHERE
            h.rsi BETWEEN 45 AND 68
            AND h.volume_ratio >= 1.5
            AND h.price_change_pct BETWEEN 0.5 AND 9.0
            AND h.trading_value >= 5000000000
        ORDER BY h.momentum_score DESC, h.volume_ratio DESC
        LIMIT 20
        """,
    )

    today_losses = {
        r["ticker"]
        for r in fetch_all(
            "SELECT DISTINCT ticker FROM trades WHERE date=DATE('now','localtime') AND action='stop_loss'",
        )
    }

    return [dict(r) for r in rows if r["ticker"] not in today_losses]


def _ask_claude(candidates: list[dict]) -> list[dict]:
    """Claude에게 5순위 후보 목록 요청. 반환: [{rank, ticker, name, reason}, ...]"""
    market = fetch_one(
        "SELECT market_score, direction FROM market_condition ORDER BY collected_at DESC LIMIT 1"
    )
    market_summary = ""
    if market:
        market_summary = f"오늘 시장: 시장점수={market['market_score']}, 방향={market['direction']}"

    prompt = f"{market_summary}\n\n후보 종목 목록:\n"
    for i, c in enumerate(candidates, 1):
        wr = f"{c['win_rate']*100:.0f}%" if c["win_rate"] else "신규"
        prompt += (
            f"{i}. {c['name']}({c['ticker']}) "
            f"RSI={c['rsi']:.0f} 등락={c['price_change_pct']:+.1f}% "
            f"거래량={c['volume_ratio']:.1f}배 모멘텀={c['momentum_score']:.0f} "
            f"RS당일={c['rs_daily']:+.1f}% RS5일={c['rs_5d']:+.1f}% "
            f"외인={'+'if c['frgn_net_buy']>0 else '-'} 기관={'+'if c['inst_net_buy']>0 else '-'} "
            f"과거승률={wr} 섹터={c['sector'] or '?'}\n"
        )

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=600,
            temperature=0,
            timeout=30.0,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()
        result = json.loads(raw)
        if not isinstance(result, list):
            return []
        valid = []
        for item in result:
            if item.get("ticker") and item.get("rank"):
                valid.append({
                    "rank": int(item["rank"]),
                    "ticker": str(item["ticker"]),
                    "name": str(item.get("name", "")),
                    "reason": str(item.get("reason", "")),
                })
        valid.sort(key=lambda x: x["rank"])
        return valid[:5]
    except Exception as e:
        logger.error(f"[저녁 선점] Claude 호출 실패: {e}")
        return []


def _fetch_ref_prices(gw: KISGateway, tickers: list[str]) -> dict[str, float]:
    """종목별 현재가 조회 (기준가 저장용)."""
    prices: dict[str, float] = {}
    for ticker in tickers:
        try:
            resp = gw.request(
                method="GET",
                path="/uapi/domestic-stock/v1/quotations/inquire-price",
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                tr_id="FHKST01010100",
                priority=RequestPriority.LOW,
            )
            price = float(resp.get("output", {}).get("stck_prpr", 0) or 0)
            if price > 0:
                prices[ticker] = price
        except Exception as e:
            logger.debug(f"[저녁 선점] {ticker} 기준가 조회 실패: {e}")
    return prices
