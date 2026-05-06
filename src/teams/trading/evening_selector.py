"""
evening_selector.py — 전일 저녁 내일 매수 종목 선정 (방향 1 전략)

매일 장 마감 후 16:40에 실행.
오늘 하루 데이터를 바탕으로 Claude가 내일 시초가에 매수할 종목 1개를 선정한다.

선정 기준:
  - RSI 45~68 (과열 아님, 상승 추세 중)
  - 거래량비율 1.5배 이상
  - 당일 등락 +0~+8% (적당한 모멘텀)
  - 당일 hot_list 등장 이력
  - 섹터 강세 여부
  - ticker_stats 과거 성과

내일 시초가 시점에 갭이 너무 크면 (±7% 초과) 실행을 스킵한다.
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_SYSTEM_PROMPT = """너는 국내 주식 퀀트 트레이더야.
오늘 장 데이터를 보고 내일 시초가(09:00)에 매수할 종목 1개를 선정해줘.

선정 기준:
1. RSI 45~68: 과열되지 않았지만 상승 모멘텀이 있는 구간
2. 거래량 비율 1.5배 이상: 오늘 평균보다 거래가 활발했음
3. 당일 등락 +1%~+8%: 너무 폭등한 종목은 피함 (내일 급락 위험)
4. 모멘텀 점수 높을수록 유리
5. 외인/기관 순매수 플러스면 가산점
6. ticker_stats 과거 성과 (승률, 평균손익) 참고

응답은 반드시 JSON만:
{
  "ticker": "종목코드 6자리",
  "name": "종목명",
  "reason": "선정 이유 2~3줄 (한국어)"
}

후보가 없거나 시장이 불안하면:
{"ticker": null, "name": null, "reason": "선정 불가 이유"}"""


def run_evening_selection() -> None:
    """장 마감 후 내일 매수 종목 선정. scheduler에서 16:40에 호출."""
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    # 주말 건너뜀
    dow = (date.today() + timedelta(days=1)).weekday()
    if dow >= 5:  # 토(5) 일(6)
        tomorrow = (date.today() + timedelta(days=7 - dow)).isoformat()

    # 이미 선정했으면 스킵
    existing = fetch_one("SELECT ticker FROM tomorrow_pick WHERE pick_date=?", (tomorrow,))
    if existing:
        logger.info(f"[저녁 선점] 이미 선정됨: {existing['ticker']} ({tomorrow})")
        return

    candidates = _build_candidates()
    if not candidates:
        logger.warning("[저녁 선점] 후보 없음 — 선정 건너뜀")
        return

    ticker, name, reason = _ask_claude(candidates)

    if ticker:
        execute(
            """
            INSERT OR REPLACE INTO tomorrow_pick (pick_date, ticker, name, reason, status)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (tomorrow, ticker, name, reason),
        )
        msg = (
            f"📌 <b>[내일 선점]</b> {name}({ticker})\n"
            f"📅 매수 예정: {tomorrow} 시초가\n"
            f"📝 {reason}"
        )
        logger.info(f"[저녁 선점] 완료: {ticker} ({name}) → {tomorrow}")
        notify(msg)
    else:
        logger.info(f"[저녁 선점] Claude 선정 없음: {reason}")


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
        LIMIT 15
        """,
    )

    # 오늘 손절 이력 있는 종목 제외
    today_losses = {
        r["ticker"]
        for r in fetch_all(
            "SELECT DISTINCT ticker FROM trades WHERE date=DATE('now','localtime') AND action='stop_loss'",
        )
    }

    return [dict(r) for r in rows if r["ticker"] not in today_losses]


def _ask_claude(candidates: list[dict]) -> tuple[str | None, str | None, str]:
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
            f"과거승률={wr}\n"
        )

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=300,
            temperature=0,
            timeout=30.0,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        # JSON 추출
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()
        result = json.loads(raw)
        ticker = result.get("ticker")
        name = result.get("name", "")
        reason = result.get("reason", "")
        return ticker, name, reason
    except Exception as e:
        logger.error(f"[저녁 선점] Claude 호출 실패: {e}")
        return None, None, str(e)
