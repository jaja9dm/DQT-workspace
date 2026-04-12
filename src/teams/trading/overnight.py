"""
overnight.py — 오버나잇 보유 판단 모듈

역할:
  15:10 장 마감 20분 전, 보유 포지션별로 오버나잇 유지 vs 당일 청산을 판단.
  Claude가 각 포지션의 기술적 흐름·현재 손익·시황·리스크를 종합해 결정.

오버나잇 보유 기준 (Claude 판단):
  - 일봉 MACD 강세 유지 + 손절선(트레일링 플로어) 위
  - 분봉 MACD sell_pre 신호 없음
  - 현재 수익권 또는 소폭 손실
  - 내일 글로벌 리스크 낮음

당일 청산 기준 (Claude 판단):
  - 분봉 MACD sell_pre 신호 발생
  - 현재 손익 악화 추세
  - 글로벌 리스크 상승 우려 (overnight 위험)
  - 보유 일수 임박 (5일 기준 -1일)

실행: 스케줄러가 15:10에 호출.
"""

from __future__ import annotations

import json
from datetime import date, datetime

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, Priority
from src.teams.risk.engine import get_current_risk
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_KIS_ORDER_PATH   = "/uapi/domestic-stock/v1/trading/order-cash"
_KIS_BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"


def evaluate_overnight() -> list[dict]:
    """
    15:10 오버나잇 판단 메인 함수.

    보유 포지션 전체를 조회하여 Claude에게 유지/청산 판단 요청.
    청산 결정 시 즉시 시장가 매도 실행.

    Returns:
        처리 결과 목록 [{"ticker": .., "decision": "hold"|"sell", "reason": ..}]
    """
    logger.info("=== 15:10 오버나잇 판단 시작 ===")
    positions = _fetch_positions()

    if not positions:
        logger.info("보유 포지션 없음 — 오버나잇 판단 불필요")
        return []

    global_ctx  = _load_global_context()
    market_ctx  = _load_market_context()
    risk        = get_current_risk()
    risk_level  = risk.get("risk_level", 3)

    results: list[dict] = []
    sell_list: list[dict] = []
    hold_list: list[dict] = []

    for pos in positions:
        ticker   = pos["ticker"]
        name     = pos.get("name", ticker)
        pnl_pct  = pos["pnl_pct"]
        quantity = pos["quantity"]
        held_days = pos["held_days"]
        current_price = pos["current_price"]

        # 최신 분봉 MACD 신호
        from src.teams.intraday_macd.engine import get_latest_macd_signal
        macd_sig = get_latest_macd_signal(ticker, max_age_minutes=10)

        # 트레일링 스톱 정보
        ts_row = fetch_one("SELECT trailing_floor, entry_price FROM trailing_stop WHERE ticker = ?", (ticker,))
        trailing_info = ""
        if ts_row:
            floor_pct = (current_price / float(ts_row["trailing_floor"]) - 1) * 100
            trailing_info = f"손절선 대비 현재가 +{floor_pct:.1f}%"

        decision = _ask_claude_overnight(
            ticker=ticker,
            name=name,
            pnl_pct=pnl_pct,
            quantity=quantity,
            held_days=held_days,
            current_price=current_price,
            macd_signal=macd_sig,
            trailing_info=trailing_info,
            risk_level=risk_level,
            global_risk=global_ctx.get("global_risk_score", 5),
            market_score=market_ctx.get("market_score", 0.0),
            outlook=global_ctx.get("korea_market_outlook", "neutral"),
        )

        result = {
            "ticker": ticker,
            "name": name,
            "decision": decision["action"],
            "reason": decision["reason"],
            "pnl_pct": pnl_pct,
        }
        results.append(result)

        if decision["action"] == "sell":
            sell_list.append(pos)
            _execute_sell(pos, reason=decision["reason"])
        else:
            hold_list.append(pos)

    # 텔레그램 요약 발송
    _notify_overnight_summary(hold_list, sell_list, results)

    logger.info(
        f"오버나잇 판단 완료 — 유지 {len(hold_list)}종목 / 청산 {len(sell_list)}종목"
    )
    return results


# ──────────────────────────────────────────────
# Claude 오버나잇 판단
# ──────────────────────────────────────────────

def _ask_claude_overnight(
    ticker: str, name: str, pnl_pct: float, quantity: int,
    held_days: int, current_price: float, macd_signal: str,
    trailing_info: str, risk_level: int, global_risk: int,
    market_score: float, outlook: str,
) -> dict:
    """Claude에게 오버나잇 유지/청산 판단 요청."""

    prompt = f"""당신은 국내 주식 퀀트 트레이더입니다.
15:10, 장 마감 20분 전입니다. 아래 종목을 오늘 청산할지 내일까지 보유할지 판단하세요.

## 포지션 정보
- 종목: {ticker} ({name})
- 현재 손익: {pnl_pct:+.2f}%
- 보유 수량: {quantity}주
- 보유 일수: {held_days}일 (최대 5일)
- 현재가: {current_price:,.0f}원
- 분봉 MACD 신호: {macd_signal}  (buy_pre=강세임박 / sell_pre=약세임박 / hold=중립)
- 트레일링 스톱: {trailing_info if trailing_info else "정보 없음"}

## 시장 상황
- 리스크 레벨: {risk_level}/5
- 글로벌 리스크: {global_risk}/10
- 한국 시장 전망: {outlook}
- 국내 시황 점수: {market_score:+.2f}

## 판단 기준
오버나잇 유지 (hold): 아래 조건 충족 시
  - 수익권(pnl > 0) 또는 소폭 손실(-1% 이내)
  - 분봉 MACD sell_pre 신호 없음
  - 글로벌 리스크 ≤ 5, 리스크 레벨 ≤ 3
  - 보유 일수 ≤ 4일

당일 청산 (sell): 아래 중 하나라도 해당 시
  - 분봉 MACD sell_pre 신호 발생
  - 손실 -2% 초과이면서 반등 신호 없음
  - 글로벌 리스크 7 이상 or 리스크 레벨 4 이상
  - 보유 일수 5일 (타임컷 임박)

JSON만 응답:
{{"action": "hold"|"sell", "reason": "<근거 25자 이내>"}}"""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=128,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        logger.info(
            f"오버나잇 판단 [{ticker}] → {result.get('action','hold').upper()} "
            f"| {result.get('reason','')}"
        )
        return result
    except Exception as e:
        logger.warning(f"오버나잇 Claude 판단 실패 [{ticker}]: {e} — 기본값 hold")
        return {"action": "hold", "reason": "Claude 판단 불가 — 보유 유지"}


# ──────────────────────────────────────────────
# 매도 실행
# ──────────────────────────────────────────────

def _execute_sell(pos: dict, reason: str) -> None:
    """오버나잇 청산: KIS 시장가 매도."""
    ticker   = pos["ticker"]
    quantity = pos["quantity"]
    price    = pos["current_price"]

    gw = KISGateway()
    acnt_no, acnt_prdt_cd = (settings.KIS_ACCOUNT_NO.split("-") + ["01"])[:2]
    tr_id = "VTTC0801U" if settings.KIS_MODE == "paper" else "TTTC0801U"

    try:
        gw.request(
            method="POST",
            path=_KIS_ORDER_PATH,
            body={
                "CANO": acnt_no,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": ticker,
                "ORD_DVSN": "01",
                "ORD_QTY": str(quantity),
                "ORD_UNPR": "0",
                "ALGO_NO": "",
            },
            tr_id=tr_id,
            priority=Priority.TRADING,
        )
        execute(
            """
            INSERT INTO trades
                (date, ticker, name, action, order_type, exec_price,
                 quantity, status, signal_source, strategy_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(date.today()), ticker, pos.get("name", ""),
                "sell", "market", price, quantity,
                "filled", "overnight_judge", reason[:50],
            ),
        )
        # trailing_stop 레코드 삭제
        execute("DELETE FROM trailing_stop WHERE ticker = ?", (ticker,))
        logger.info(f"오버나잇 청산 완료 [{ticker}] {quantity}주 @ {price:,.0f}원 | {reason}")
    except Exception as e:
        logger.error(f"오버나잇 청산 실패 [{ticker}]: {e}")


# ──────────────────────────────────────────────
# 텔레그램 요약
# ──────────────────────────────────────────────

def _notify_overnight_summary(
    hold_list: list[dict],
    sell_list: list[dict],
    results: list[dict],
) -> None:
    """오버나잇 판단 결과 텔레그램 발송."""
    lines = ["🌙 <b>[15:10 오버나잇 판단]</b>", ""]

    result_map = {r["ticker"]: r for r in results}

    if hold_list:
        lines.append("🔵 <b>오버나잇 유지</b>")
        for pos in hold_list:
            r = result_map.get(pos["ticker"], {})
            lines.append(
                f"  • {pos['ticker']} {pos.get('name','')} "
                f"{r.get('pnl_pct',0):+.2f}% — {r.get('reason','')}"
            )

    if sell_list:
        lines.append("")
        lines.append("🔴 <b>당일 청산</b>")
        for pos in sell_list:
            r = result_map.get(pos["ticker"], {})
            lines.append(
                f"  • {pos['ticker']} {pos.get('name','')} "
                f"{r.get('pnl_pct',0):+.2f}% — {r.get('reason','')}"
            )

    if not hold_list and not sell_list:
        lines.append("보유 포지션 없음")

    notify("\n".join(lines))


# ──────────────────────────────────────────────
# 데이터 조회 헬퍼
# ──────────────────────────────────────────────

def _fetch_positions() -> list[dict]:
    """KIS 잔고에서 보유 포지션 조회."""
    from src.teams.position_monitor.engine import _fetch_positions as _pm_fetch
    return _pm_fetch()


def _load_global_context() -> dict:
    row = fetch_one(
        "SELECT global_risk_score, korea_market_outlook FROM global_condition ORDER BY created_at DESC LIMIT 1"
    )
    return dict(row) if row else {"global_risk_score": 5, "korea_market_outlook": "neutral"}


def _load_market_context() -> dict:
    row = fetch_one(
        "SELECT market_score, market_direction FROM market_condition ORDER BY created_at DESC LIMIT 1"
    )
    return dict(row) if row else {"market_score": 0.0, "market_direction": "neutral"}
