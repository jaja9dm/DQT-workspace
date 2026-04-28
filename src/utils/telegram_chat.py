"""
telegram_chat.py — 텔레그램 양방향 AI 파트너

사용자가 텔레그램으로 메시지를 보내면 Claude가 DQT 시스템 데이터를 조회해
종목 분석·전략 토론을 하고, 사용자 동의 하에 관심종목을 실제로 변경한다.

대화 이력: 세션 메모리 유지 (최대 30턴, 시스템 재시작 시 초기화)
변경 툴(add/remove/replace): 반드시 사용자 동의 확인 후 실행

명령어:
  /start  — 인사 + 현황 요약
  /reset  — 대화 이력 초기화
  일반 텍스트 — Claude 자유 대화
"""
from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime
from typing import Any

import anthropic
import requests as _requests

# HTTP 세션 재사용 (keep-alive) — 매번 TLS 핸드셰이크 방지
_session = _requests.Session()
_session.headers.update({"Content-Type": "application/json"})

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_BASE_URL = "https://api.telegram.org/bot{token}/{method}"
_POLL_TIMEOUT = 10      # long polling 대기 시간 (초) — 짧게 해야 재시작 시 409 회피
_HISTORY_MAX  = 30      # 채팅별 최대 보관 메시지 수 (15회 왕복)
_MAX_TOKENS   = 1024

# chat_id → [{"role": "user"|"assistant", "content": str}, ...]
_chat_history: dict[str, list[dict]] = {}
_history_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────
# Telegram HTTP 헬퍼
# ─────────────────────────────────────────────────────────────

def _tg(method: str, payload: dict | None = None, timeout: int = 10) -> dict:
    url = _BASE_URL.format(token=settings.TELEGRAM_BOT_TOKEN, method=method)
    try:
        resp = _session.post(url, json=payload or {}, timeout=timeout)
        if resp.status_code == 409 and method == "getUpdates":
            wait = _POLL_TIMEOUT + 5
            logger.debug(f"Telegram 409: 이전 세션 만료 대기 {wait}s")
            time.sleep(wait)
            return {}
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.debug(f"Telegram {method} 오류: {e}")
        return {}


def _send_message(chat_id: str, text: str) -> None:
    text = text[:4000]
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for _ in range(3):  # 최대 3회 재시도
        result = _tg("sendMessage", payload, timeout=20)
        if result.get("ok") or result:
            break
        time.sleep(2)


def _send_typing(chat_id: str) -> None:
    _tg("sendChatAction", {"chat_id": chat_id, "action": "typing"}, timeout=5)


# ─────────────────────────────────────────────────────────────
# Claude Tools 정의
# ─────────────────────────────────────────────────────────────

_TOOLS: list[dict] = [
    {
        "name": "get_hot_list",
        "description": "오늘 관심종목(hot_list)과 슬롯 배치 현황 조회",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_positions",
        "description": "현재 보유 포지션, 슬롯 상태, 오늘 실현손익 조회",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_stock_data",
        "description": "특정 종목의 현재 지표 조회. 오늘 hot_list에 있으면 저장된 데이터 반환, 없으면 KIS API로 실시간 조회",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "종목코드 6자리 (예: 005930)"},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_market_condition",
        "description": "현재 시장 상황 조회: KOSPI/KOSDAQ 등락, 외인/기관 동향, 리스크 레벨, 글로벌 리스크",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_trade_history",
        "description": "최근 매매 내역과 손익 조회",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "조회 일수 (기본 1 = 오늘만)", "default": 1},
            },
        },
    },
    {
        "name": "add_to_hot_list",
        "description": (
            "종목을 관심종목에 추가하고 슬롯에 배치. 실제 매매 대상이 된다. "
            "반드시 분석 근거를 설명하고 사용자 동의('응', '해줘', '그래' 등)를 받은 후에만 사용할 것."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "종목코드 6자리"},
                "name":   {"type": "string", "description": "종목명"},
                "slot": {
                    "type": "string",
                    "enum": ["leader", "breakout", "pullback"],
                    "description": "leader=모멘텀주도주, breakout=갭업돌파, pullback=눌림반등",
                },
                "signal_type": {
                    "type": "string",
                    "description": "신호 유형 (momentum / gap_up_breakout / pullback_rebound 등)",
                },
                "reason": {"type": "string", "description": "추가 근거 (분석 내용 + 사용자 판단 요약)"},
            },
            "required": ["ticker", "name", "slot", "reason"],
        },
    },
    {
        "name": "remove_from_hot_list",
        "description": (
            "종목을 관심종목에서 제거하고 슬롯을 해제. "
            "반드시 사용자 동의를 받은 후에만 사용할 것."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "종목코드 6자리"},
                "reason": {"type": "string", "description": "제거 이유"},
            },
            "required": ["ticker", "reason"],
        },
    },
    {
        "name": "replace_slot",
        "description": (
            "슬롯의 종목을 교체. 기존 종목 제거 + 새 종목 추가를 한 번에 수행. "
            "반드시 사용자 동의를 받은 후에만 사용할 것."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "slot": {"type": "string", "enum": ["leader", "breakout", "pullback"]},
                "old_ticker": {"type": "string", "description": "현재 슬롯에 있는 종목코드"},
                "new_ticker": {"type": "string", "description": "새로 배치할 종목코드"},
                "new_name":   {"type": "string", "description": "새 종목명"},
                "signal_type": {"type": "string", "default": "momentum"},
                "reason": {"type": "string"},
            },
            "required": ["slot", "old_ticker", "new_ticker", "new_name", "reason"],
        },
    },
    {
        "name": "buy_stock",
        "description": (
            "KIS API로 시장가 매수 주문 실행. "
            "amount(원) 또는 quantity(주) 중 하나를 지정. "
            "반드시 종목 분석 후 사용자 동의('사줘', '매수해줘', '응' 등)를 받은 후에만 실행."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker":   {"type": "string", "description": "종목코드 6자리"},
                "name":     {"type": "string", "description": "종목명"},
                "amount":   {"type": "integer", "description": "매수 금액 (원). quantity 없으면 필수."},
                "quantity": {"type": "integer", "description": "매수 수량 (주). amount 없으면 필수."},
                "slot":     {"type": "string", "enum": ["leader", "breakout", "pullback"], "description": "슬롯 배정 (기본 leader)"},
                "reason":   {"type": "string", "description": "매수 근거"},
            },
            "required": ["ticker", "name", "reason"],
        },
    },
    {
        "name": "sell_stock",
        "description": (
            "KIS API로 시장가 매도 주문 실행. "
            "quantity(주) 또는 pct(보유수량 %)로 지정. 미지정 시 전량 매도. "
            "반드시 사용자 동의('팔아줘', '매도해줘', '응' 등)를 받은 후에만 실행."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker":   {"type": "string", "description": "종목코드 6자리"},
                "quantity": {"type": "integer", "description": "매도 수량. 미지정 시 전량."},
                "pct":      {"type": "number", "description": "보유수량 대비 비율 (0~100). quantity 없을 때 사용."},
                "reason":   {"type": "string", "description": "매도 근거"},
            },
            "required": ["ticker", "reason"],
        },
    },
]


# ─────────────────────────────────────────────────────────────
# Tool 실행 함수
# ─────────────────────────────────────────────────────────────

def _exec_tool(name: str, inputs: dict) -> str:
    try:
        if name == "get_hot_list":
            return _tool_get_hot_list()
        elif name == "get_positions":
            return _tool_get_positions()
        elif name == "get_stock_data":
            return _tool_get_stock_data(inputs["ticker"])
        elif name == "get_market_condition":
            return _tool_get_market_condition()
        elif name == "get_trade_history":
            return _tool_get_trade_history(int(inputs.get("days", 1)))
        elif name == "add_to_hot_list":
            return _tool_add_to_hot_list(inputs)
        elif name == "remove_from_hot_list":
            return _tool_remove_from_hot_list(inputs["ticker"], inputs["reason"])
        elif name == "replace_slot":
            return _tool_replace_slot(inputs)
        elif name == "buy_stock":
            return _tool_buy_stock(inputs)
        elif name == "sell_stock":
            return _tool_sell_stock(inputs)
        return f"알 수 없는 툴: {name}"
    except Exception as e:
        logger.warning(f"툴 실행 오류 [{name}]: {e}")
        return f"오류: {type(e).__name__}: {e}"


def _tool_get_hot_list() -> str:
    today = str(date.today())
    rows = fetch_all(
        """
        SELECT h.ticker, h.name, h.signal_type, h.momentum_score,
               h.price_change_pct, h.rsi, h.volume_ratio, h.obv_slope,
               h.exec_strength, h.frgn_net_buy, h.inst_net_buy,
               COALESCE(h.slot, '') AS slot,
               COALESCE(sa.health_score, 100) AS health_score,
               COALESCE(sa.replace_requested, 0) AS replace_requested
        FROM hot_list h
        LEFT JOIN slot_assignments sa ON sa.ticker = h.ticker AND sa.trade_date = ?
        WHERE DATE(h.created_at) = ?
        GROUP BY h.ticker
        ORDER BY h.momentum_score DESC
        """,
        (today, today),
    )
    if not rows:
        return f"오늘({today}) 관심종목 없음"

    _slot_label = {"leader": "👑주도주", "breakout": "🚀돌파", "pullback": "🔄눌림목"}
    lines = [f"📋 오늘 관심종목 — {len(rows)}종목"]
    for r in rows:
        slot_tag = _slot_label.get(r["slot"], "미배정")
        health = f" 건강:{r['health_score']:.0f}" if r["slot"] else ""
        warn = " ⚠️교체요청" if r["replace_requested"] else ""
        lines.append(
            f"\n[{r['ticker']}] {r['name']} {slot_tag}{health}{warn}\n"
            f"  점수:{r['momentum_score']:.0f} | 등락:{r['price_change_pct']:+.1f}%"
            f" | RSI:{r['rsi']:.0f} | 거래량:{r['volume_ratio']:.1f}배\n"
            f"  OBV:{r['obv_slope']:+.3f} | 체결강도:{r['exec_strength']:.0f}"
            f" | 외인:{r['frgn_net_buy']:+,}주 | 기관:{r['inst_net_buy']:+,}주"
        )
    return "\n".join(lines)


def _tool_get_positions() -> str:
    today = str(date.today())
    rows = fetch_all(
        """
        SELECT ts.ticker, ts.entry_price, ts.trailing_floor, ts.highest_price,
               ts.trigger_pct, ts.floor_pct,
               COALESCE(sa.name, ts.ticker) AS name,
               COALESCE(sa.slot, '') AS slot,
               COALESCE(sa.health_score, 100) AS health_score,
               COALESCE(sa.signal_type, '') AS signal_type
        FROM trailing_stop ts
        LEFT JOIN slot_assignments sa ON sa.ticker = ts.ticker AND sa.trade_date = ?
        """,
        (today,),
    )
    pnl_row = fetch_one(
        "SELECT COALESCE(SUM(pnl), 0) AS total FROM trades WHERE date=? AND pnl IS NOT NULL",
        (today,),
    )
    total_pnl = float(pnl_row["total"]) if pnl_row else 0.0

    _slot_label = {"leader": "👑주도주", "breakout": "🚀돌파", "pullback": "🔄눌림목"}
    pnl_sign = "+" if total_pnl >= 0 else ""

    if not rows:
        return f"보유 포지션 없음\n오늘 실현손익: {pnl_sign}{total_pnl:,.0f}원"

    lines = [f"💼 보유 포지션 {len(rows)}종목"]
    for r in rows:
        slot_tag = _slot_label.get(r["slot"], "")
        entry = float(r["entry_price"])
        floor_ = float(r["trailing_floor"])
        high  = float(r["highest_price"])
        pnl_est_pct = (high - entry) / entry * 100 if entry > 0 else 0
        lines.append(
            f"\n[{r['ticker']}] {r['name']} {slot_tag}\n"
            f"  진입:{entry:,.0f} | 현재최고:{high:,.0f}({pnl_est_pct:+.1f}%)\n"
            f"  손절선:{floor_:,.0f} | 건강:{r['health_score']:.0f}점"
        )
    lines.append(f"\n💰 오늘 실현손익: {pnl_sign}{total_pnl:,.0f}원")
    return "\n".join(lines)


def _tool_get_stock_data(ticker: str) -> str:
    today = str(date.today())
    # hot_list 오늘 데이터 우선
    row = fetch_one(
        """
        SELECT ticker, name, signal_type, momentum_score, price_change_pct,
               rsi, volume_ratio, obv_slope, exec_strength, atr_pct,
               frgn_net_buy, inst_net_buy, trading_value, created_at
        FROM hot_list
        WHERE ticker = ? AND DATE(created_at) = ?
        ORDER BY created_at DESC LIMIT 1
        """,
        (ticker, today),
    )
    if row:
        r = dict(row)
        return (
            f"📊 {r.get('name', ticker)} ({ticker}) — 오늘 스캔 기준\n"
            f"  등락: {r.get('price_change_pct', 0):+.1f}% | RSI: {r.get('rsi', 0):.0f}\n"
            f"  거래량비: {r.get('volume_ratio', 0):.1f}배 | OBV기울기: {r.get('obv_slope', 0):+.3f}\n"
            f"  모멘텀점수: {r.get('momentum_score', 0):.0f}/130 | 체결강도: {r.get('exec_strength', 100):.0f}\n"
            f"  ATR: {r.get('atr_pct', 0):.2f}% | 신호: {r.get('signal_type', '-')}\n"
            f"  외인순매수: {r.get('frgn_net_buy', 0):+,}주 | 기관순매수: {r.get('inst_net_buy', 0):+,}주\n"
            f"  스캔시각: {str(r.get('created_at', ''))[-8:-3]}"
        )

    # KIS API 실시간 조회
    try:
        from src.teams.domestic_stock.collector import _fetch_price_from_kis
        price, chg, vol, tval, frgn, inst, high, low, open_, exec_str = _fetch_price_from_kis(ticker)
        if price > 0:
            return (
                f"📊 {ticker} — 실시간 조회 (hot_list 미등재)\n"
                f"  현재가: {price:,.0f}원 | 등락: {chg:+.1f}%\n"
                f"  거래량: {vol:,}주 | 거래대금: {tval/1e8:.1f}억\n"
                f"  외인순매수: {frgn:+,}주 | 기관순매수: {inst:+,}주\n"
                f"  체결강도: {exec_str:.0f} | 고가: {high:,.0f} | 저가: {low:,.0f}\n"
                f"  ※ RSI·OBV 등 기술적 지표는 장중 스캔 후 확인 가능"
            )
    except Exception as e:
        logger.debug(f"KIS 실시간 조회 실패 [{ticker}]: {e}")

    return f"{ticker} 데이터 없음 — hot_list 미등재, KIS 조회 실패 (장외 시간이거나 종목코드 확인 필요)"


def _tool_get_market_condition() -> str:
    mc = fetch_one("SELECT * FROM market_condition ORDER BY created_at DESC LIMIT 1")
    gc = fetch_one(
        "SELECT global_risk_score, korea_market_outlook FROM global_condition ORDER BY created_at DESC LIMIT 1"
    )
    risk_row = fetch_one(
        "SELECT risk_level FROM risk_status ORDER BY created_at DESC LIMIT 1"
    )

    lines = ["📈 시장 현황"]
    if mc:
        mc = dict(mc)
        summary = {}
        try:
            summary = json.loads(mc.get("summary") or "{}")
        except Exception:
            pass
        kospi  = summary.get("kospi", 0)
        kosdaq = summary.get("kosdaq", 0)
        dir_kr = {"bullish": "강세", "bearish": "약세", "neutral": "중립"}.get(
            mc.get("market_direction", "neutral"), "중립"
        )
        fnet = mc.get("foreign_net_buy_bn", 0) or 0
        inet = mc.get("institutional_net_buy_bn", 0) or 0
        lines += [
            f"  KOSPI {kospi:+.2f}% | KOSDAQ {kosdaq:+.2f}%",
            f"  시장방향: {dir_kr} | 점수: {mc.get('market_score', 0):+.2f}",
            f"  외인: {'매수' if fnet > 100 else '매도' if fnet < -100 else '중립'}"
            f" | 기관: {'매수' if inet > 50 else '매도' if inet < -50 else '중립'}",
        ]
    if gc:
        outlook_kr = {"positive": "긍정", "negative": "부정", "neutral": "중립"}.get(
            gc["korea_market_outlook"], gc["korea_market_outlook"]
        )
        lines.append(f"  글로벌리스크: {gc['global_risk_score']}/10 | 한국전망: {outlook_kr}")
    if risk_row:
        level = risk_row["risk_level"]
        level_kr = {1: "정상", 2: "주의", 3: "경계", 4: "위험", 5: "극위험"}.get(level, str(level))
        lines.append(f"  리스크레벨: {level}단계 ({level_kr})")
    return "\n".join(lines) if len(lines) > 1 else "시장 데이터 없음 (스캔 전 또는 장외)"


def _tool_get_trade_history(days: int = 1) -> str:
    rows = fetch_all(
        """
        SELECT ticker, name, action, exec_price, quantity, pnl, pnl_pct, date,
               strftime('%H:%M', created_at) AS hhmm
        FROM trades
        WHERE date >= date('now', ? || ' days')
        ORDER BY created_at DESC LIMIT 30
        """,
        (f"-{days - 1}",),
    )
    if not rows:
        return f"최근 {days}일간 매매 없음"

    _action_kr = {
        "buy": "매수", "sell": "매도", "stop_loss": "손절",
        "take_profit": "익절", "time_cut": "시간청산",
        "force_close": "강제청산", "partial_exit": "부분익절",
    }
    sell_rows = [r for r in rows if r["action"] != "buy"]
    total_pnl = sum(float(r["pnl"] or 0) for r in sell_rows)
    wins = sum(1 for r in sell_rows if (r["pnl_pct"] or 0) > 0)
    pnl_sign = "+" if total_pnl >= 0 else ""

    summary = (
        f"📜 매매내역 {len(rows)}건"
        + (f" | 승률 {wins}/{len(sell_rows)} | 실현손익 {pnl_sign}{total_pnl:,.0f}원" if sell_rows else "")
    )
    lines = [summary]
    for r in rows:
        pnl_str = f" ({float(r['pnl_pct'] or 0):+.2f}%)" if r["action"] != "buy" else ""
        lines.append(
            f"  [{r['date']} {r['hhmm']}] {_action_kr.get(r['action'], r['action'])}"
            f" {r['name']}({r['ticker']}) {float(r['exec_price']):,.0f}원×{r['quantity']}주{pnl_str}"
        )
    return "\n".join(lines)


def _tool_add_to_hot_list(inputs: dict) -> str:
    ticker      = inputs["ticker"]
    name        = inputs.get("name", ticker)
    slot        = inputs["slot"]
    signal_type = inputs.get("signal_type", "momentum")
    reason      = inputs["reason"]
    today       = str(date.today())

    existing = fetch_one(
        "SELECT ticker FROM hot_list WHERE ticker = ? AND DATE(created_at) = ?",
        (ticker, today),
    )
    if existing:
        execute(
            "UPDATE hot_list SET slot = ?, signal_type = ?, created_at = CURRENT_TIMESTAMP WHERE ticker = ? AND DATE(created_at) = ?",
            (slot, signal_type, ticker, today),
        )
    else:
        execute(
            """
            INSERT OR REPLACE INTO hot_list
                (ticker, name, signal_type, volume_ratio, price_change_pct,
                 rsi, momentum_score, obv_slope, exec_strength,
                 frgn_net_buy, inst_net_buy, atr_pct, slot)
            VALUES (?, ?, ?, 1.0, 0.0, 50.0, 50.0, 0.0, 100.0, 0, 0, 0.0, ?)
            """,
            (ticker, name, signal_type, slot),
        )

    execute(
        """
        INSERT INTO slot_assignments (slot, ticker, name, signal_type, reason, trade_date, status)
        VALUES (?, ?, ?, ?, ?, ?, 'active')
        ON CONFLICT(slot) DO UPDATE SET
            ticker            = excluded.ticker,
            name              = excluded.name,
            signal_type       = excluded.signal_type,
            reason            = excluded.reason,
            trade_date        = excluded.trade_date,
            status            = 'active',
            health_score      = 100.0,
            replace_requested = 0,
            updated_at        = CURRENT_TIMESTAMP
        """,
        (slot, ticker, name, signal_type, reason, today),
    )
    _slot_label = {"leader": "👑주도주", "breakout": "🚀돌파", "pullback": "🔄눌림목"}
    return f"✅ {name}({ticker}) → {_slot_label.get(slot, slot)} 슬롯 추가 완료\n근거: {reason}"


def _tool_remove_from_hot_list(ticker: str, reason: str) -> str:
    today = str(date.today())
    row = fetch_one(
        "SELECT name, slot FROM hot_list WHERE ticker = ? AND DATE(created_at) = ? LIMIT 1",
        (ticker, today),
    )
    name = row["name"] if row else ticker
    slot = row["slot"] if row else None

    execute(
        "DELETE FROM hot_list WHERE ticker = ? AND DATE(created_at) = ?",
        (ticker, today),
    )
    if slot:
        execute(
            "UPDATE slot_assignments SET status = 'empty', updated_at = CURRENT_TIMESTAMP WHERE slot = ? AND trade_date = ?",
            (slot, today),
        )
        _slot_label = {"leader": "👑주도주", "breakout": "🚀돌파", "pullback": "🔄눌림목"}
        return f"🗑 {name}({ticker}) 제거 완료 ({_slot_label.get(slot, slot)} 슬롯 해제)\n이유: {reason}"
    return f"🗑 {name}({ticker}) 제거 완료\n이유: {reason}"


def _tool_replace_slot(inputs: dict) -> str:
    _tool_remove_from_hot_list(inputs["old_ticker"], f"슬롯 교체: {inputs['reason']}")
    result = _tool_add_to_hot_list({
        "ticker":      inputs["new_ticker"],
        "name":        inputs.get("new_name", inputs["new_ticker"]),
        "slot":        inputs["slot"],
        "signal_type": inputs.get("signal_type", "momentum"),
        "reason":      inputs["reason"],
    })
    return f"🔄 교체 완료: {inputs['old_ticker']} → {inputs['new_ticker']}\n{result}"


def _tool_buy_stock(inputs: dict) -> str:
    from src.infra.kis_gateway import KISGateway
    from src.infra.database import execute as db_execute

    ticker  = inputs["ticker"]
    name    = inputs["name"]
    reason  = inputs["reason"]
    gw      = KISGateway()

    # 중복 매수 방지 (오늘 이미 매수했으면 거부)
    today = str(date.today())
    existing = fetch_one(
        "SELECT id FROM trades WHERE ticker=? AND action='buy' AND date=? AND signal_source='chat' LIMIT 1",
        (ticker, today),
    )
    if existing:
        return f"⚠️ 오늘 이미 {name}({ticker}) 매수 완료. 중복 주문 방지."

    # 현재가 조회
    try:
        price_data = gw.get_current_price(ticker)
        current_price = int(price_data.get("stck_prpr", 0))
    except Exception:
        current_price = 0

    # 수량 계산
    quantity = inputs.get("quantity")
    if not quantity:
        amount = inputs.get("amount", 0)
        if amount and current_price:
            quantity = max(1, int(amount / current_price))
        else:
            return "오류: amount(원) 또는 quantity(주) 중 하나를 지정해야 합니다."

    if current_price == 0:
        return f"오류: {ticker} 현재가 조회 실패"
    if quantity <= 0:
        return "오류: 수량은 1 이상이어야 합니다."

    # KIS 시장가 매수
    try:
        resp = gw.place_order(ticker, "buy", quantity=quantity, price=0)
        order_no = resp.get("output", {}).get("ODNO", "")
    except Exception as e:
        return f"매수 주문 실패: {e}"

    # trades 테이블 기록
    today = str(date.today())
    try:
        db_execute(
            """INSERT INTO trades
               (date, ticker, name, action, order_type, order_price, exec_price,
                quantity, tranche, status, signal_source)
               VALUES (?, ?, ?, 'buy', 'market', ?, ?, ?, 1, 'filled', 'chat')""",
            (today, ticker, name, current_price, current_price, quantity),
        )
    except Exception:
        pass

    # hot_list 등록 + 슬롯 배정 → 이후 position_monitor·trading engine이 자동 관리
    slot = inputs.get("slot", "leader")
    try:
        db_execute(
            """INSERT OR REPLACE INTO hot_list
               (ticker, name, signal_type, momentum_score, price_change_pct,
                volume_ratio, slot, created_at)
               VALUES (?, ?, 'chat_buy', 80.0, 0.0, 1.0, ?, CURRENT_TIMESTAMP)""",
            (ticker, name, slot),
        )
        db_execute(
            """INSERT INTO slot_assignments
               (slot, ticker, name, signal_type, reason, trade_date, status,
                health_score, replace_requested)
               VALUES (?, ?, ?, 'chat_buy', ?, ?, 'active', 100.0, 0)
               ON CONFLICT(slot) DO UPDATE SET
                   ticker=excluded.ticker, name=excluded.name,
                   signal_type=excluded.signal_type, reason=excluded.reason,
                   trade_date=excluded.trade_date, status='active',
                   health_score=100.0, replace_requested=0,
                   updated_at=CURRENT_TIMESTAMP""",
            (slot, ticker, name, reason, today),
        )
        # trailing stop 초기화 (초기 손절 2%)
        floor = round(current_price * 0.98)
        db_execute(
            """INSERT OR REPLACE INTO trailing_stop
               (ticker, entry_price, current_floor, trigger_pct, floor_pct, updated_at)
               VALUES (?, ?, ?, 3.0, 2.5, CURRENT_TIMESTAMP)""",
            (ticker, current_price, floor),
        )
    except Exception as e:
        logger.warning(f"자동 슬롯 등록 실패 [{ticker}]: {e}")

    total = current_price * quantity
    return (
        f"✅ 매수 완료 — 이후 자동 관리 시작\n"
        f"  종목: {name}({ticker})\n"
        f"  수량: {quantity:,}주 @ {current_price:,}원\n"
        f"  총액: {total:,}원\n"
        f"  슬롯: {slot} | 손절선: {floor:,}원\n"
        f"  주문번호: {order_no}\n"
        f"  근거: {reason}"
    )


def _tool_sell_stock(inputs: dict) -> str:
    from src.infra.kis_gateway import KISGateway
    from src.infra.database import execute as db_execute

    ticker = inputs["ticker"]
    reason = inputs["reason"]
    gw     = KISGateway()

    # 현재 보유수량 조회
    try:
        balance = gw.get_balance()
        holdings = {item["pdno"]: item for item in balance.get("output1", [])}
        held = holdings.get(ticker)
        if not held:
            return f"오류: {ticker} 보유 없음"
        held_qty   = int(held.get("hldg_qty", 0))
        held_name  = held.get("prdt_name", ticker)
        avg_price  = float(held.get("pchs_avg_pric", 0))
        curr_price = int(held.get("prpr", 0))
    except Exception as e:
        return f"보유수량 조회 실패: {e}"

    # 매도 수량 결정
    quantity = inputs.get("quantity")
    if not quantity:
        pct = inputs.get("pct", 100)
        quantity = max(1, int(held_qty * pct / 100))

    quantity = min(quantity, held_qty)
    if quantity <= 0:
        return "오류: 매도 수량이 0입니다."

    # KIS 시장가 매도
    try:
        resp = gw.place_order(ticker, "sell", quantity=quantity, price=0)
        order_no = resp.get("output", {}).get("ODNO", "")
    except Exception as e:
        return f"매도 주문 실패: {e}"

    # trades 테이블 기록
    pnl = (curr_price - avg_price) * quantity if avg_price else 0
    try:
        db_execute(
            """INSERT INTO trades
               (date, ticker, name, action, order_type, order_price, exec_price,
                quantity, tranche, status, signal_source, pnl)
               VALUES (?, ?, ?, 'sell', 'market', ?, ?, ?, 1, 'filled', 'chat', ?)""",
            (str(date.today()), ticker, held_name, curr_price, curr_price, quantity, pnl),
        )
    except Exception:
        pass

    pnl_sign = "+" if pnl >= 0 else ""
    return (
        f"✅ 매도 완료\n"
        f"  종목: {held_name}({ticker})\n"
        f"  수량: {quantity:,}주 @ {curr_price:,}원\n"
        f"  손익: {pnl_sign}{pnl:,.0f}원\n"
        f"  주문번호: {order_no}\n"
        f"  근거: {reason}"
    )


# ─────────────────────────────────────────────────────────────
# Claude 대화 처리
# ─────────────────────────────────────────────────────────────

def _system_prompt() -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"""당신은 DQT(한국 주식 단타 트레이딩 시스템)의 AI 파트너입니다.
트레이더와 함께 실시간으로 매매 전략을 논의하고, 종목 선택을 도와줍니다.

현재 시각: {now}

역할:
- 사용자가 종목에 대해 의견을 말하면 먼저 get_stock_data로 지표를 확인하고 함께 분석
- 사용자가 관심종목이 마음에 안 든다면 이유를 탐색하고 대안을 찾아 제시
- 사용자가 특정 종목 추천 시 지표 확인 후 슬롯 배치 의견을 제시하고 동의 받아 반영
- 단순 조회(포지션, 손익, 시황)는 즉시 조회 후 답변

슬롯 구조:
- leader (👑주도주): 당일 가장 강한 모멘텀 종목, 메인 포지션
- breakout (🚀돌파): 갭업 또는 저항선 돌파 종목, 불타기 전략
- pullback (🔄눌림목): 강한 종목의 일시 조정, 물타기 전략

중요 규칙:
1. add_to_hot_list, remove_from_hot_list, replace_slot, buy_stock, sell_stock은 실제 시스템을 변경합니다
2. 변경 전 반드시 분석 근거를 설명하고 "바꿀까요?", "살까요?", "팔까요?" 형태로 동의를 구하세요
3. 사용자가 "응", "해줘", "그래", "사줘", "팔아줘", "매수해줘", "매도해줘" 등으로 동의하면 즉시 실행하세요
4. 매수 시: get_stock_data로 현재가·지표 확인 → 금액 또는 수량 제안 → 동의 후 buy_stock 실행
5. 매도 시: get_positions로 보유수량 확인 → 전량/일부 제안 → 동의 후 sell_stock 실행
6. 데이터 없이 추측하지 말고 반드시 조회 후 판단하세요
7. 답변은 간결하게, 핵심 수치 중심으로 (텔레그램 메시지 특성상 짧게)"""


def _call_claude(chat_id: str, user_text: str) -> str:
    """사용자 메시지를 받아 Claude 응답 반환. 대화 이력 자동 관리."""
    with _history_lock:
        history = _chat_history.setdefault(chat_id, [])
        history.append({"role": "user", "content": user_text})
        # 최대 보관 수 초과 시 오래된 것 제거 (user/assistant 쌍 단위 유지)
        while len(history) > _HISTORY_MAX:
            history.pop(0)
        messages_snapshot = list(history)

    # tool_use 루프 — Claude가 툴을 다 쓸 때까지 반복
    api_messages = [{"role": m["role"], "content": m["content"]} for m in messages_snapshot]

    final_text = ""
    for _ in range(8):  # 최대 8번 툴 호출 허용
        try:
            response = _client.messages.create(
                model=settings.CLAUDE_MODEL_MAIN,
                max_tokens=_MAX_TOKENS,
                system=_system_prompt(),
                tools=_TOOLS,
                messages=api_messages,
                temperature=0,
            )
        except Exception as e:
            logger.error(f"Claude API 오류: {e}")
            return "Claude API 오류가 발생했습니다. 잠시 후 다시 시도해주세요."

        if response.stop_reason == "end_turn":
            final_text = "".join(
                b.text for b in response.content if hasattr(b, "text")
            )
            break

        if response.stop_reason == "tool_use":
            # 어시스턴트 메시지(tool_use 포함) 추가
            api_messages.append({"role": "assistant", "content": response.content})

            # 툴 실행 후 결과 추가
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    logger.info(f"[Chat Tool] {block.name}({block.input})")
                    result = _exec_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            api_messages.append({"role": "user", "content": tool_results})
            continue

        break  # stop_reason이 예상 밖이면 중단

    if not final_text:
        final_text = "응답을 받지 못했습니다."

    with _history_lock:
        _chat_history[chat_id].append({"role": "assistant", "content": final_text})

    return final_text


# ─────────────────────────────────────────────────────────────
# 텔레그램 봇 클래스
# ─────────────────────────────────────────────────────────────

class TelegramChatBot:
    """텔레그램 long-polling 기반 AI 파트너 봇."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="telegram-chat",
        )
        self._last_update_id = 0

    def start(self) -> None:
        if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
            logger.info("텔레그램 설정 없음 — 챗봇 비활성")
            return
        # webhook 제거 (409 Conflict 방지) 후 Telegram 서버 이전 세션 만료 대기
        _tg("deleteWebhook", {"drop_pending_updates": False})
        time.sleep(2)
        # 기존 pending 업데이트를 flush해서 offset 동기화
        result = _tg("getUpdates", {"offset": -1, "timeout": 1}, timeout=5)
        updates = result.get("result", [])
        if updates:
            self._last_update_id = updates[-1]["update_id"]
        time.sleep(1)
        self._thread.start()
        logger.info("텔레그램 AI 파트너 봇 시작")

    def stop(self) -> None:
        self._stop_event.set()

    def _poll_loop(self) -> None:
        retry_delay = 5
        while not self._stop_event.is_set():
            try:
                result = _tg(
                    "getUpdates",
                    {"offset": self._last_update_id + 1, "timeout": _POLL_TIMEOUT},
                    timeout=_POLL_TIMEOUT + 5,
                )
                updates = result.get("result", [])
                for update in updates:
                    self._last_update_id = max(self._last_update_id, update["update_id"])
                    self._handle_update(update)
                retry_delay = 5  # 성공 시 리셋
            except HTTPError as e:
                if e.code == 409:
                    # 이전 세션이 Telegram 서버에 남아있음 — 세션 만료까지 대기
                    logger.debug(f"폴링 409: 이전 세션 대기 중 ({_POLL_TIMEOUT + 5}s)")
                    time.sleep(_POLL_TIMEOUT + 5)
                else:
                    logger.debug(f"폴링 오류: {e}")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60)
            except Exception as e:
                logger.debug(f"폴링 오류: {e}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    def _handle_update(self, update: dict) -> None:
        msg = update.get("message") or update.get("edited_message")
        if not msg:
            return

        chat_id = str(msg.get("chat", {}).get("id", ""))
        text = msg.get("text", "").strip()

        if not text or not chat_id:
            return

        # 인가된 chat_id만 허용 (보안)
        if chat_id != str(settings.TELEGRAM_CHAT_ID):
            logger.debug(f"비인가 chat_id 무시: {chat_id}")
            return

        # 명령어 처리
        if text == "/start":
            _send_message(chat_id, (
                "👋 <b>DQT AI 파트너 시작</b>\n\n"
                "관심종목·포지션·시황을 자유롭게 물어보세요.\n"
                "종목 추가/제거도 대화로 할 수 있어요.\n\n"
                "/reset — 대화 이력 초기화"
            ))
            return

        if text == "/reset":
            with _history_lock:
                _chat_history.pop(chat_id, None)
            _send_message(chat_id, "🔄 대화 이력을 초기화했습니다.")
            return

        # Claude 응답 (타이핑 표시 후 처리)
        _send_typing(chat_id)
        threading.Thread(
            target=self._respond,
            args=(chat_id, text),
            daemon=True,
        ).start()

    def _respond(self, chat_id: str, text: str) -> None:
        try:
            reply = _call_claude(chat_id, text)
            _send_message(chat_id, reply)
        except Exception as e:
            logger.error(f"응답 생성 오류: {e}")
            _send_message(chat_id, "⚠️ 처리 중 오류가 발생했습니다.")
