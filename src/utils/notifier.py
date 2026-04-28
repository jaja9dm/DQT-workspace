"""
notifier.py — 텔레그램 공통 알림 모듈

모든 팀이 알림을 보낼 때 이 모듈을 통한다.
텔레그램 Bot API를 사용하며, 마크다운 포맷을 지원한다.

환경변수:
  TELEGRAM_BOT_TOKEN — BotFather에서 발급
  TELEGRAM_CHAT_ID   — 발송 대상 채널/그룹/개인 ID

사용법:
  from src.utils.notifier import notify, notify_trade, notify_risk

  notify("메시지 내용")
  notify_trade(ticker="005930", action="buy", quantity=10, price=75000)
  notify_risk(level=4, alerts=["포트폴리오 -5% 손실"])
"""

from __future__ import annotations

import threading
from datetime import datetime

import requests as _requests

from src.config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = 10
_lock = threading.Lock()  # 동시 발송 직렬화
_session = _requests.Session()
_session.headers.update({"Content-Type": "application/json"})


# ── 공개 API ──────────────────────────────────────────────────

def notify(text: str, parse_mode: str = "HTML") -> bool:
    """
    텔레그램으로 메시지 발송.

    Args:
        text: 발송 내용 (HTML 또는 MarkdownV2)
        parse_mode: "HTML" | "MarkdownV2"

    Returns:
        True if 성공
    """
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        logger.debug("텔레그램 설정 없음 — 알림 스킵")
        return False

    payload = {
        "chat_id": settings.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    return _send(payload)


def notify_trade(
    ticker: str,
    name: str,
    action: str,
    quantity: int,
    price: float,
    pnl_pct: float | None = None,
    reason: str = "",
) -> bool:
    """
    매매 체결 알림.

    action: buy | sell | stop_loss | take_profit | time_cut
    """
    emoji = {
        "buy": "🟢",
        "sell": "🔵",
        "stop_loss": "🔴",
        "take_profit": "💰",
        "time_cut": "⏰",
    }.get(action, "⚪")

    action_label = {
        "buy": "매수",
        "sell": "매도",
        "stop_loss": "손절",
        "take_profit": "익절",
        "time_cut": "타임컷",
    }.get(action, action)

    lines = [
        f"{emoji} <b>[{action_label}] {ticker} {name}</b>",
        f"수량: {quantity:,}주  @  {price:,.0f}원",
    ]
    if pnl_pct is not None:
        lines.append(f"손익: <b>{pnl_pct:+.2f}%</b>")
    if reason:
        lines.append(f"사유: {reason}")
    lines.append(f"<i>{datetime.now().strftime('%H:%M:%S')}</i>")

    return notify("\n".join(lines))


def notify_risk(level: int, alerts: list[str]) -> bool:
    """리스크 레벨 변경 알림."""
    emoji = {1: "🟢", 2: "🟡", 3: "🟠", 4: "🔴", 5: "🚨"}.get(level, "⚪")
    label = {
        1: "정상", 2: "주의", 3: "경계", 4: "위험", 5: "극위험"
    }.get(level, str(level))

    lines = [f"{emoji} <b>리스크 레벨 {level} — {label}</b>"]
    for alert in alerts[:5]:
        lines.append(f"• {alert}")
    lines.append(f"<i>{datetime.now().strftime('%H:%M:%S')}</i>")

    return notify("\n".join(lines))


def notify_daily_report(report: dict) -> bool:
    """일일 성과 리포트 발송 — 종목별 매수가·손익 상세 포함."""
    date_str     = report.get("date", datetime.now().strftime("%Y-%m-%d"))
    total_pnl_amt = report.get("total_pnl_amt", 0.0)
    trade_count  = report.get("trade_count", 0)
    win_count    = report.get("win_count", 0)
    loss_count   = report.get("loss_count", 0)
    win_rate     = report.get("win_rate", 0.0)
    profit_factor = report.get("profit_factor", 0.0)

    pnl_emoji = "📈" if total_pnl_amt >= 0 else "📉"
    pnl_str   = f"{total_pnl_amt:+,.0f}원"

    lines = [
        f"📊 <b>DQT 일일 리포트 — {date_str}</b>",
        "",
        f"{pnl_emoji} 실현 손익: <b>{pnl_str}</b>",
        f"거래: {trade_count}건 | 승률: {win_rate:.1f}% (익 {win_count} / 손 {loss_count})"
        + (f" | 손익비: {profit_factor:.2f}" if profit_factor > 0 else ""),
        "",
    ]

    # 종목별 성과 — 매수가·매도가·손익 상세
    positions = report.get("positions", [])
    if positions:
        lines.append("📋 <b>종목별 성과</b>")
        for pos in positions[:8]:
            pnl_pct = pos.get("pnl_pct", 0)
            pnl_amt = pos.get("pnl_amt", 0)
            p_emoji = "▲" if pnl_pct >= 0 else "▼"
            name_str = pos.get("name") or pos["ticker"]
            amt_str  = f" ({pnl_amt:+,.0f}원)" if pnl_amt else ""
            lines.append(
                f"  {p_emoji} {name_str}({pos['ticker']}): "
                f"<b>{pnl_pct:+.2f}%</b>{amt_str}"
            )
        lines.append("")

    # Hot List 적중률
    hl = report.get("hot_list_accuracy", {})
    if hl.get("total", 0) > 0:
        hl_rate = hl["win"] / hl["traded"] * 100 if hl.get("traded", 0) > 0 else 0
        lines.append(
            f"🎯 Hot List 적중: {hl.get('win',0)}/{hl.get('traded',0)}건 "
            f"({hl_rate:.0f}%) | 후보 {hl.get('total',0)}종목"
        )
        lines.append("")

    # 알림
    alerts = report.get("alerts", [])
    if alerts:
        lines.append("⚠️ <b>주요 알림</b>")
        for alert in alerts[:3]:
            lines.append(f"  • {alert}")
        lines.append("")

    lines.append(f"<i>발송: {datetime.now().strftime('%Y-%m-%d %H:%M')}</i>")
    return notify("\n".join(lines))


def notify_error(source: str, message: str) -> bool:
    """시스템 에러 알림."""
    text = (
        f"🚨 <b>[에러] {source}</b>\n"
        f"{message}\n"
        f"<i>{datetime.now().strftime('%H:%M:%S')}</i>"
    )
    return notify(text)


def check_claude_error(e: Exception, source: str) -> None:
    """
    Claude API 예외를 분석하여 잔액 부족 등 치명적 오류는 텔레그램으로 즉시 알림.

    Usage:
        except Exception as e:
            check_claude_error(e, "글로벌 시황")
            return fallback()
    """
    msg = str(e).lower()

    # 잔액 부족 (402 / credit balance)
    if any(kw in msg for kw in ("credit", "billing", "402", "payment", "insufficient")):
        notify(
            f"💳 <b>[Claude 잔액 부족]</b>\n"
            f"Anthropic API 크레딧이 소진되었습니다.\n"
            f"출처: {source}\n"
            f"→ console.anthropic.com 에서 충전 필요\n"
            f"<i>{datetime.now().strftime('%H:%M:%S')}</i>"
        )
    # 인증 오류 (401 / invalid key)
    elif any(kw in msg for kw in ("401", "authentication", "invalid api key", "permission")):
        notify(
            f"🔑 <b>[Claude 인증 오류]</b>\n"
            f"API 키가 유효하지 않습니다.\n"
            f"출처: {source}\n"
            f"<i>{datetime.now().strftime('%H:%M:%S')}</i>"
        )


# ── 내부 발송 ─────────────────────────────────────────────────

def _send(payload: dict) -> bool:
    """텔레그램 API HTTP 요청 (재시도 1회)."""
    url = _BASE_URL.format(token=settings.TELEGRAM_BOT_TOKEN)

    with _lock:
        for attempt in range(2):
            try:
                resp = _session.post(url, json=payload, timeout=_TIMEOUT)
                body = resp.json()
                if body.get("ok"):
                    return True
                logger.warning(f"텔레그램 발송 실패: {body.get('description')}")
                return False
            except Exception as e:
                if attempt == 0:
                    logger.warning(f"텔레그램 연결 오류 (재시도): {e}")
                else:
                    logger.error(f"텔레그램 발송 최종 실패: {e}")
    return False
