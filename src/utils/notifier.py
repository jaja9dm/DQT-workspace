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

import json
import threading
from datetime import datetime
from urllib.error import URLError
from urllib.request import Request, urlopen

from src.config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = 10
_lock = threading.Lock()  # 동시 발송 직렬화


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
    """일일 성과 리포트 발송."""
    date_str = report.get("date", datetime.now().strftime("%Y-%m-%d"))
    total_pnl_pct = report.get("total_pnl_pct", 0.0)
    trade_count = report.get("trade_count", 0)
    win_count = report.get("win_count", 0)
    loss_count = report.get("loss_count", 0)
    win_rate = report.get("win_rate", 0.0)

    pnl_emoji = "📈" if total_pnl_pct >= 0 else "📉"
    lines = [
        f"📊 <b>DQT 일일 리포트 — {date_str}</b>",
        "",
        f"{pnl_emoji} 당일 손익: <b>{total_pnl_pct:+.2f}%</b>",
        f"거래 건수: {trade_count}건 (익절 {win_count} / 손절 {loss_count})",
        f"승률: {win_rate:.1f}%",
        "",
    ]

    # 종목별 성과 (최대 5종목)
    positions = report.get("positions", [])
    if positions:
        lines.append("📋 <b>종목별 성과</b>")
        for pos in positions[:5]:
            p_emoji = "▲" if pos["pnl_pct"] >= 0 else "▼"
            lines.append(
                f"  {p_emoji} {pos['ticker']} {pos.get('name','')}: "
                f"{pos['pnl_pct']:+.2f}%"
            )

    # 알림
    alerts = report.get("alerts", [])
    if alerts:
        lines.append("")
        lines.append("⚠️ <b>주요 알림</b>")
        for alert in alerts[:3]:
            lines.append(f"  • {alert}")

    lines.append(f"\n<i>발송: {datetime.now().strftime('%Y-%m-%d %H:%M')}</i>")
    return notify("\n".join(lines))


def notify_error(source: str, message: str) -> bool:
    """시스템 에러 알림."""
    text = (
        f"🚨 <b>[에러] {source}</b>\n"
        f"{message}\n"
        f"<i>{datetime.now().strftime('%H:%M:%S')}</i>"
    )
    return notify(text)


# ── 내부 발송 ─────────────────────────────────────────────────

def _send(payload: dict) -> bool:
    """텔레그램 API HTTP 요청 (재시도 1회)."""
    url = _BASE_URL.format(token=settings.TELEGRAM_BOT_TOKEN)
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with _lock:
        for attempt in range(2):
            try:
                with urlopen(req, timeout=_TIMEOUT) as resp:
                    body = json.loads(resp.read())
                    if body.get("ok"):
                        return True
                    logger.warning(f"텔레그램 발송 실패: {body.get('description')}")
                    return False
            except URLError as e:
                if attempt == 0:
                    logger.warning(f"텔레그램 연결 오류 (재시도): {e}")
                else:
                    logger.error(f"텔레그램 발송 최종 실패: {e}")
            except Exception as e:
                logger.error(f"텔레그램 예외: {e}")
                return False
    return False
