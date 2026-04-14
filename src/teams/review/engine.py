"""
engine.py — 일일 매매 복기 팀

역할:
  매일 장 마감 후 16:30에 자동 실행.
  오늘 발생한 모든 매매를 분석하고 무엇이 잘 됐는지, 무엇을 고쳐야 하는지
  Claude가 판단해 trade_review 테이블에 저장하고 Telegram으로 리포트한다.

출력:
  - trade_review 테이블에 당일 복기 레코드 저장
  - Telegram 복기 요약 발송
  - 연구소가 내일 전략 파라미터 조정에 참고

자기 개선 흐름:
  오늘 복기 → improvements JSON → 내일 _ask_claude / opening_gate 프롬프트 참고
  → 장기적으로 전략 파라미터 자동 튜닝의 기초 데이터로 활용
"""

from __future__ import annotations

import json
from datetime import date, datetime

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)


# ──────────────────────────────────────────────
# 메인 진입점
# ──────────────────────────────────────────────

def run_daily_review() -> dict | None:
    """
    오늘의 매매를 복기하고 trade_review에 저장.

    Returns:
        저장된 리뷰 딕셔너리 또는 None (매매 없는 날)
    """
    today = str(date.today())
    logger.info(f"=== 일일 복기 시작: {today} ===")

    # 이미 오늘 복기가 있으면 스킵 (재실행 방지)
    existing = fetch_one("SELECT id FROM trade_review WHERE review_date = ?", (today,))
    if existing:
        logger.info("오늘 복기 이미 완료됨 — 스킵")
        return None

    trades = _load_todays_trades(today)
    if not trades:
        logger.info("오늘 매매 내역 없음 — 복기 스킵")
        notify("📋 <b>[일일 복기]</b> 오늘 매매 없음")
        return None

    # 기초 통계
    stats = _calc_stats(trades)

    # 포지션 스냅샷 (오늘 보유 종목의 가격 흐름 맥락)
    snapshots = _load_snapshots_context(today, [t["ticker"] for t in trades])

    # Claude 분석
    review = _ask_claude_review(today, trades, stats, snapshots)

    # DB 저장
    _save_review(today, stats, review)

    # Telegram 발송
    _notify_review(today, stats, review)

    logger.info(f"일일 복기 완료 — 매매 {stats['total']}건, 수익 {stats['win']}건, 손실 {stats['loss']}건")
    return review


# ──────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────

def _load_todays_trades(today: str) -> list[dict]:
    """오늘 체결된 매매 내역 전체 조회."""
    rows = fetch_all(
        """
        SELECT ticker, name, action, order_type, exec_price,
               quantity, pnl, pnl_pct, signal_source, strategy_id, filled_at
        FROM trades
        WHERE date = ?
          AND status IN ('filled', 'pending')
        ORDER BY filled_at ASC
        """,
        (today,),
    )
    return [dict(r) for r in rows] if rows else []


def _load_snapshots_context(today: str, tickers: list[str]) -> dict[str, list[dict]]:
    """
    오늘 포지션 스냅샷 — 각 종목의 가격 흐름 (최대 10개 포인트).
    복기 맥락 제공용.
    """
    ctx: dict[str, list[dict]] = {}
    for ticker in set(tickers):
        rows = fetch_all(
            """
            SELECT pnl_pct, current_price, snapshot_at
            FROM position_snapshot
            WHERE ticker = ?
              AND date(snapshot_at) = ?
            ORDER BY snapshot_at ASC
            LIMIT 10
            """,
            (ticker, today),
        )
        if rows:
            ctx[ticker] = [dict(r) for r in rows]
    return ctx


# ──────────────────────────────────────────────
# 통계 계산
# ──────────────────────────────────────────────

def _calc_stats(trades: list[dict]) -> dict:
    """매매 기초 통계."""
    sell_trades = [t for t in trades if t["action"] in ("sell", "stop_loss", "take_profit", "time_cut")]
    buy_trades  = [t for t in trades if t["action"] == "buy"]

    win_trades  = [t for t in sell_trades if (t.get("pnl_pct") or 0) > 0]
    loss_trades = [t for t in sell_trades if (t.get("pnl_pct") or 0) <= 0]
    total_pnl   = sum((t.get("pnl") or 0) for t in sell_trades)

    best  = max(sell_trades, key=lambda t: t.get("pnl_pct") or 0) if sell_trades else None
    worst = min(sell_trades, key=lambda t: t.get("pnl_pct") or 0) if sell_trades else None

    return {
        "total":      len(trades),
        "buys":       len(buy_trades),
        "sells":      len(sell_trades),
        "win":        len(win_trades),
        "loss":       len(loss_trades),
        "total_pnl":  total_pnl,
        "win_rate":   len(win_trades) / len(sell_trades) if sell_trades else 0.0,
        "best":       best,
        "worst":      worst,
    }


# ──────────────────────────────────────────────
# Claude 분석
# ──────────────────────────────────────────────

def _ask_claude_review(
    today: str,
    trades: list[dict],
    stats: dict,
    snapshots: dict[str, list[dict]],
) -> dict:
    """
    Claude에게 오늘 매매 복기 및 개선점 분석 요청.

    Returns:
        {
          "pattern_hits":  [...],  # 잘 작동한 패턴
          "pattern_fails": [...],  # 실패한 패턴
          "improvements":  [...],  # 내일 당장 적용 가능한 개선점
          "summary":       "..."   # 한국어 총평
        }
    """
    # 매매 내역 텍스트화
    trade_lines = []
    for t in trades:
        pnl_str = f"손익 {t['pnl_pct']:+.2f}%" if t.get("pnl_pct") is not None else "손익 미확정"
        trade_lines.append(
            f"  - [{t['action']}] {t.get('name', t['ticker'])}({t['ticker']}) "
            f"{t['quantity']}주 @ {t.get('exec_price', 0):,.0f}원 | {pnl_str} | "
            f"사유: {t.get('strategy_id', '') or ''}"
        )

    # 가격 흐름 맥락
    snap_lines = []
    for ticker, snaps in snapshots.items():
        if snaps:
            first_pnl = snaps[0].get("pnl_pct", 0)
            last_pnl  = snaps[-1].get("pnl_pct", 0)
            snap_lines.append(f"  - {ticker}: 장중 손익 {first_pnl:+.1f}% → {last_pnl:+.1f}%")

    prompt = f"""당신은 국내 주식 퀀트 트레이딩 시스템의 성과 분석 AI입니다.
오늘({today}) 매매 전체를 분석하고, 무엇이 잘 됐는지·무엇을 고쳐야 하는지 판단하세요.

## 오늘 매매 요약
- 총 매매: {stats['total']}건 (매수 {stats['buys']}, 매도 {stats['sells']})
- 승률: {stats['win_rate']*100:.0f}% (수익 {stats['win']}건 / 손실 {stats['loss']}건)
- 당일 실현 손익: {stats['total_pnl']:+,.0f}원

## 매매 상세
{chr(10).join(trade_lines) if trade_lines else "  (없음)"}

## 장중 가격 흐름
{chr(10).join(snap_lines) if snap_lines else "  (없음)"}

## 시스템 로직 요약 (참고용)
- 매수: Hot List 기반, Claude 최종 판단, 분할 매수 (60/25/15%)
- 손절: 트레일링 스톱 (-5% 초기, 수익 시 상향), MACD 조기 손절
- 물타기: -1%~-5% 구간 + MACD 비약세 + Hot List 활성 시
- 불타기: +1.5% + MACD 강세 + 거래량 2배 + Hot List 30분 이내
- 익절: +5%/+10% MACD 연동, 물타기 후 MACD 반등 조기 탈출

## 분석 요청
1. **pattern_hits**: 오늘 잘 작동한 패턴·로직 (구체적으로, 최대 5개)
2. **pattern_fails**: 오늘 아쉬웠거나 실패한 패턴 (최대 5개)
3. **improvements**: 내일 당장 적용 가능한 개선 제안 (파라미터·로직 수정, 최대 5개)
4. **summary**: 오늘 하루 매매 총평 (3문장 이내, 한국어)

JSON만 응답:
{{
  "pattern_hits":  ["...", ...],
  "pattern_fails": ["...", ...],
  "improvements":  ["...", ...],
  "summary":       "..."
}}"""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=1024,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as e:
        logger.error(f"Claude 복기 분석 실패: {e}")
        return {
            "pattern_hits":  [],
            "pattern_fails": [],
            "improvements":  ["Claude 분석 실패 — 로그 확인 필요"],
            "summary":       "복기 분석 중 오류 발생.",
        }


# ──────────────────────────────────────────────
# DB 저장
# ──────────────────────────────────────────────

def _save_review(today: str, stats: dict, review: dict) -> None:
    best_t  = stats.get("best")
    worst_t = stats.get("worst")

    best_json  = json.dumps(
        {"ticker": best_t["ticker"],  "pnl_pct": best_t.get("pnl_pct"),  "reason": best_t.get("strategy_id")},
        ensure_ascii=False,
    ) if best_t else None
    worst_json = json.dumps(
        {"ticker": worst_t["ticker"], "pnl_pct": worst_t.get("pnl_pct"), "reason": worst_t.get("strategy_id")},
        ensure_ascii=False,
    ) if worst_t else None

    execute(
        """
        INSERT OR REPLACE INTO trade_review
            (review_date, total_trades, win_trades, loss_trades, total_pnl,
             best_trade, worst_trade, pattern_hits, pattern_fails, improvements, summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            today,
            stats["total"],
            stats["win"],
            stats["loss"],
            stats["total_pnl"],
            best_json,
            worst_json,
            json.dumps(review.get("pattern_hits",  []), ensure_ascii=False),
            json.dumps(review.get("pattern_fails", []), ensure_ascii=False),
            json.dumps(review.get("improvements",  []), ensure_ascii=False),
            review.get("summary", ""),
        ),
    )
    logger.info(f"trade_review 저장 완료: {today}")


# ──────────────────────────────────────────────
# Telegram 알림
# ──────────────────────────────────────────────

def _notify_review(today: str, stats: dict, review: dict) -> None:
    win_rate_str = f"{stats['win_rate']*100:.0f}%"
    pnl_str = f"{stats['total_pnl']:+,.0f}원" if stats['total_pnl'] else "미확정"
    pnl_emoji = "🟢" if (stats['total_pnl'] or 0) >= 0 else "🔴"

    lines = [
        f"📊 <b>[일일 복기] {today}</b>",
        f"매매 {stats['total']}건 | 승률 {win_rate_str} | {pnl_emoji} {pnl_str}",
        "",
    ]

    if review.get("pattern_hits"):
        lines.append("✅ <b>잘 된 것</b>")
        for h in review["pattern_hits"][:3]:
            lines.append(f"  • {h}")
        lines.append("")

    if review.get("pattern_fails"):
        lines.append("⚠️ <b>아쉬운 것</b>")
        for f_ in review["pattern_fails"][:3]:
            lines.append(f"  • {f_}")
        lines.append("")

    if review.get("improvements"):
        lines.append("🔧 <b>내일 개선 포인트</b>")
        for imp in review["improvements"][:3]:
            lines.append(f"  • {imp}")
        lines.append("")

    if review.get("summary"):
        lines.append(f"💬 {review['summary']}")

    notify("\n".join(lines))
