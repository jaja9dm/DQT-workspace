"""
generate_trading_journal.py — 일일 매매 일지 자동 생성

장 마감 후 16:45에 스케줄러가 자동 실행.
해당 날짜의 모든 거래를 분석하여 docs/trading_journal/YYYY-MM-DD.md 파일로 저장.

수수료 계산 기준 (KIS 실전 HTS):
  - 매수 수수료: 0.015% (온라인)
  - 매도 수수료: 0.015% (온라인)
  - 증권거래세: 0.18% (매도 시)
  - 왕복 총비용: 약 0.21% (매수 0.015% + 매도 0.015% + 세금 0.18%)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

# 프로젝트 루트를 PYTHONPATH에 추가
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from src.infra.database import fetch_all, fetch_one
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── 수수료 상수 (KIS 실전 온라인 기준) ────────────────
_BUY_FEE_RATE   = 0.00015   # 매수 수수료 0.015%
_SELL_FEE_RATE  = 0.00015   # 매도 수수료 0.015%
_TAX_RATE       = 0.0018    # 증권거래세 0.18% (매도 시)

_JOURNAL_DIR  = _ROOT / "docs" / "trading_journal"
_JOURNAL_FILE = _JOURNAL_DIR / "journal.md"


def _calc_fee(buy_amt: float, sell_amt: float) -> dict:
    """실제 수수료 + 세금 계산."""
    buy_fee  = buy_amt  * _BUY_FEE_RATE
    sell_fee = sell_amt * _SELL_FEE_RATE
    tax      = sell_amt * _TAX_RATE
    total    = buy_fee + sell_fee + tax
    return {
        "buy_fee":  round(buy_fee),
        "sell_fee": round(sell_fee),
        "tax":      round(tax),
        "total":    round(total),
    }


def _fmt_won(v: float | int) -> str:
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.0f}원"


def _fmt_pct(v: float) -> str:
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"


def generate(target_date: str | None = None) -> Path:
    """
    지정 날짜(기본: 오늘)의 매매 일지를 journal.md에 추가(prepend).
    이미 해당 날짜 섹션이 있으면 덮어씀.
    """
    today = target_date or date.today().isoformat()
    _JOURNAL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _JOURNAL_FILE

    # ── 1. 거래 이력 조회 ──────────────────────────────
    trades = fetch_all(
        """
        SELECT t.id, t.ticker, t.name, t.action, t.exec_price,
               t.quantity, t.pnl, t.pnl_pct, t.created_at, t.signal_source,
               tc.signal_type, tc.rsi, tc.entry_score, tc.momentum_score,
               tc.sector, tc.exec_strength, tc.entry_hhmm, tc.rs_daily
        FROM trades t
        LEFT JOIN trade_context tc ON tc.trade_id = t.id
        WHERE t.date = ?
        ORDER BY t.created_at
        """,
        (today,),
    )

    # ── 2. 시황 조회 ───────────────────────────────────
    market = fetch_one(
        """
        SELECT market_score, market_direction, foreign_net_buy_bn,
               institutional_net_buy_bn, advancing_stocks, declining_stocks, summary
        FROM market_condition
        WHERE DATE(created_at) = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (today,),
    )

    # ── 3. 복기 리포트 조회 ────────────────────────────
    review = fetch_one(
        "SELECT * FROM trade_review WHERE review_date = ?",
        (today,),
    )

    # ── 4. 예수금 조회 (복기 market_context에 포함돼 있으면 사용) ──
    cash_info: dict = {}
    if review and review["market_context"]:
        try:
            ctx = json.loads(review["market_context"])
            cash_info = ctx.get("portfolio", {})
        except Exception:
            pass

    # ── 5. 종목별 집계 ────────────────────────────────
    ticker_map: dict[str, dict] = {}
    for t in trades:
        tk = t["ticker"]
        if tk not in ticker_map:
            ticker_map[tk] = {
                "name": t["name"] or tk,
                "sector": t["sector"] or "-",
                "signal_type": t["signal_type"] or t["signal_source"] or "-",
                "buys": [],
                "sells": [],
                "rsi": t["rsi"],
                "entry_score": t["entry_score"],
                "momentum_score": t["momentum_score"],
                "rs_daily": t["rs_daily"],
            }
        action = t["action"]
        record = {
            "price": t["exec_price"] or 0,
            "qty":   t["quantity"] or 0,
            "time":  (t["created_at"] or "")[:16],
            "pnl":   t["pnl"],
            "pnl_pct": t["pnl_pct"],
        }
        if action == "buy":
            ticker_map[tk]["buys"].append(record)
        else:
            ticker_map[tk]["sells"].append(record)

    # ── 6. 수익 계산 ──────────────────────────────────
    total_buy_amt  = sum(
        r["price"] * r["qty"]
        for d in ticker_map.values()
        for r in d["buys"]
    )
    total_sell_amt = sum(
        r["price"] * r["qty"]
        for d in ticker_map.values()
        for r in d["sells"]
    )
    fees     = _calc_fee(total_buy_amt, total_sell_amt)
    gross_pnl = sum(
        (r["pnl"] or 0)
        for d in ticker_map.values()
        for r in d["sells"]
        if r["pnl"] is not None
    )
    net_pnl   = gross_pnl - fees["total"]

    sell_trades_cnt = sum(len(d["sells"]) for d in ticker_map.values())
    win_cnt  = sum(
        1 for d in ticker_map.values()
        for r in d["sells"] if (r["pnl"] or 0) > 0
    )
    loss_cnt = sell_trades_cnt - win_cnt
    win_rate = (win_cnt / sell_trades_cnt * 100) if sell_trades_cnt > 0 else 0.0

    # ── 7. 복기 내용 파싱 ─────────────────────────────
    def _parse_list(field: str) -> list[str]:
        if not review:
            return []
        val = review[field]
        if not val:
            return []
        try:
            return json.loads(val) if isinstance(val, str) else (val or [])
        except Exception:
            return [str(val)]

    pattern_hits  = _parse_list("pattern_hits")
    pattern_fails = _parse_list("pattern_fails")
    improvements  = _parse_list("improvements")
    summary_text  = review["summary"] if review else ""

    signal_analytics: dict = {}
    if review and review["signal_analytics"]:
        try:
            signal_analytics = json.loads(review["signal_analytics"])
        except Exception:
            pass

    # ── 8. Markdown 생성 ──────────────────────────────
    lines: list[str] = []

    def h(level: int, text: str):
        lines.append(f"\n{'#' * level} {text}\n")

    def row(*cells):
        lines.append("| " + " | ".join(str(c) for c in cells) + " |")

    # 날짜 섹션 헤더 (파일 내 구분자)
    lines.append(f"## {today}")
    lines.append("")
    lines.append(f"> 생성: {datetime.now().strftime('%Y-%m-%d %H:%M')} | KIS 실전투자")
    lines.append("")

    # ── 당일 요약 ──
    h(2, "📊 당일 요약")
    lines.append("| 항목 | 값 |")
    lines.append("|------|-----|")
    lines.append(f"| 거래 종목 수 | {len(ticker_map)}종목 |")
    lines.append(f"| 체결 건수 | 매수 {sum(len(d['buys']) for d in ticker_map.values())}건 / 매도 {sell_trades_cnt}건 |")
    lines.append(f"| 승률 | {win_rate:.0f}% ({win_cnt}승 {loss_cnt}패) |")
    lines.append(f"| 총 매수금액 | {total_buy_amt:,.0f}원 |")
    lines.append(f"| 총 매도금액 | {total_sell_amt:,.0f}원 |")
    lines.append(f"| 총 수익 (세전) | {_fmt_won(gross_pnl)} |")
    lines.append(f"| 수수료+세금 | -{fees['total']:,.0f}원 (매수수수료 {fees['buy_fee']:,}원 + 매도수수료 {fees['sell_fee']:,}원 + 거래세 {fees['tax']:,}원) |")
    lines.append(f"| **실현 순수익** | **{_fmt_won(net_pnl)}** |")
    if cash_info:
        lines.append(f"| 예수금 (장마감) | {cash_info.get('available_cash', 0):,.0f}원 |")
        lines.append(f"| 총 평가금액 | {cash_info.get('total_eval_amt', 0):,.0f}원 |")
    lines.append("")

    # ── 시황 ──
    h(2, "🌏 당일 시황")
    if market:
        direction_emoji = {"bullish": "📈", "bearish": "📉", "neutral": "➡️"}.get(
            market["market_direction"] or "neutral", "➡️"
        )
        lines.append(f"| 항목 | 내용 |")
        lines.append(f"|------|------|")
        lines.append(f"| 시장 방향 | {direction_emoji} {market['market_direction']} (점수: {market['market_score']:.2f}) |")
        frgn = market["foreign_net_buy_bn"] or 0
        inst = market["institutional_net_buy_bn"] or 0
        lines.append(f"| 외국인 수급 | {'순매수' if frgn >= 0 else '순매도'} {abs(frgn):.0f}억원 |")
        lines.append(f"| 기관 수급 | {'순매수' if inst >= 0 else '순매도'} {abs(inst):.0f}억원 |")
        adv = market["advancing_stocks"] or 0
        dec = market["declining_stocks"] or 0
        lines.append(f"| 시장 폭 | 상승 {adv}종목 / 하락 {dec}종목 |")
        if market["summary"]:
            lines.append(f"| 요약 | {market['summary']} |")
    else:
        lines.append("_시황 데이터 없음_")
    lines.append("")

    # ── 종목별 상세 ──
    h(2, "📋 종목별 매매 상세")

    for tk, d in ticker_map.items():
        ticker_gross = sum((r["pnl"] or 0) for r in d["sells"] if r["pnl"] is not None)
        ticker_buy_amt  = sum(r["price"] * r["qty"] for r in d["buys"])
        ticker_sell_amt = sum(r["price"] * r["qty"] for r in d["sells"])
        ticker_fees = _calc_fee(ticker_buy_amt, ticker_sell_amt)
        ticker_net  = ticker_gross - ticker_fees["total"]

        status = "✅" if ticker_gross > 0 else "❌" if ticker_gross < 0 else "⏳"
        h(3, f"{status} {tk} {d['name']}")

        lines.append(f"- **섹터**: {d['sector']}")
        lines.append(f"- **진입 신호**: {d['signal_type']}")
        if d["rsi"]:
            lines.append(f"- **진입 지표**: RSI {d['rsi']:.0f} | 모멘텀 {d['momentum_score'] or 0:.0f}점 | 진입점수 {d['entry_score'] or 0:.0f}점 | RS {d['rs_daily'] or 0:+.1f}%")
        lines.append("")

        # 매수 내역
        if d["buys"]:
            lines.append("**매수 내역**")
            lines.append("| 시각 | 단가 | 수량 | 금액 |")
            lines.append("|------|------|------|------|")
            for b in d["buys"]:
                amt = b["price"] * b["qty"]
                row(b["time"][11:], f"{b['price']:,}원", f"{b['qty']}주", f"{amt:,.0f}원")
        lines.append("")

        # 매도 내역
        if d["sells"]:
            lines.append("**매도 내역**")
            lines.append("| 시각 | 단가 | 수량 | 손익(세전) | 수익률 |")
            lines.append("|------|------|------|-----------|--------|")
            for s in d["sells"]:
                pnl_str = _fmt_won(s["pnl"]) if s["pnl"] is not None else "-"
                pct_str = _fmt_pct(s["pnl_pct"]) if s["pnl_pct"] is not None else "-"
                row(s["time"][11:], f"{s['price']:,}원", f"{s['qty']}주", pnl_str, pct_str)
        lines.append("")

        # 종목 수익 요약
        lines.append(f"| 구분 | 금액 |")
        lines.append(f"|------|------|")
        lines.append(f"| 총 수익 (세전) | {_fmt_won(ticker_gross)} |")
        lines.append(f"| 수수료+세금 | -{ticker_fees['total']:,}원 |")
        lines.append(f"| **실현 순수익** | **{_fmt_won(ticker_net)}** |")
        lines.append("")

    # ── 수수료 명세 ──
    h(2, "💸 수수료 명세")
    lines.append("| 항목 | 계산 | 금액 |")
    lines.append("|------|------|------|")
    lines.append(f"| 매수 수수료 | {total_buy_amt:,.0f}원 × 0.015% | {fees['buy_fee']:,}원 |")
    lines.append(f"| 매도 수수료 | {total_sell_amt:,.0f}원 × 0.015% | {fees['sell_fee']:,}원 |")
    lines.append(f"| 증권거래세 | {total_sell_amt:,.0f}원 × 0.18% | {fees['tax']:,}원 |")
    lines.append(f"| **합계** | | **{fees['total']:,}원** |")
    lines.append("")
    lines.append("> KIS 실전 온라인 기준: 매수/매도 각 0.015%, 거래세 0.18% (코스피/코스닥 공통)")
    lines.append("")

    # ── 성과 분석 ──
    h(2, "📐 성과 분석")
    if signal_analytics:
        h(3, "신호 유형별 성과")
        lines.append("| 신호 | 거래 | 승률 | 평균 손익 |")
        lines.append("|------|------|------|----------|")
        for sig, stat in signal_analytics.items():
            lines.append(f"| {sig} | {stat.get('count', 0)}건 | {stat.get('win_rate', 0):.0f}% | {_fmt_pct(stat.get('avg_pnl_pct', 0))} |")
        lines.append("")

    # ── 복기 (Claude 분석) ──
    h(2, "🔬 복기")
    if summary_text:
        lines.append(f"> {summary_text}")
        lines.append("")

    if pattern_hits:
        h(3, "✅ 잘 된 것")
        for item in pattern_hits:
            lines.append(f"- {item}")
        lines.append("")

    if pattern_fails:
        h(3, "⚠️ 아쉬운 것")
        for item in pattern_fails:
            lines.append(f"- {item}")
        lines.append("")

    if improvements:
        h(3, "🔧 내일 개선 포인트")
        for item in improvements:
            lines.append(f"- {item}")
        lines.append("")

    # ── 파일에 추가 (오늘 섹션이 이미 있으면 교체, 없으면 맨 위에 삽입) ──
    new_section = "\n".join(lines) + "\n\n---\n"

    if out_path.exists():
        existing = out_path.read_text(encoding="utf-8")
        # 파일 첫 줄이 헤더(# 매매 일지)면 유지, 아니면 없는 것
        marker = f"## {today}"
        if marker in existing:
            # 기존 날짜 섹션을 새 내용으로 교체
            import re as _re
            pattern = rf"(## {_re.escape(today)}.*?)(?=\n## |\Z)"
            existing = _re.sub(pattern, new_section.rstrip("\n-").strip(), existing, flags=_re.DOTALL)
            out_path.write_text(existing, encoding="utf-8")
        else:
            # 파일 맨 위에 오늘 섹션 삽입
            header = "# 매매 일지 (DQT)\n\n"
            body = existing
            if existing.startswith("# 매매 일지"):
                # 기존 헤더 다음에 삽입
                idx = existing.index("\n\n") + 2 if "\n\n" in existing else len(header)
                body = existing[idx:]
            out_path.write_text(header + new_section + body, encoding="utf-8")
    else:
        # 파일 최초 생성
        out_path.write_text("# 매매 일지 (DQT)\n\n" + new_section, encoding="utf-8")

    logger.info(f"매매 일지 업데이트: {out_path}")
    return out_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="매매 일지 생성")
    parser.add_argument("--date", help="날짜 (YYYY-MM-DD, 기본: 오늘)")
    args = parser.parse_args()
    path = generate(args.date)
    print(f"생성 완료: {path}")
