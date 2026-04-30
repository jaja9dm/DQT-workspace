"""
generate_trading_journal.py — 일일 매매 일지 자동 생성

장 마감 후 16:05에 스케줄러가 자동 실행.
해당 날짜에 거래가 없으면 생성하지 않음.
journal.md 단일 파일에 날짜별 섹션으로 누적 (최신 날짜가 맨 위).
생성 완료 시 텔레그램으로 해당 날짜 일지 전송.

수수료 계산 기준 (KIS 실전 온라인):
  - 매수 수수료: 0.015%
  - 매도 수수료: 0.015%
  - 증권거래세: 0.18% (매도 시)
  - 왕복 총비용: 약 0.21%
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date, datetime
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from src.infra.database import fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_BUY_FEE_RATE  = 0.00015
_SELL_FEE_RATE = 0.00015
_TAX_RATE      = 0.0018

_JOURNAL_DIR  = _ROOT / "docs" / "trading_journal"
_JOURNAL_FILE = _JOURNAL_DIR / "journal.md"

_SLOT_EMOJI = {
    "leader":   "👑",
    "breakout": "🚀",
    "pullback": "🔄",
}
_SLOT_LABEL = {
    "leader":   "주도주",
    "breakout": "돌파매매",
    "pullback": "눌림목",
}


def _calc_fee(buy_amt: float, sell_amt: float) -> dict:
    buy_fee  = round(buy_amt  * _BUY_FEE_RATE)
    sell_fee = round(sell_amt * _SELL_FEE_RATE)
    tax      = round(sell_amt * _TAX_RATE)
    return {"buy_fee": buy_fee, "sell_fee": sell_fee, "tax": tax, "total": buy_fee + sell_fee + tax}


def _fw(v: float | int) -> str:
    return f"{'+'if v>0 else ''}{v:,.0f}원"

def _fp(v: float) -> str:
    return f"{'+'if v>0 else ''}{v:.2f}%"


def generate(target_date: str | None = None) -> Path | None:
    """
    지정 날짜 매매 일지 생성. 거래 없으면 None 반환.
    journal.md 맨 위에 섹션 삽입 (기존 섹션이면 교체).
    생성 완료 시 텔레그램 발송.
    """
    today = target_date or date.today().isoformat()
    _JOURNAL_DIR.mkdir(parents=True, exist_ok=True)

    # ── 거래 이력 조회 ────────────────────────────────
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

    # 거래 없는 날은 생성 안 함
    if not trades:
        logger.info(f"매매 일지 생성 스킵 — {today} 거래 없음")
        return None

    # ── 슬롯 정보 조회 ───────────────────────────────
    slot_rows = fetch_all(
        "SELECT ticker, slot, signal_type FROM slot_assignments WHERE trade_date = ?",
        (today,),
    )
    slot_map: dict[str, str] = {r["ticker"]: r["slot"] for r in slot_rows if r["slot"]}

    # ── 시황 조회 ─────────────────────────────────────
    market = fetch_one(
        """
        SELECT market_score, market_direction, foreign_net_buy_bn,
               institutional_net_buy_bn, advancing_stocks, declining_stocks, summary
        FROM market_condition WHERE DATE(created_at)=? ORDER BY created_at DESC LIMIT 1
        """,
        (today,),
    )

    # ── 복기 리포트 조회 ──────────────────────────────
    review = fetch_one("SELECT * FROM trade_review WHERE review_date=?", (today,))

    # ── 예수금 ────────────────────────────────────────
    cash_info: dict = {}
    if review and review["market_context"]:
        try:
            cash_info = json.loads(review["market_context"]).get("portfolio", {})
        except Exception:
            pass

    # ── 종목별 집계 ───────────────────────────────────
    ticker_map: dict[str, dict] = {}
    for t in trades:
        tk = t["ticker"]
        if tk not in ticker_map:
            ticker_map[tk] = {
                "name":           t["name"] or tk,
                "sector":         t["sector"] or "-",
                "signal_type":    t["signal_type"] or t["signal_source"] or "-",
                "slot":           slot_map.get(tk, ""),
                "buys":           [],
                "sells":          [],
                "rsi":            t["rsi"],
                "entry_score":    t["entry_score"],
                "momentum_score": t["momentum_score"],
                "rs_daily":       t["rs_daily"],
            }
        rec = {
            "price":   t["exec_price"] or 0,
            "qty":     t["quantity"] or 0,
            "time":    (t["created_at"] or "")[:16],
            "pnl":     t["pnl"],
            "pnl_pct": t["pnl_pct"],
        }
        if t["action"] == "buy":
            ticker_map[tk]["buys"].append(rec)
        else:
            ticker_map[tk]["sells"].append(rec)

    # ── 수익 집계 ─────────────────────────────────────
    total_buy_amt  = sum(r["price"]*r["qty"] for d in ticker_map.values() for r in d["buys"])
    total_sell_amt = sum(r["price"]*r["qty"] for d in ticker_map.values() for r in d["sells"])
    fees           = _calc_fee(total_buy_amt, total_sell_amt)
    gross_pnl      = sum((r["pnl"] or 0) for d in ticker_map.values() for r in d["sells"] if r["pnl"] is not None)
    net_pnl        = gross_pnl - fees["total"]

    sell_cnt = sum(len(d["sells"]) for d in ticker_map.values())
    win_cnt  = sum(1 for d in ticker_map.values() for r in d["sells"] if (r["pnl"] or 0) > 0)
    loss_cnt = sell_cnt - win_cnt
    win_rate = (win_cnt / sell_cnt * 100) if sell_cnt > 0 else 0.0

    # ── 복기 파싱 ─────────────────────────────────────
    def _plist(field: str) -> list[str]:
        if not review or not review[field]:
            return []
        v = review[field]
        try:
            return json.loads(v) if isinstance(v, str) else (v or [])
        except Exception:
            return [str(v)]

    pattern_hits  = _plist("pattern_hits")
    pattern_fails = _plist("pattern_fails")
    improvements  = _plist("improvements")
    summary_text  = (review["summary"] if review else "") or ""

    signal_analytics: dict = {}
    if review and review["signal_analytics"]:
        try:
            signal_analytics = json.loads(review["signal_analytics"])
        except Exception:
            pass

    # ── Markdown 생성 ─────────────────────────────────
    L: list[str] = []

    def h(level: int, text: str):
        L.extend(["", f"{'#'*level} {text}", ""])

    def trow(*cells):
        L.append("| " + " | ".join(str(c) for c in cells) + " |")

    # 날짜 섹션
    L.append(f"## {today}")
    L.append("")
    L.append(f"> 생성: {datetime.now().strftime('%Y-%m-%d %H:%M')} | KIS 실전투자")
    L.append("")

    # 당일 요약
    h(3, "📊 당일 요약")
    buy_cnt = sum(len(d["buys"]) for d in ticker_map.values())
    L.append("| 항목 | 값 |")
    L.append("|------|-----|")
    L.append(f"| 거래 종목 수 | {len(ticker_map)}종목 |")
    L.append(f"| 체결 건수 | 매수 {buy_cnt}건 / 매도 {sell_cnt}건 |")
    L.append(f"| 승률 | {win_rate:.0f}% ({win_cnt}승 {loss_cnt}패) |")
    L.append(f"| 총 매수금액 | {total_buy_amt:,.0f}원 |")
    L.append(f"| 총 매도금액 | {total_sell_amt:,.0f}원 |")
    L.append(f"| 총 수익 (세전) | {_fw(gross_pnl)} |")
    L.append(f"| 수수료+세금 | -{fees['total']:,}원 (매수 {fees['buy_fee']:,}원 + 매도 {fees['sell_fee']:,}원 + 거래세 {fees['tax']:,}원) |")
    L.append(f"| **실현 순수익** | **{_fw(net_pnl)}** |")
    if cash_info:
        L.append(f"| 예수금 (장마감) | {cash_info.get('available_cash', 0):,.0f}원 |")
        L.append(f"| 총 평가금액 | {cash_info.get('total_eval_amt', 0):,.0f}원 |")
    L.append("")

    # 시황
    h(3, "🌏 당일 시황")
    if market:
        dir_emoji = {"bullish": "📈", "bearish": "📉"}.get(market["market_direction"] or "", "➡️")
        L.append("| 항목 | 내용 |")
        L.append("|------|------|")
        L.append(f"| 시장 방향 | {dir_emoji} {market['market_direction']} (점수: {market['market_score']:.2f}) |")
        frgn = market["foreign_net_buy_bn"] or 0
        inst = market["institutional_net_buy_bn"] or 0
        L.append(f"| 외국인 수급 | {'순매수' if frgn>=0 else '순매도'} {abs(frgn):.0f}억원 |")
        L.append(f"| 기관 수급 | {'순매수' if inst>=0 else '순매도'} {abs(inst):.0f}억원 |")
        adv = market["advancing_stocks"] or 0
        dec = market["declining_stocks"] or 0
        L.append(f"| 시장 폭 | 상승 {adv}종목 / 하락 {dec}종목 |")
        if market["summary"]:
            L.append(f"| 요약 | {market['summary']} |")
    else:
        L.append("_시황 데이터 없음_")
    L.append("")

    # 종목별 상세
    h(3, "📋 종목별 매매 상세")

    for tk, d in ticker_map.items():
        t_gross    = sum((r["pnl"] or 0) for r in d["sells"] if r["pnl"] is not None)
        t_buy_amt  = sum(r["price"]*r["qty"] for r in d["buys"])
        t_sell_amt = sum(r["price"]*r["qty"] for r in d["sells"])
        t_fees     = _calc_fee(t_buy_amt, t_sell_amt)
        t_net      = t_gross - t_fees["total"]

        status = "✅" if t_gross > 0 else "❌" if t_gross < 0 else "⏳"
        slot   = d["slot"]
        slot_tag = f" {_SLOT_EMOJI.get(slot, '')} [{_SLOT_LABEL.get(slot, slot)}]" if slot else ""

        h(4, f"{status} {tk} {d['name']}{slot_tag}")

        L.append(f"- **섹터**: {d['sector']}")
        L.append(f"- **슬롯**: {_SLOT_EMOJI.get(slot,'')} {_SLOT_LABEL.get(slot, slot or '-')}")
        L.append(f"- **진입 신호**: {d['signal_type']}")
        if d["rsi"]:
            L.append(
                f"- **진입 지표**: RSI {d['rsi']:.0f}"
                f" | 모멘텀 {d['momentum_score'] or 0:.0f}점"
                f" | 진입점수 {d['entry_score'] or 0:.0f}점"
                f" | RS {d['rs_daily'] or 0:+.1f}%"
            )
        L.append("")

        if d["buys"]:
            L.append("**매수 내역**")
            L.append("| 시각 | 단가 | 수량 | 금액 |")
            L.append("|------|------|------|------|")
            for b in d["buys"]:
                trow(b["time"][11:], f"{b['price']:,}원", f"{b['qty']}주", f"{b['price']*b['qty']:,.0f}원")
            L.append("")

        if d["sells"]:
            L.append("**매도 내역**")
            L.append("| 시각 | 단가 | 수량 | 손익(세전) | 수익률 |")
            L.append("|------|------|------|-----------|--------|")
            for s in d["sells"]:
                trow(
                    s["time"][11:],
                    f"{s['price']:,}원",
                    f"{s['qty']}주",
                    _fw(s["pnl"]) if s["pnl"] is not None else "-",
                    _fp(s["pnl_pct"]) if s["pnl_pct"] is not None else "-",
                )
            L.append("")

        L.append("| 구분 | 금액 |")
        L.append("|------|------|")
        L.append(f"| 총 수익 (세전) | {_fw(t_gross)} |")
        L.append(f"| 수수료+세금 | -{t_fees['total']:,}원 |")
        L.append(f"| **실현 순수익** | **{_fw(t_net)}** |")
        L.append("")

    # 수수료 명세
    h(3, "💸 수수료 명세")
    L.append("| 항목 | 계산 | 금액 |")
    L.append("|------|------|------|")
    L.append(f"| 매수 수수료 | {total_buy_amt:,.0f}원 × 0.015% | {fees['buy_fee']:,}원 |")
    L.append(f"| 매도 수수료 | {total_sell_amt:,.0f}원 × 0.015% | {fees['sell_fee']:,}원 |")
    L.append(f"| 증권거래세 | {total_sell_amt:,.0f}원 × 0.18% | {fees['tax']:,}원 |")
    L.append(f"| **합계** | | **{fees['total']:,}원** |")
    L.append("")
    L.append("> KIS 실전 온라인 기준: 매수/매도 각 0.015%, 거래세 0.18%")
    L.append("")

    # 성과 분석
    if signal_analytics:
        h(3, "📐 신호 유형별 성과")
        L.append("| 신호 | 거래 | 승률 | 평균 손익 |")
        L.append("|------|------|------|----------|")
        for sig, stat in signal_analytics.items():
            L.append(f"| {sig} | {stat.get('count',0)}건 | {stat.get('win_rate',0):.0f}% | {_fp(stat.get('avg_pnl_pct',0))} |")
        L.append("")

    # 복기
    h(3, "🔬 복기")
    if summary_text:
        L.append(f"> {summary_text}")
        L.append("")
    if pattern_hits:
        L.append("**✅ 잘 된 것**")
        for item in pattern_hits:
            L.append(f"- {item}")
        L.append("")
    if pattern_fails:
        L.append("**⚠️ 아쉬운 것**")
        for item in pattern_fails:
            L.append(f"- {item}")
        L.append("")
    if improvements:
        L.append("**🔧 내일 개선 포인트**")
        for item in improvements:
            L.append(f"- {item}")
        L.append("")

    # ── 파일 저장 ─────────────────────────────────────
    new_section = "\n".join(L) + "\n\n---\n\n"
    marker = f"## {today}"

    if _JOURNAL_FILE.exists():
        existing = _JOURNAL_FILE.read_text(encoding="utf-8")
        if marker in existing:
            pattern = rf"## {re.escape(today)}.*?(?=\n## |\Z)"
            existing = re.sub(pattern, new_section.rstrip("\n- "), existing, flags=re.DOTALL)
            _JOURNAL_FILE.write_text(existing, encoding="utf-8")
        else:
            header = "# 매매 일지 (DQT)\n\n"
            body = existing[existing.index("\n\n")+2:] if "\n\n" in existing and existing.startswith("# 매매") else existing
            _JOURNAL_FILE.write_text(header + new_section + body, encoding="utf-8")
    else:
        _JOURNAL_FILE.write_text("# 매매 일지 (DQT)\n\n" + new_section, encoding="utf-8")

    logger.info(f"매매 일지 업데이트: {_JOURNAL_FILE}")

    # ── 텔레그램 발송 ─────────────────────────────────
    _send_telegram(today, ticker_map, gross_pnl, net_pnl, fees, win_cnt, loss_cnt,
                   win_rate, total_buy_amt, cash_info, pattern_hits, pattern_fails, improvements)

    return _JOURNAL_FILE


def _send_telegram(
    today: str,
    ticker_map: dict,
    gross_pnl: float,
    net_pnl: float,
    fees: dict,
    win_cnt: int,
    loss_cnt: int,
    win_rate: float,
    total_buy_amt: float,
    cash_info: dict,
    pattern_hits: list,
    pattern_fails: list,
    improvements: list,
) -> None:
    """텔레그램으로 일지 요약 발송."""
    pnl_emoji = "🟢" if net_pnl >= 0 else "🔴"
    lines = [f"📒 <b>매매 일지 — {today}</b>"]
    lines.append("")
    lines.append(f"{pnl_emoji} 순수익: <b>{_fw(net_pnl)}</b>  (세전 {_fw(gross_pnl)} / 수수료 -{fees['total']:,}원)")
    lines.append(f"승률: {win_rate:.0f}%  ({win_cnt}승 {loss_cnt}패)")
    lines.append("")

    # 종목별 한 줄 요약
    lines.append("📋 종목별")
    for tk, d in ticker_map.items():
        t_gross = sum((r["pnl"] or 0) for r in d["sells"] if r["pnl"] is not None)
        t_fees  = _calc_fee(
            sum(r["price"]*r["qty"] for r in d["buys"]),
            sum(r["price"]*r["qty"] for r in d["sells"]),
        )
        t_net   = t_gross - t_fees["total"]
        slot    = d["slot"]
        slot_tag = f"{_SLOT_EMOJI.get(slot,'')} " if slot else ""
        result_emoji = "▲" if t_net > 0 else "▼" if t_net < 0 else "⏳"
        lines.append(f"  {result_emoji} {slot_tag}{d['name']}({tk})  {_fw(t_net)}")

    if cash_info.get("available_cash"):
        lines.append("")
        lines.append(f"💰 예수금: {cash_info['available_cash']:,.0f}원")

    if pattern_hits:
        lines.append("")
        lines.append("✅ 잘 된 것")
        for item in pattern_hits[:2]:
            lines.append(f"  • {item}")

    if pattern_fails:
        lines.append("")
        lines.append("⚠️ 아쉬운 것")
        for item in pattern_fails[:2]:
            lines.append(f"  • {item}")

    if improvements:
        lines.append("")
        lines.append("🔧 내일 개선")
        for item in improvements[:2]:
            lines.append(f"  • {item}")

    try:
        notify("\n".join(lines), parse_mode="HTML")
        logger.info("매매 일지 텔레그램 발송 완료")
    except Exception as e:
        logger.error(f"매매 일지 텔레그램 발송 실패: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="매매 일지 생성")
    parser.add_argument("--date", help="날짜 (YYYY-MM-DD, 기본: 오늘)")
    args = parser.parse_args()
    result = generate(args.date)
    if result:
        print(f"생성 완료: {result}")
    else:
        print("거래 없음 — 생성 스킵")
