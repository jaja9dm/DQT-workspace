"""
generate_trading_journal.py — 일일 매매일지 생성기

장 마감 후 DB 데이터를 읽어 docs/trading_journal/YYYY-MM-DD.md 파일을 생성한다.

사용법:
    python scripts/generate_trading_journal.py            # 오늘 날짜
    python scripts/generate_trading_journal.py 2026-05-04 # 특정 날짜

수수료 기준 (한국투자증권 온라인/MTS):
    - 매매 수수료: 0.014996% (매수/매도 각각)
    - 증권거래세:  0.20%     (매도 시 부과, 2026년 기준)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.infra.database import fetch_all, fetch_one

# ── 수수료 상수 ──────────────────────────────────────────────────
_FEE_RATE       = 0.00014996   # 매수/매도 각 0.014996%
_TRANS_TAX_RATE = 0.0020       # 증권거래세 0.20% (매도 시)

OUTPUT_DIR = ROOT / "docs" / "trading_journal"


# ─────────────────────────────────────────────────────────────────
# 수수료 계산
# ─────────────────────────────────────────────────────────────────

def _calc_fees(buy_amt: float, sell_amt: float) -> dict:
    """매수/매도 금액 기준 수수료·거래세·총비용 산출."""
    buy_fee   = round(buy_amt  * _FEE_RATE, 0)
    sell_fee  = round(sell_amt * _FEE_RATE, 0)
    trans_tax = round(sell_amt * _TRANS_TAX_RATE, 0)
    total     = buy_fee + sell_fee + trans_tax
    return {
        "buy_fee":   buy_fee,
        "sell_fee":  sell_fee,
        "trans_tax": trans_tax,
        "total":     total,
    }


# ─────────────────────────────────────────────────────────────────
# DB 데이터 로드
# ─────────────────────────────────────────────────────────────────

def _load_trades(trade_date: str) -> list[dict]:
    rows = fetch_all(
        "SELECT * FROM trades WHERE date=? ORDER BY created_at",
        (trade_date,),
    )
    return [dict(r) for r in rows]


def _load_slots(trade_date: str) -> list[dict]:
    rows = fetch_all(
        "SELECT * FROM slot_assignments WHERE trade_date=? ORDER BY assigned_at",
        (trade_date,),
    )
    return [dict(r) for r in rows]


def _load_global_condition(trade_date: str) -> dict:
    row = fetch_one(
        "SELECT * FROM global_condition WHERE date(created_at)=? ORDER BY created_at DESC LIMIT 1",
        (trade_date,),
    )
    return dict(row) if row else {}


def _load_market_condition(trade_date: str) -> dict:
    row = fetch_one(
        "SELECT * FROM market_condition WHERE date(created_at)=? ORDER BY created_at DESC LIMIT 1",
        (trade_date,),
    )
    return dict(row) if row else {}


def _load_review(trade_date: str) -> dict:
    row = fetch_one(
        "SELECT * FROM trade_review WHERE review_date=? ORDER BY created_at DESC LIMIT 1",
        (trade_date,),
    )
    return dict(row) if row else {}


def _load_cash_snapshot(trade_date: str) -> dict:
    """position_snapshot에서 당일 예수금 조회."""
    row = fetch_one(
        "SELECT * FROM position_snapshot WHERE date(saved_at)=? ORDER BY saved_at ASC LIMIT 1",
        (trade_date,),
    )
    start = dict(row) if row else {}
    row2 = fetch_one(
        "SELECT * FROM position_snapshot WHERE date(saved_at)=? ORDER BY saved_at DESC LIMIT 1",
        (trade_date,),
    )
    end = dict(row2) if row2 else {}
    return {"start": start, "end": end}


# ─────────────────────────────────────────────────────────────────
# 섹션 빌더
# ─────────────────────────────────────────────────────────────────

def _fmt_krw(v: float | None) -> str:
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.0f}원"


def _pct_str(v: float | None) -> str:
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"


def _weekday_ko(dt: date) -> str:
    return ["월", "화", "수", "목", "금", "토", "일"][dt.weekday()]


def _build_header(dt: date) -> str:
    return (
        f"# 📈 DQT 매매일지 — {dt.year}년 {dt.month}월 {dt.day}일 ({_weekday_ko(dt)}요일)\n\n"
        f"> 생성: {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  계좌: 한국투자증권 {os.getenv('KIS_ACCOUNT_NO','—')}\n"
    )


def _build_market_section(gc: dict, mc: dict) -> str:
    lines = ["## 📊 시장 개요\n"]

    # 국내 지수
    summary_raw = mc.get("summary") or "{}"
    try:
        summary = json.loads(summary_raw) if isinstance(summary_raw, str) else summary_raw
    except Exception:
        summary = {}

    kospi  = summary.get("kospi",  "—")
    kosdaq = summary.get("kosdaq", "—")
    lines.append("### 국내 지수")
    lines.append("| 지수 | 등락률 |")
    lines.append("|------|--------|")
    lines.append(f"| KOSPI  | {_pct_str(kospi) if isinstance(kospi, float) else kospi} |")
    lines.append(f"| KOSDAQ | {_pct_str(kosdaq) if isinstance(kosdaq, float) else kosdaq} |")
    lines.append("")

    # 글로벌 시황
    if gc:
        vix       = gc.get("vix", 0)
        vix_label = "낮음(<18)" if vix < 18 else "주의(18~25)" if vix < 25 else "경계(25~30)" if vix < 30 else "위험(30↑)"
        outlook   = gc.get("korea_market_outlook", "—")
        risk      = gc.get("global_risk_score", "—")

        lines.append("### 글로벌 시황")
        lines.append("| 지표 | 값 |")
        lines.append("|------|----|")
        lines.append(f"| S&P 500 | {_pct_str(gc.get('sp500_change'))} |")
        lines.append(f"| NASDAQ  | {_pct_str(gc.get('nasdaq_change'))} |")
        lines.append(f"| VIX     | {vix:.2f} ({vix_label}) |")
        lines.append(f"| USD/KRW | {gc.get('usd_krw', '—'):,.2f}원 |")
        lines.append(f"| WTI 원유 | ${gc.get('wti_oil', '—'):.2f}/배럴 |")
        lines.append(f"| 미국 10Y 국채 | {gc.get('us_10y_yield', '—'):.3f}% |")
        lines.append(f"| 글로벌 리스크 점수 | {risk}/10 |")
        lines.append(f"| 한국 시장 전망 | **{outlook}** |")
        lines.append("")

        key_events_raw = gc.get("key_events") or "[]"
        try:
            events = json.loads(key_events_raw) if isinstance(key_events_raw, str) else key_events_raw
        except Exception:
            events = []
        if events:
            lines.append("**주요 리스크 요인:**")
            for e in events:
                lines.append(f"- {e}")
            lines.append("")

    # 국내 시황 분석
    if summary:
        analysis = summary.get("analysis", "")
        if analysis:
            lines.append(f"**국내 시황:** {analysis}\n")
        key_reasons = summary.get("key_reasons", [])
        if key_reasons:
            for r in key_reasons:
                lines.append(f"- {r}")
            lines.append("")

    return "\n".join(lines)


def _build_cash_section(cash: dict, total_pnl_net: float) -> str:
    lines = ["## 💰 예수금\n"]
    start_snap = cash.get("start", {})
    end_snap   = cash.get("end",   {})

    start_cash = start_snap.get("cash_balance") or start_snap.get("available_cash")
    end_cash   = end_snap.get("cash_balance")   or end_snap.get("available_cash")

    lines.append("| 항목 | 금액 |")
    lines.append("|------|------|")
    lines.append(f"| 장 시작 예수금 | {_fmt_krw(start_cash)} |")
    lines.append(f"| 장 마감 예수금 | {_fmt_krw(end_cash)} |")
    lines.append(f"| 당일 순손익 (수수료 포함) | {_fmt_krw(total_pnl_net)} |")
    lines.append("")
    return "\n".join(lines)


def _build_slots_section(slots: list[dict]) -> str:
    if not slots:
        return "## 🎯 슬롯 배정\n\n당일 슬롯 배정 없음\n\n"

    lines = ["## 🎯 슬롯 배정\n"]
    lines.append("| 슬롯 | 종목 | 선정 이유 | 상태 |")
    lines.append("|------|------|---------|------|")
    status_ko = {"active": "보유 중", "exited": "청산 완료", "stopped": "손절 청산", "pending": "대기"}
    for s in slots:
        slot   = s.get("slot", "—")
        ticker = s.get("ticker", "—")
        name   = s.get("name", "—")
        reason = s.get("reason", "—")
        status = status_ko.get(s.get("status", ""), s.get("status", "—"))
        lines.append(f"| {slot} | {name}({ticker}) | {reason} | {status} |")
    lines.append("")
    return "\n".join(lines)


def _build_trades_section(trades: list[dict]) -> tuple[str, float, float, float]:
    """
    매매 내역 섹션 생성.
    Returns: (markdown_text, total_gross_pnl, total_fees, total_net_pnl)
    """
    if not trades:
        return "## 🔄 매매 내역\n\n당일 매매 없음\n\n", 0.0, 0.0, 0.0

    # 종목별 그룹핑
    by_ticker: dict[str, list[dict]] = {}
    for t in trades:
        tk = t["ticker"]
        by_ticker.setdefault(tk, []).append(t)

    lines = ["## 🔄 매매 내역\n"]
    total_gross = 0.0
    total_fees  = 0.0
    total_buy   = 0.0
    total_sell  = 0.0

    for ticker, ticker_trades in by_ticker.items():
        name = ticker_trades[0].get("name", ticker)
        lines.append(f"### {name} ({ticker})\n")

        lines.append("| 구분 | 시각(KST) | 수량 | 단가 | 체결금액 | 트랜셰 | 비고 |")
        lines.append("|------|-----------|------|------|---------|--------|------|")

        buy_amt  = 0.0
        sell_amt = 0.0
        gross    = 0.0
        buy_reasons  = []
        sell_reasons = []

        for t in ticker_trades:
            action    = t.get("action", "")
            qty       = t.get("quantity") or 0
            price     = t.get("exec_price") or t.get("order_price") or 0.0
            amt       = qty * price
            tranche   = t.get("tranche") or "—"
            src       = t.get("signal_source", "—")
            created   = t.get("created_at", "")
            time_str  = created[11:16] if len(created) >= 16 else "—"

            action_ko = {"buy": "📈 매수", "sell": "📉 매도", "stop_loss": "🛑 손절"}.get(action, action)
            note = ""
            if action in ("sell", "stop_loss"):
                pnl = t.get("pnl")
                pnl_pct = t.get("pnl_pct")
                if pnl is not None:
                    note = f"손익: {_fmt_krw(pnl)} ({_pct_str(pnl_pct)})"

            lines.append(
                f"| {action_ko} | {time_str} | {qty:,}주 | {price:,.0f}원 "
                f"| {amt:,.0f}원 | {tranche} | {note} |"
            )

            if action == "buy":
                buy_amt += amt
                buy_reasons.append(src)
            else:
                sell_amt += amt
                pnl_v = t.get("pnl") or 0.0
                gross += pnl_v
                sell_reasons.append(f"{action_ko}({src})")

        total_buy  += buy_amt
        total_sell += sell_amt

        fees = _calc_fees(buy_amt, sell_amt)
        net  = gross - fees["total"]
        total_gross += gross
        total_fees  += fees["total"]

        lines.append("")
        if buy_reasons:
            lines.append(f"**매수 근거:** {', '.join(dict.fromkeys(buy_reasons))}")
        if sell_reasons:
            lines.append(f"**매도 사유:** {', '.join(dict.fromkeys(sell_reasons))}")
        lines.append("")
        lines.append("| 항목 | 금액 |")
        lines.append("|------|------|")
        lines.append(f"| 총 매수금액 | {buy_amt:,.0f}원 |")
        lines.append(f"| 총 매도금액 | {sell_amt:,.0f}원 |")
        lines.append(f"| 세전 손익 | {_fmt_krw(gross)} |")
        lines.append(f"| 매수 수수료 (0.015%) | -{fees['buy_fee']:,.0f}원 |")
        lines.append(f"| 매도 수수료 (0.015%) | -{fees['sell_fee']:,.0f}원 |")
        lines.append(f"| 증권거래세 (0.20%)   | -{fees['trans_tax']:,.0f}원 |")
        lines.append(f"| **세후 순손익** | **{_fmt_krw(net)}** |")
        lines.append("")

    return "\n".join(lines), total_gross, total_fees, total_gross - total_fees


def _build_summary_section(
    trades: list[dict],
    total_gross: float,
    total_fees: float,
    start_cash: float | None,
) -> str:
    buys  = [t for t in trades if t.get("action") == "buy"]
    sells = [t for t in trades if t.get("action") in ("sell", "stop_loss")]
    wins  = [t for t in sells if (t.get("pnl") or 0) > 0]
    losses = [t for t in sells if (t.get("pnl") or 0) <= 0]

    total_net = total_gross - total_fees
    win_rate  = len(wins) / len(sells) * 100 if sells else 0.0
    roi       = total_net / start_cash * 100 if start_cash and start_cash > 0 else None

    lines = ["## 📊 수익 요약\n"]
    lines.append("| 항목 | 값 |")
    lines.append("|------|----|")
    lines.append(f"| 총 매수 건수 | {len(buys)}건 |")
    lines.append(f"| 총 매도 건수 | {len(sells)}건 (익절 {len(wins)} / 손절 {len(losses)}) |")
    lines.append(f"| 승률 | {win_rate:.0f}% |")
    lines.append(f"| 세전 총 손익 | {_fmt_krw(total_gross)} |")
    lines.append(f"| 총 수수료+세금 | -{total_fees:,.0f}원 |")
    lines.append(f"| **세후 순손익** | **{_fmt_krw(total_net)}** |")
    if roi is not None:
        lines.append(f"| 계좌 수익률 | {_pct_str(roi)} |")
    lines.append("")
    return "\n".join(lines)


def _build_review_section(review: dict) -> str:
    if not review:
        return "## 💬 피드백\n\n복기 데이터 없음 (16:30 이후 생성됩니다)\n\n"

    lines = ["## 💬 피드백\n"]

    summary = review.get("summary", "")
    if summary:
        lines.append(f"**총평:** {summary}\n")

    pattern_hits = review.get("pattern_hits") or []
    if isinstance(pattern_hits, str):
        try:
            pattern_hits = json.loads(pattern_hits)
        except Exception:
            pattern_hits = []

    pattern_fails = review.get("pattern_fails") or []
    if isinstance(pattern_fails, str):
        try:
            pattern_fails = json.loads(pattern_fails)
        except Exception:
            pattern_fails = []

    improvements = review.get("improvements") or []
    if isinstance(improvements, str):
        try:
            improvements = json.loads(improvements)
        except Exception:
            improvements = []

    if pattern_hits:
        lines.append("### ✅ 잘한 점")
        for h in pattern_hits:
            lines.append(f"- {h}")
        lines.append("")

    if pattern_fails:
        lines.append("### ❌ 아쉬운 점")
        for f in pattern_fails:
            lines.append(f"- {f}")
        lines.append("")

    if improvements:
        lines.append("### 🔧 개선 사항")
        for i in improvements:
            lines.append(f"- {i}")
        lines.append("")

    # 시그널 분석
    sa_raw = review.get("signal_analytics")
    if sa_raw:
        try:
            sa = json.loads(sa_raw) if isinstance(sa_raw, str) else sa_raw
            by_sig = sa.get("by_signal_type", {})
            if by_sig:
                lines.append("### 📈 시그널 유형별 성과")
                lines.append("| 시그널 | 매매수 | 승/패 | 평균손익 |")
                lines.append("|--------|--------|-------|---------|")
                for sig, stat in by_sig.items():
                    cnt = stat.get("count", 0)
                    w   = stat.get("win", 0)
                    l   = stat.get("loss", 0)
                    avg = stat.get("avg_pnl", 0)
                    lines.append(f"| {sig} | {cnt} | {w}승/{l}패 | {_pct_str(avg)} |")
                lines.append("")
        except Exception:
            pass

    return "\n".join(lines)


def _build_memo_section() -> str:
    return "## 📝 메모\n\n_(장 중 특이사항 또는 수동 메모)_\n\n"


# ─────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────

def generate(trade_date: str | None = None) -> Path:
    """
    매매일지 생성.
    trade_date: 'YYYY-MM-DD' 형식. None이면 오늘.
    Returns: 저장된 파일 경로
    """
    if trade_date is None:
        trade_date = date.today().isoformat()

    dt = date.fromisoformat(trade_date)

    # 데이터 로드
    trades  = _load_trades(trade_date)
    slots   = _load_slots(trade_date)
    gc      = _load_global_condition(trade_date)
    mc      = _load_market_condition(trade_date)
    review  = _load_review(trade_date)
    cash    = _load_cash_snapshot(trade_date)

    start_snap = cash.get("start", {})
    start_cash = (
        start_snap.get("cash_balance")
        or start_snap.get("available_cash")
    )

    # 섹션 생성
    trades_md, gross, fees, net = _build_trades_section(trades)

    sections = [
        _build_header(dt),
        _build_market_section(gc, mc),
        _build_cash_section(cash, net),
        _build_slots_section(slots),
        trades_md,
        _build_summary_section(trades, gross, fees, start_cash),
        _build_review_section(review),
        _build_memo_section(),
    ]

    content = "\n".join(sections)

    # 파일 저장
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{trade_date}.md"
    out_path.write_text(content, encoding="utf-8")
    print(f"매매일지 저장: {out_path}")
    return out_path


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    generate(arg)
