"""
후성(093370) 2026-04-17(금) DQT 시뮬레이션 v2
시나리오 A: 구 로직 — AND buy_pre, OR sell_pre(부호 무관), Gate 4.5 없음, 즉시 전량 청산
시나리오 B: 신 로직 — OR buy_pre, AND sell_pre(양수 구간), Gate 4.5, 동적 부분 익절 + 연속 추적
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json

TICKER           = "093370"
DATE             = "2026-04-17"
CAPITAL          = 1_000_000
INITIAL_STOP_PCT = 2.0
TRIGGER_PCT      = 3.0
FLOOR_PCT        = 2.0
TRANCHE_RATIO    = 0.60
VOL_SURGE_RATIO  = 2.5

print("데이터 로딩...")
tk  = yf.Ticker(f"{TICKER}.KS")
raw = tk.history(start=DATE, end="2026-04-18", interval="1m")
raw = raw.reset_index()
raw["time"] = raw["Datetime"].dt.strftime("%H:%M")
raw = raw[["time", "Open", "High", "Low", "Close", "Volume"]].copy()
raw.columns = ["time", "open", "high", "low", "close", "volume"]
raw = raw.reset_index(drop=True)
rows_list = raw.to_dict("records")
print(f"  {len(rows_list)}봉 로드 완료 ({rows_list[0]['time']} ~ {rows_list[-1]['time']})")


# ── MACD 계산 ─────────────────────────────────────────────────────────────

def calc_hist(closes, fast=12, slow=26, sig=9):
    s = pd.Series(closes, dtype=float)
    ema_f  = s.ewm(span=fast, adjust=False).mean()
    ema_s  = s.ewm(span=slow, adjust=False).mean()
    macd   = ema_f - ema_s
    signal = macd.ewm(span=sig, adjust=False).mean()
    return (macd - signal).values


def aggregate(buf, period):
    out = []
    for i in range(period - 1, len(buf), period):
        s = max(0, i - period + 1)
        out.append({
            "close":  buf[i]["close"],
            "high":   max(r["high"]   for r in buf[s:i+1]),
            "low":    min(r["low"]    for r in buf[s:i+1]),
            "volume": sum(r["volume"] for r in buf[s:i+1]),
        })
    return out


def macd_signal_tf(candles_agg, n=2, strict_sell=False):
    """
    buy_pre  = 히스토그램 n봉 연속 상승 (모멘텀 회복 = 방향성 확인)
    sell_pre:
      strict_sell=False (구 로직): n봉 연속 하락이면 부호 무관 sell_pre
      strict_sell=True  (신 로직): n봉 연속 하락 + 마지막 히스토그램 > 0 (양수 구간에서만)
    """
    if len(candles_agg) < 6:
        return "hold", 0.0
    hist = calc_hist([c["close"] for c in candles_agg])
    if len(hist) < n + 1:
        return "hold", float(hist[-1]) if len(hist) > 0 else 0.0
    tail   = hist[-(n + 1):]
    diffs  = np.diff(tail)
    last   = tail[-1]
    rising  = bool(np.all(diffs > 0))
    falling = bool(np.all(diffs < 0))

    if rising:
        return "buy_pre", float(last)
    if falling and (not strict_sell or last > 0):
        return "sell_pre", float(last)
    return "hold", float(last)


def get_signals(buf, new_logic):
    """
    Returns: (combined_sig, s3, s5, h3, h5)
    new_logic:
      buy  = OR  (하나만 buy_pre여도 진입)    / old = AND (둘 다여야)
      sell = AND + positive  / old = OR (하나만 sell_pre여도 청산)
    """
    agg3 = aggregate(buf, 3)
    agg5 = aggregate(buf, 5)

    strict = new_logic
    s3, h3 = macd_signal_tf(agg3, strict_sell=strict)
    s5, h5 = macd_signal_tf(agg5, strict_sell=strict)

    if new_logic:
        # buy: 3m OR 5m
        if s3 == "buy_pre" or s5 == "buy_pre":
            csig = "buy_pre"
        # sell: 3m AND 5m (모두 양수 구간에서 하락)
        elif s3 == "sell_pre" and s5 == "sell_pre":
            csig = "sell_pre"
        else:
            csig = "hold"
    else:
        # buy: 3m AND 5m
        if s3 == "buy_pre" and s5 == "buy_pre":
            csig = "buy_pre"
        # sell: 3m OR 5m (하나만 하락해도)
        elif s3 == "sell_pre" or s5 == "sell_pre":
            csig = "sell_pre"
        else:
            csig = "hold"

    return csig, s3, s5, h3, h5


def vol_surge(buf):
    vols = [r["volume"] for r in buf]
    if len(vols) < 10:
        return 1.0
    recent = float(np.mean(vols[-5:]))
    base   = float(np.mean(vols[-25:-5])) if len(vols) >= 25 else float(np.mean(vols[:-5]) or 1)
    return recent / base if base > 0 else 1.0


# ── 시뮬레이션 엔진 ───────────────────────────────────────────────────────

def run_sim(new_logic: bool):
    events       = []
    price_series = []
    macd_series  = []

    pos_qty     = 0
    avg_price   = 0.0
    entry_price = 0.0
    stop_price  = 0.0
    peak_price  = 0.0
    trailing_on = False
    scalp_done  = False
    consec_sell = 0
    hist_prev   = 0.0
    capital     = CAPITAL
    realized    = 0.0
    ever_bought = False
    buf: list[dict] = []

    def do_buy(t, price, qty, reason, btype="BUY"):
        nonlocal capital, entry_price, stop_price, peak_price, avg_price, pos_qty, ever_bought
        if qty <= 0:
            return
        if pos_qty == 0:
            avg_price = entry_price = price
            stop_price  = round(price * (1 - INITIAL_STOP_PCT / 100))
            peak_price  = price
        else:
            avg_price  = (avg_price * pos_qty + price * qty) / (pos_qty + qty)
            stop_price = round(avg_price * (1 - INITIAL_STOP_PCT / 100))
        pos_qty    += qty
        capital    -= qty * price
        ever_bought = True
        events.append({
            "time": t, "type": btype, "price": price, "qty": qty,
            "reason": reason, "pnl": None, "pnl_pct": None,
            "avg": round(avg_price), "stop": stop_price,
        })

    def do_sell(t, price, qty, stype, reason):
        nonlocal capital, realized, pos_qty, avg_price, stop_price, peak_price
        nonlocal trailing_on, scalp_done, consec_sell, hist_prev
        pnl     = (price - avg_price) * qty
        pnl_pct = (price - avg_price) / avg_price * 100
        capital  += qty * price
        realized += pnl
        pos_qty  -= qty
        events.append({
            "time": t, "type": stype, "price": price, "qty": qty,
            "reason": reason, "pnl": round(pnl), "pnl_pct": round(pnl_pct, 2),
            "avg": round(avg_price) if pos_qty > 0 else None,
            "stop": stop_price if pos_qty > 0 else None,
        })
        if pos_qty <= 0:
            pos_qty = 0; avg_price = 0.0; stop_price = 0.0; peak_price = 0.0
            trailing_on = False; scalp_done = False
            consec_sell = 0; hist_prev = 0.0

    for i, row in enumerate(rows_list):
        t     = row["time"]
        close = float(row["close"])
        high  = float(row["high"])
        low   = float(row["low"])

        buf.append(row)
        if len(buf) > 30:
            buf = buf[-30:]

        sig, s3, s5, h3, h5 = get_signals(buf, new_logic)
        macd_series.append({
            "time": t, "h3": round(h3, 1), "h5": round(h5, 1),
            "sig": sig, "s3": s3, "s5": s5,
        })

        # 연속 sell_pre 추적 (포지션 보유 중)
        if pos_qty > 0:
            consec_sell = (consec_sell + 1) if sig == "sell_pre" else 0

        price_series.append({
            "time":  t, "close": close, "high": high, "low": low,
            "stop":  stop_price if pos_qty > 0 else None,
            "avg":   round(avg_price) if pos_qty > 0 else None,
            "peak":  round(peak_price) if trailing_on else None,
        })

        # ── 09:10 1차 매수 ─────────────────────────
        if t == "09:10" and pos_qty == 0 and not ever_bought:
            gate_ok = True
            if new_logic and sig == "sell_pre":
                gate_ok = False
                events.append({
                    "time": t, "type": "GATE_45_BLOCK", "price": close, "qty": 0,
                    "reason": "Gate 4.5: MACD sell_pre — 장초반 수급 이탈",
                    "pnl": None, "pnl_pct": None, "avg": None, "stop": None,
                })
            if gate_ok:
                qty = int((CAPITAL * TRANCHE_RATIO) // close)
                do_buy(t, close, qty, "1차 매수 (60%) — Opening Gate 해제", "BUY")
            continue

        # ── 포지션 없음: 재진입 대기 ───────────────
        if pos_qty == 0:
            if ever_bought and t >= "09:30":
                vs = vol_surge(buf)
                if sig == "buy_pre" and vs >= VOL_SURGE_RATIO:
                    qty = int((CAPITAL * TRANCHE_RATIO) // close)
                    do_buy(t, close, qty,
                           f"MACD 재진입 (vs {vs:.1f}배 / {s3}·{s5})", "BUY_REENTRY")
                    scalp_done = False; consec_sell = 0
            continue

        # ── 손절 체크 (low 기준) ───────────────────
        if low <= stop_price > 0:
            do_sell(t, stop_price, pos_qty, "SELL_STOP",
                    f"{'트레일링 ' if trailing_on else '초기 '}손절 (−{INITIAL_STOP_PCT:.0f}%)")
            continue

        # ── 트레일링 활성화 (high 기준) ───────────
        cur_hi_pnl = (high - entry_price) / entry_price * 100
        if not trailing_on and cur_hi_pnl >= TRIGGER_PCT:
            trailing_on = True
            peak_price  = high
            stop_price  = round(high * (1 - FLOOR_PCT / 100))
            events.append({
                "time": t, "type": "TRAIL_ACTIVATE", "price": round(high), "qty": 0,
                "reason": f"트레일링 활성 — 진입가 +{TRIGGER_PCT:.0f}% 돌파",
                "pnl": None, "pnl_pct": None,
                "avg": round(avg_price), "stop": stop_price,
            })

        # ── 고점 갱신 → 손절선 상향 ───────────────
        if trailing_on and high > peak_price:
            peak_price = high
            ns = round(high * (1 - FLOOR_PCT / 100))
            if ns > stop_price:
                stop_price = ns

        pnl_now = (close - avg_price) / avg_price * 100

        # ── 청산 로직 ──────────────────────────────
        if new_logic:
            # 동적 부분 익절: 소진도 기반
            if not scalp_done and trailing_on and pnl_now > 0:
                exhaustion = 0.0
                exh_tags   = []

                # MACD 소진 신호
                if sig == "sell_pre":
                    exhaustion += 0.45
                    exh_tags.append("MACD하강(3m+5m)")
                elif h3 < 0 and hist_prev >= 0:
                    exhaustion += 0.40
                    exh_tags.append("MACD음전환")
                elif h3 > 0 and hist_prev > 0 and h3 < hist_prev:
                    exhaustion += 0.25
                    exh_tags.append("MACD피크감소")

                # 연속 sell_pre 보너스 ("파란 바 누적")
                if consec_sell >= 2:
                    bonus = min(0.30, 0.12 * (consec_sell - 1))
                    exhaustion += bonus
                    exh_tags.append(f"연속sell_pre {consec_sell}회")

                # 수익률 자체 가산
                if pnl_now >= 7.0:
                    exhaustion += 0.20; exh_tags.append(f"+{pnl_now:.1f}% 고수익")
                elif pnl_now >= 4.5:
                    exhaustion += 0.10; exh_tags.append(f"+{pnl_now:.1f}% 수익")

                exhaustion = min(1.0, exhaustion)
                eff_thr    = max(0.70, 2.0 * max(0.40, 1.0 - exhaustion * 0.55))

                if exhaustion >= 0.20 and pnl_now >= eff_thr:
                    eff_ratio = min(0.80, max(0.30, 0.30 + exhaustion * 0.50))
                    sq        = max(1, int(pos_qty * eff_ratio))
                    scalp_done = True
                    do_sell(t, close, sq, "SELL_PARTIAL",
                            f"동적 부분익절 {eff_ratio*100:.0f}% | 소진도 {exhaustion:.2f} | "
                            + " · ".join(exh_tags))
                    if pos_qty == 0:
                        hist_prev = h3; continue

            # 연속 sell_pre 3회+: 파란 바 누적 → 잔여 전량 청산
            if scalp_done and consec_sell >= 3 and trailing_on and pnl_now > 0:
                do_sell(t, close, pos_qty, "SELL_FINAL",
                        f"파란 바 {consec_sell}연속 → 잔여 전량 청산 ({pnl_now:+.1f}%)")
                hist_prev = h3; continue

        else:
            # 구 로직: sell_pre (OR 조건) → 수익권 + 트레일링 → 즉시 전량 청산
            if sig == "sell_pre" and trailing_on and pnl_now > 1.0:
                do_sell(t, close, pos_qty, "SELL_MACD",
                        f"MACD 조기청산 (OR 조건) | {pnl_now:+.1f}%")
                hist_prev = h3; continue

        hist_prev = h3

        # ── 14:50 시간컷 ──────────────────────────
        if t == "14:50" and pos_qty > 0 and pnl_now > 0:
            do_sell(t, close, pos_qty, "SELL_TIMECUT", f"14:50 시간컷 ({pnl_now:+.1f}%)")
            continue

        # ── 14:59 장마감 강제 청산 ────────────────
        if t == "14:59" and pos_qty > 0:
            do_sell(t, close, pos_qty, "SELL_EOD", f"장마감 ({pnl_now:+.1f}%)")

    if pos_qty > 0:
        last = rows_list[-1]
        pnl_now = (float(last["close"]) - avg_price) / avg_price * 100
        do_sell(last["time"], float(last["close"]), pos_qty,
                "SELL_EOD", f"미청산 강제청산 ({pnl_now:+.1f}%)")

    return events, price_series, macd_series, realized, capital


# ── 실행 ─────────────────────────────────────────────────────────────────
evts_a, prices_a, macd_a, pnl_a, cap_a = run_sim(new_logic=False)
evts_b, prices_b, macd_b, pnl_b, cap_b = run_sim(new_logic=True)

ret_a = pnl_a / CAPITAL * 100
ret_b = pnl_b / CAPITAL * 100
diff  = pnl_b - pnl_a

print(f"\n시나리오 A (구 로직): {pnl_a:+,.0f}원 ({ret_a:+.2f}%)")
for e in evts_a:
    ps = f"  → {e['pnl']:+,.0f}원 ({e['pnl_pct']:+.2f}%)" if e.get("pnl") is not None else ""
    print(f"  [{e['time']}] {e['type']:20s} {e['price']:,.0f}원 ×{e['qty']}주  {e['reason']}{ps}")

print(f"\n시나리오 B (신 로직): {pnl_b:+,.0f}원 ({ret_b:+.2f}%)")
for e in evts_b:
    ps = f"  → {e['pnl']:+,.0f}원 ({e['pnl_pct']:+.2f}%)" if e.get("pnl") is not None else ""
    print(f"  [{e['time']}] {e['type']:20s} {e['price']:,.0f}원 ×{e['qty']}주  {e['reason']}{ps}")


# ── HTML ─────────────────────────────────────────────────────────────────

EVT = {
    "BUY":           ("1차 매수",     "#10b981", "#dcfce7"),
    "BUY_REENTRY":   ("재진입",       "#059669", "#d1fae5"),
    "TRAIL_ACTIVATE":("트레일링 활성","#8b5cf6", "#ede9fe"),
    "SELL_STOP":     ("손절",         "#ef4444", "#fee2e2"),
    "SELL_MACD":     ("MACD 즉시 청산","#f59e0b","#fef3c7"),
    "SELL_PARTIAL":  ("부분 익절",    "#3b82f6", "#dbeafe"),
    "SELL_FINAL":    ("파란바 전량",  "#1d4ed8", "#bfdbfe"),
    "SELL_TIMECUT":  ("시간컷",       "#64748b", "#f1f5f9"),
    "SELL_EOD":      ("장마감",       "#475569", "#f8fafc"),
    "GATE_45_BLOCK": ("Gate4.5 차단", "#dc2626", "#fef2f2"),
}


def badge(etype):
    lbl, color, _ = EVT.get(etype, (etype, "#6b7280", "#f9fafb"))
    return (f'<span style="background:{color};color:#fff;padding:2px 10px;'
            f'border-radius:12px;font-size:11px;font-weight:700;">{lbl}</span>')


def build_rows(evts):
    html = ""
    for e in evts:
        _, _, bg = EVT.get(e["type"], ("", "#6b7280", "#f9fafb"))
        pstr = ""
        if e.get("pnl") is not None:
            c = "#10b981" if e["pnl"] >= 0 else "#ef4444"
            pstr = (f'<span style="color:{c};font-weight:800;">'
                    f'{e["pnl"]:+,.0f}원&nbsp;({e["pnl_pct"]:+.2f}%)</span>')
        extra = ""
        if e.get("avg"):  extra += f' <span style="color:#64748b;font-size:11px">평단 {e["avg"]:,.0f}원</span>'
        if e.get("stop"): extra += f' <span style="color:#ef4444;font-size:11px">손절 {e["stop"]:,.0f}원</span>'
        html += f"""
        <tr style="background:{bg}">
          <td style="padding:8px 12px;font-weight:700;white-space:nowrap;color:#1e293b">{e['time']}</td>
          <td style="padding:8px 12px">{badge(e['type'])}</td>
          <td style="padding:8px 12px;font-weight:700;color:#1e293b">{e['price']:,.0f}원</td>
          <td style="padding:8px 12px;color:#374151">{e['qty'] if e['qty'] else '—'}</td>
          <td style="padding:8px 12px;color:#374151;font-size:12px">{e['reason']}{extra}</td>
          <td style="padding:8px 12px">{pstr}</td>
        </tr>"""
    return html


rows_a = build_rows(evts_a)
rows_b = build_rows(evts_b)
rc_a = "#10b981" if pnl_a >= 0 else "#ef4444"
rc_b = "#10b981" if pnl_b >= 0 else "#ef4444"
diff_pct = diff / CAPITAL * 100
diff_color = "#3b82f6" if diff >= 0 else "#ef4444"
diff_arrow = "▲" if diff >= 0 else "▼"


def algo_comparison():
    items = [
        ("buy_pre 조합", "3m AND 5m 모두 상승", "3m OR 5m 중 하나만 상승"),
        ("sell_pre 조합", "3m OR 5m 하강 (부호 무관)", "3m AND 5m 모두 양수 구간에서 하강"),
        ("Gate 4.5", "없음 — 조건 없이 진입", "sell_pre 차단 + 09:30 전 buy_pre 확인"),
        ("청산 방식", "즉시 전량 청산", "소진도 기반 동적 부분 익절"),
        ("연속 sell_pre 추적", "없음", "3사이클+ → 잔여 전량 청산"),
        ("재진입 buy 조건", "AND (더 엄격)", "OR (더 빠른 포착)"),
    ]
    rows = ""
    for item, old, new in items:
        rows += f"""<tr>
          <td style="padding:10px 14px;font-weight:600;color:#475569;font-size:13px">{item}</td>
          <td style="padding:10px 14px;color:#ef4444;font-size:13px">{old}</td>
          <td style="padding:10px 14px;color:#10b981;font-size:13px">{new}</td>
        </tr>"""
    return rows


def key_timeline():
    pts = [
        ("09:02", "시가 형성", "전일 대비 강세 갭업 시작", "#3b82f6"),
        ("09:10", "Opening Gate 해제", "DQT 1차 매수 집행 (60% 비중)", "#10b981"),
        ("09:15", "초기 손절 발동", "Low 하락 → 초기 손절선(−2%) 돌파", "#ef4444"),
        ("09:37", "거래량 급증", "평균 4배↑ 거래량 + MACD 반등 신호", "#f59e0b"),
        ("09:40~41", "1차 급등 피크", "거래량 절정, MACD 히스토그램 급격히 확대", "#8b5cf6"),
        ("10:04", "2차 급등", "13,630원 돌파 — 트레일링 손절선 상승", "#8b5cf6"),
        ("11:58", "3차 급등", "거래량 재급증 + 14,000원 대 돌파", "#8b5cf6"),
        ("12:01", "일중 최고가", "14,430원 — 시가 대비 +10.5%, 수급 절정", "#10b981"),
        ("13:05", "매도 압력 시작", "MACD 히스토그램 파란 바 출현 — 수급 이탈 시작", "#ef4444"),
        ("14:47", "장마감 안정", "13,940원대 회복 후 횡보", "#3b82f6"),
    ]
    html = ""
    for t, title, desc, color in pts:
        html += f"""
        <div style="display:flex;gap:16px;padding:10px 0;border-bottom:1px solid #f1f5f9">
          <div style="min-width:52px;font-weight:700;color:{color};font-size:13px">{t}</div>
          <div>
            <div style="font-weight:600;font-size:13px;color:#1e293b">{title}</div>
            <div style="font-size:12px;color:#64748b">{desc}</div>
          </div>
        </div>"""
    return html


html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>DQT 시뮬레이션 v2 — 후성(093370) 2026-04-17</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;
     background:#f1f5f9;color:#1e293b;line-height:1.6}}
.wrap{{max-width:1220px;margin:0 auto;padding:32px 16px}}
h1{{font-size:24px;font-weight:900;color:#0f172a}}
.meta{{color:#64748b;font-size:13px;margin-top:4px;margin-bottom:28px}}
.cmp{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-bottom:28px}}
.rbox{{border-radius:16px;padding:24px;text-align:center;border:2px solid;background:#fff}}
.ra{{border-color:#fca5a5}}
.rb{{border-color:#86efac}}
.rd{{border-color:#93c5fd}}
.r-title{{font-size:10px;color:#94a3b8;font-weight:700;letter-spacing:1px;
          text-transform:uppercase;margin-bottom:10px}}
.r-amt{{font-size:38px;font-weight:900;margin:4px 0}}
.r-pct{{font-size:20px;font-weight:700}}
.r-lbl{{font-size:11px;color:#94a3b8;margin-top:10px;line-height:1.5}}
.tabs{{display:flex;gap:6px;margin-bottom:20px;flex-wrap:wrap}}
.tab{{padding:8px 18px;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600;
      border:1px solid #cbd5e1;color:#64748b;background:#fff;transition:.15s}}
.tab.active{{background:#3b82f6;border-color:#3b82f6;color:#fff}}
.section{{display:none}}.section.active{{display:block}}
.card{{background:#fff;border-radius:12px;border:1px solid #e2e8f0;
       margin-bottom:20px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05)}}
.ch{{padding:14px 18px;border-bottom:1px solid #f1f5f9;font-weight:700;font-size:14px;color:#0f172a}}
.cb{{padding:18px}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{padding:9px 12px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.6px;
    color:#94a3b8;background:#f8fafc;border-bottom:2px solid #f1f5f9;white-space:nowrap}}
td{{border-bottom:1px solid #f8fafc;vertical-align:middle}}
tr:last-child td{{border-bottom:none}}
.g3{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px}}
.stat{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:14px;text-align:center}}
.stat .lbl{{font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.6px;font-weight:600}}
.stat .val{{font-size:22px;font-weight:800;color:#0f172a;margin-top:2px}}
.note{{background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:14px 18px;
       font-size:12px;color:#1d4ed8;line-height:1.8;margin-top:16px}}
.warn{{background:#fefce8;border:1px solid #fde047;border-radius:10px;padding:14px 18px;
       font-size:12px;color:#713f12;line-height:1.8;margin-top:12px}}
@media(max-width:680px){{.cmp,.g3,.g2{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="wrap">

<h1>🤖 DQT 시뮬레이션 v2 — 후성(093370)</h1>
<div class="meta">
  2026년 04월 17일 (금) &nbsp;·&nbsp; 1분봉 완전 재생 &nbsp;·&nbsp;
  투자 원금 {CAPITAL:,.0f}원 &nbsp;·&nbsp;
  <strong>구 로직 vs 신 로직 매매 비교</strong>
</div>

<div class="cmp">
  <div class="rbox ra">
    <div class="r-title">시나리오 A — 구 로직</div>
    <div class="r-amt" style="color:{rc_a}">{pnl_a:+,.0f}원</div>
    <div class="r-pct" style="color:{rc_a}">{ret_a:+.2f}%</div>
    <div class="r-lbl">OR sell_pre (즉시 전량 청산)<br>AND buy_pre · Gate 4.5 없음</div>
  </div>
  <div class="rbox rb">
    <div class="r-title">시나리오 B — 신 로직</div>
    <div class="r-amt" style="color:{rc_b}">{pnl_b:+,.0f}원</div>
    <div class="r-pct" style="color:{rc_b}">{ret_b:+.2f}%</div>
    <div class="r-lbl">AND sell_pre (동적 부분 익절)<br>OR buy_pre · Gate 4.5 · 연속 추적</div>
  </div>
  <div class="rbox rd">
    <div class="r-title">신 로직 개선폭</div>
    <div class="r-amt" style="color:{diff_color}">{diff_arrow} {abs(diff):,.0f}원</div>
    <div class="r-pct" style="color:{diff_color}">{diff_pct:+.2f}%p</div>
    <div class="r-lbl">{'신 로직이 더 유리한 결과' if diff >= 0 else '구 로직이 더 유리한 결과'}<br>(이 종목/날짜 기준)</div>
  </div>
</div>

<div class="tabs">
  <button class="tab active" onclick="sw(this,'tc')">📊 차트</button>
  <button class="tab" onclick="sw(this,'ta')">🅰 구 로직 상세</button>
  <button class="tab" onclick="sw(this,'tb')">🅱 신 로직 상세</button>
  <button class="tab" onclick="sw(this,'talgo')">⚡ 알고리즘 비교</button>
  <button class="tab" onclick="sw(this,'ttl')">🕐 타임라인</button>
  <button class="tab" onclick="sw(this,'tpar')">⚙ 파라미터</button>
</div>

<!-- 차트 -->
<div id="tc" class="section active">
  <div class="card">
    <div class="ch">📈 후성(093370) 1분봉 가격 차트 — 매매 포인트 표시</div>
    <div class="cb"><canvas id="cMain" height="100"></canvas></div>
  </div>
  <div class="card">
    <div class="ch">📊 MACD 히스토그램 (3분봉) &nbsp;
      <span style="font-size:11px;font-weight:400;color:#64748b">
        <span style="color:#10b981">■</span> buy_pre &nbsp;
        <span style="color:#ef4444">■</span> sell_pre &nbsp;
        <span style="color:#60a5fa">■</span> 양수/hold &nbsp;
        <span style="color:#94a3b8">■</span> 음수/hold
      </span>
    </div>
    <div class="cb"><canvas id="cMacd" height="70"></canvas></div>
  </div>
  <div class="g3">
    <div class="stat"><div class="lbl">시가</div><div class="val">13,070원</div></div>
    <div class="stat"><div class="lbl">종가</div><div class="val" style="color:#10b981">13,955원</div></div>
    <div class="stat"><div class="lbl">일중 고가</div><div class="val">14,430원</div></div>
    <div class="stat"><div class="lbl">일중 저가</div><div class="val" style="color:#ef4444">12,060원</div></div>
    <div class="stat"><div class="lbl">당일 등락률</div><div class="val" style="color:#10b981">+6.77%</div></div>
    <div class="stat"><div class="lbl">장중 최대 진폭</div><div class="val">+19.8%</div></div>
  </div>
</div>

<!-- 시나리오 A -->
<div id="ta" class="section">
  <div class="card">
    <div class="ch">🅰 구 로직 — OR sell_pre · AND buy_pre · 즉시 전량 청산</div>
    <table>
      <thead><tr><th>시각</th><th>유형</th><th>체결가</th><th>수량</th><th>사유</th><th>손익</th></tr></thead>
      <tbody>{rows_a}</tbody>
    </table>
  </div>
  <div class="note">
    💡 <strong>구 로직 특성:</strong>
    두 타임프레임 중 하나만 하강(OR)해도 즉시 전량 청산 → 단기 노이즈에도 포지션이 청산됩니다.
    buy_pre는 AND 조건(둘 다 상승)이어야 재진입 → 더 늦게 들어갑니다.
  </div>
</div>

<!-- 시나리오 B -->
<div id="tb" class="section">
  <div class="card">
    <div class="ch">🅱 신 로직 — AND sell_pre · OR buy_pre · 동적 부분 익절 · 연속 추적</div>
    <table>
      <thead><tr><th>시각</th><th>유형</th><th>체결가</th><th>수량</th><th>사유</th><th>손익</th></tr></thead>
      <tbody>{rows_b}</tbody>
    </table>
  </div>
  <div class="note">
    💡 <strong>신 로직 특성:</strong>
    sell_pre는 양쪽 타임프레임 모두 양수에서 하강해야 발동(AND) → 단기 노이즈에 덜 민감합니다.
    소진도 점수로 부분 익절 비율을 동적 결정 → 잔여 포지션으로 추가 수익을 노립니다.
    "파란 바" 3연속 누적 시 잔여 전량 청산.
  </div>
</div>

<!-- 알고리즘 비교 -->
<div id="talgo" class="section">
  <div class="card">
    <div class="ch">⚡ 알고리즘 변경 상세 비교</div>
    <table>
      <thead><tr>
        <th style="width:22%">항목</th>
        <th style="width:39%">🅰 구 로직 (이전)</th>
        <th style="width:39%">🅱 신 로직 (현재)</th>
      </tr></thead>
      <tbody>{algo_comparison()}</tbody>
    </table>
  </div>
  <div class="card">
    <div class="ch">📖 변경 배경 — 사용자 매매 직관 반영</div>
    <div class="cb" style="font-size:13px;line-height:2;color:#374151">
      <p><strong>① buy_pre OR 완화</strong>: 히스토그램이 음수 → 회복하는 초기에는 하나의 타임프레임이 먼저 반응합니다.
      AND 조건은 두 타임프레임이 동시에 신호를 줄 때까지 기다리므로 진입이 늦어집니다.</p>
      <br>
      <p><strong>② sell_pre AND + 양수 제한</strong>: OR 조건에서는 단기 노이즈 하나에도 전량 청산.
      양쪽 모두 양수 구간에서 하강할 때만 진짜 수급 이탈로 봅니다.
      음수 구간 하락은 이미 데드크로스가 지난 후이므로 sell_pre에서 제외합니다.</p>
      <br>
      <p><strong>③ 동적 부분 익절</strong>: "히스토그램 피크 찍고 내려올 때 일부 익절, 나머지 홀드"를
      소진도(0~1) 점수로 수치화. 소진도 낮으면 모멘텀 살아있음 → 익절 안 함.
      소진도 높을수록 많은 비율 매도(최대 80%).</p>
      <br>
      <p><strong>④ 연속 sell_pre (파란 바 누적)</strong>: 3사이클(9분) 연속으로 양쪽 타임프레임이
      하강하면 "이제 나갈 때"로 판단 → 잔여 포지션 전량 청산.</p>
    </div>
  </div>
  <div class="warn">
    ⚠️ 단일 종목·날짜 기준 결과입니다. 알고리즘의 실제 유효성은
    충분한 종목과 기간에 걸친 백테스트로 검증해야 합니다.
  </div>
</div>

<!-- 타임라인 -->
<div id="ttl" class="section">
  <div class="card">
    <div class="ch">🕐 당일 주요 이벤트 타임라인</div>
    <div class="cb">{key_timeline()}</div>
  </div>
</div>

<!-- 파라미터 -->
<div id="tpar" class="section">
  <div class="card">
    <div class="ch">⚙ 시뮬레이션 파라미터</div>
    <div class="cb">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
        {"".join(
          f'<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;'
          f'padding:10px 14px;display:flex;justify-content:space-between;align-items:center">'
          f'<span style="color:#64748b;font-size:13px">{k}</span>'
          f'<span style="color:#0f172a;font-weight:700;font-size:13px">{v}</span></div>'
          for k, v in [
              ("투자 원금",       f"{CAPITAL:,.0f}원"),
              ("1차 매수 비율",   f"{TRANCHE_RATIO*100:.0f}%"),
              ("초기 손절",       f"진입가 −{INITIAL_STOP_PCT:.0f}%"),
              ("트레일링 활성",   f"진입가 +{TRIGGER_PCT:.0f}% 돌파"),
              ("트레일링 플로어", f"고점 −{FLOOR_PCT:.0f}%"),
              ("거래량 급증 기준",f"평균 대비 ×{VOL_SURGE_RATIO}"),
              ("Opening Gate",    "09:10"),
              ("시간컷",          "14:50 (수익권)"),
              ("연속 sell_pre",   "3사이클 → 잔여 전량 청산"),
              ("수수료",          "미반영 (~왕복 1,950원)"),
          ]
        )}
      </div>
    </div>
  </div>
</div>

</div>
<script>
function sw(btn, id) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById(id).classList.add('active');
}}

const pd = {json.dumps(prices_a)};
const mb = {json.dumps(macd_b)};  // 신 로직 MACD (AND+양수 기준)
const ea = {json.dumps(evts_a)};
const eb = {json.dumps(evts_b)};

const labels = pd.map(d => d.time);
const closes = pd.map(d => d.close);

// 이벤트 인덱스 계산
const buyA=[], sellA=[], buyB=[], sellB=[];
ea.forEach(e => {{
  const i = labels.indexOf(e.time); if (i<0) return;
  if (e.type.startsWith('BUY')) buyA.push(i);
  if (e.type.startsWith('SELL') && e.pnl!==null) sellA.push(i);
}});
eb.forEach(e => {{
  const i = labels.indexOf(e.time); if (i<0) return;
  if (e.type.startsWith('BUY')) buyB.push(i);
  if (e.type.startsWith('SELL') && e.pnl!==null) sellB.push(i);
}});

const ptR = labels.map((_,i) =>
  (buyA.includes(i)||sellA.includes(i)||buyB.includes(i)||sellB.includes(i)) ? 7 : 0);
const ptC = labels.map((_,i) => {{
  if (buyA.includes(i)||buyB.includes(i)) return '#10b981';
  if (sellA.includes(i)||sellB.includes(i)) return '#ef4444';
  return 'transparent';
}});

// 가격 차트
new Chart(document.getElementById('cMain').getContext('2d'), {{
  type:'line', data:{{labels, datasets:[
    {{label:'종가', data:closes, borderColor:'#3b82f6', borderWidth:2,
      pointRadius:ptR, pointBackgroundColor:ptC, pointBorderColor:ptC,
      fill:false, tension:0.12}},
    {{label:'손절선', data:pd.map(d=>d.stop), borderColor:'#ef4444',
      borderWidth:1.2, borderDash:[4,3], pointRadius:0, fill:false, spanGaps:true}},
    {{label:'평단가', data:pd.map(d=>d.avg), borderColor:'#f59e0b',
      borderWidth:1, borderDash:[2,4], pointRadius:0, fill:false, spanGaps:true}},
  ]}}, options:{{
    responsive:true, interaction:{{mode:'index',intersect:false}},
    plugins:{{
      legend:{{labels:{{color:'#64748b',font:{{size:11}}}}}},
      tooltip:{{backgroundColor:'#1e293b',titleColor:'#f8fafc',bodyColor:'#cbd5e1',
                borderColor:'#334155',borderWidth:1,
                callbacks:{{label:c=>`${{c.dataset.label}}: ${{c.parsed.y?.toLocaleString()}}원`}}}}
    }},
    scales:{{
      x:{{ticks:{{color:'#94a3b8',maxTicksLimit:14,font:{{size:10}}}},grid:{{color:'#f1f5f9'}}}},
      y:{{ticks:{{color:'#94a3b8',callback:v=>v.toLocaleString()+'원',font:{{size:10}}}},grid:{{color:'#f1f5f9'}}}}
    }}
  }}
}});

// MACD 히스토그램 차트 (신 로직)
const h3vals = mb.map(d=>d.h3);
const bColors = mb.map(d=>{{
  if (d.sig==='buy_pre')  return 'rgba(16,185,129,0.8)';
  if (d.sig==='sell_pre') return 'rgba(239,68,68,0.8)';
  return d.h3>=0 ? 'rgba(96,165,250,0.55)' : 'rgba(148,163,184,0.55)';
}});

new Chart(document.getElementById('cMacd').getContext('2d'), {{
  type:'bar', data:{{labels, datasets:[{{
    label:'MACD 히스토그램 (3분봉, 신 로직 기준)',
    data:h3vals, backgroundColor:bColors, borderWidth:0,
  }}]}}, options:{{
    responsive:true, interaction:{{mode:'index',intersect:false}},
    plugins:{{
      legend:{{labels:{{color:'#64748b',font:{{size:11}}}}}},
      tooltip:{{backgroundColor:'#1e293b',titleColor:'#f8fafc',bodyColor:'#cbd5e1',
                borderColor:'#334155',borderWidth:1,
                callbacks:{{label:c=>`MACD hist: ${{c.parsed.y?.toFixed(1)}} | ${{mb[c.dataIndex]?.sig}}`}}}}
    }},
    scales:{{
      x:{{ticks:{{color:'#94a3b8',maxTicksLimit:14,font:{{size:10}}}},grid:{{color:'#f1f5f9'}}}},
      y:{{ticks:{{color:'#94a3b8',font:{{size:10}}}},grid:{{color:'#f1f5f9'}}}}
    }}
  }}
}});
</script>
</body>
</html>"""

out = "/Users/dean/Documents/workspace-DQT/simulation_093370.html"
with open(out, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"\n✅ HTML 저장: {out}")
print(f"  시나리오 A (구 로직): {pnl_a:+,.0f}원 ({ret_a:+.2f}%)")
print(f"  시나리오 B (신 로직): {pnl_b:+,.0f}원 ({ret_b:+.2f}%)")
print(f"  개선폭: {diff:+,.0f}원 ({diff_pct:+.2f}%p)")
