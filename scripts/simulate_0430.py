"""
simulate_0430.py
2026-04-30 (목) DQT v2.3.0 시뮬레이션

네이버 금융 API로 실제 1분봉 OHLCV 데이터를 가져와
당일 슬롯 배정 → 분 단위 진입/청산 시뮬레이션.

실행:
  cd /Users/dean/Documents/workspace-DQT
  python3 scripts/simulate_0430.py
"""
from __future__ import annotations

import json
import sys
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# ══════════════════════════════════════════════════════════════
# 1. 시뮬레이션 파라미터
# ══════════════════════════════════════════════════════════════
SIM_DATE        = "2026-04-30"
INVEST_PER_SLOT = 3_000_000       # 슬롯당 투자금 300만원
TOTAL_CASH      = 10_000_000      # 계좌 총 자산 1,000만원

# v2.3.0 트레일링 스톱
TRAILING_INITIAL_STOP = 2.0       # 초기 손절선 -2%
TRAILING_TRIGGER      = 3.0       # 트레일링 상향 시작 +3%
TRAILING_FLOOR        = 2.5       # 트레일링 간격 -2.5%
TRAILING_TIGHT_FLOOR  = 1.25      # MACD sell_pre 시 절반

TAKE_PROFIT_1 = 5.0               # +5%  → 1/3 익절 or 보류
TAKE_PROFIT_2 = 10.0              # +10% → 1/3 익절 or 보류

# 시간 게이트 (분 인덱스, 0=09:00)
SLOT_ASSIGN_MIN = 7    # 09:07 슬롯 배정
ENTRY_START_MIN = 10   # 09:10 진입 시작
BUY_CUTOFF_MIN  = 180  # 12:00 매수 마감
TIMECUT_MIN     = 380  # 15:20 타임컷

# ══════════════════════════════════════════════════════════════
# 2. 후보 종목 정보
# ══════════════════════════════════════════════════════════════
# 4월 30일 실제 일봉 기준 슬롯 배정:
#   Leader    (gap_up_breakout): LG전자       — gap +3.3%, 고가+8%까지 상승
#   Breakout  (volume_surge)  : 산일전기      — 장중 +19.6%, 거래량 폭발
#   Pullback  (gap_up_breakout): 두산로보틱스  — gap +17.4% 후 -8% 하락

CANDIDATES = [
    {
        "ticker":     "066570",
        "name":       "LG전자",
        "slot":       "leader",
        "signal":     "gap_up_breakout",
        "prev_close": 135_800,
    },
    {
        "ticker":     "062040",
        "name":       "산일전기",
        "slot":       "breakout",
        "signal":     "volume_surge",
        "prev_close": 221_000,
    },
    {
        "ticker":     "454910",
        "name":       "두산로보틱스",
        "slot":       "pullback",
        "signal":     "gap_up_breakout",
        "prev_close": 102_200,
    },
]

# ══════════════════════════════════════════════════════════════
# 3. 네이버 금융 API — 실제 1분봉 취득
# ══════════════════════════════════════════════════════════════
_NAVER_URL = (
    "https://api.stock.naver.com/chart/domestic/item"
    "/{ticker}/minute?startDateTime={start}&endDateTime={end}"
)

@dataclass
class Bar:
    ts:     str    # "20260430090000"
    open:   float
    high:   float
    low:    float
    close:  float  # currentPrice
    volume: int    # accumulatedTradingVolume (누적)


def fetch_bars(ticker: str) -> list[Bar]:
    """네이버 API로 당일 1분봉 전체 취득."""
    url = _NAVER_URL.format(
        ticker=ticker,
        start="202604300900",
        end="202604301530",
    )
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0"}
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        raw = json.load(r)

    bars = []
    for d in raw:
        bars.append(Bar(
            ts=d["localDateTime"],
            open=float(d.get("openPrice") or d["currentPrice"]),
            high=float(d["highPrice"]),
            low=float(d["lowPrice"]),
            close=float(d["currentPrice"]),
            volume=int(d.get("accumulatedTradingVolume", 0)),
        ))
    return bars


def ts_to_min_idx(ts: str) -> int:
    """'20260430HHMI' → 분 인덱스 (0=09:00)."""
    h = int(ts[8:10])
    mi = int(ts[10:12])
    return (h - 9) * 60 + mi


def min_idx_to_time(m: int) -> str:
    h = 9 + m // 60
    mi = m % 60
    return f"{h:02d}:{mi:02d}"


# ══════════════════════════════════════════════════════════════
# 4. 보조 지표 (롤링 윈도우)
# ══════════════════════════════════════════════════════════════

def calc_rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas[-period:]]
    losses = [max(-d, 0) for d in deltas[-period:]]
    avg_g  = sum(gains) / period
    avg_l  = sum(losses) / period
    if avg_l == 0:
        return 100.0
    return 100 - (100 / (1 + avg_g / avg_l))


def _ema(data: list[float], period: int) -> float:
    if not data:
        return 0.0
    k = 2 / (period + 1)
    v = data[0]
    for x in data[1:]:
        v = x * k + v * (1 - k)
    return v


def calc_macd(closes: list[float]) -> tuple[float, float, str]:
    """MACD 라인, 시그널, 신호 반환."""
    if len(closes) < 27:
        return 0.0, 0.0, "neutral"

    macd_series = [
        _ema(closes[max(0, i-25):i+1], 12) - _ema(closes[max(0, i-25):i+1], 26)
        for i in range(26, len(closes))
    ]
    if len(macd_series) < 2:
        return 0.0, 0.0, "neutral"

    macd_line   = macd_series[-1]
    signal_line = _ema(macd_series[-9:], 9) if len(macd_series) >= 9 else macd_series[-1]
    hist        = macd_line - signal_line
    hist_prev   = (macd_series[-2] - signal_line) if len(macd_series) >= 2 else hist

    if hist > 0 and hist >= hist_prev:
        sig = "buy"
    elif hist > 0:
        sig = "buy_pre"
    elif hist < 0 and hist <= hist_prev:
        sig = "sell"
    else:
        sig = "sell_pre"

    return macd_line, signal_line, sig


def calc_vol_ratio(vols: list[int], lookback: int = 20) -> float:
    """현재 분봉 거래량 / 최근 lookback 분 평균."""
    if len(vols) < 2:
        return 1.0
    # 누적 거래량 → 분봉 거래량 변환
    bar_vols = [max(0, vols[i] - vols[i-1]) for i in range(1, len(vols))]
    if not bar_vols:
        return 1.0
    cur = bar_vols[-1]
    avg = sum(bar_vols[-lookback:]) / len(bar_vols[-lookback:])
    return cur / avg if avg > 0 else 1.0


# ══════════════════════════════════════════════════════════════
# 5. 진입 게이트 & 점수
# ══════════════════════════════════════════════════════════════

def check_entry_gates(stock: dict, bars_so_far: list[Bar],
                      m: int, slot: str) -> tuple[bool, str]:
    """
    v2.3.0 5-gate 진입 체크.
    Returns (ok, reason)
    """
    if len(bars_so_far) < 2:
        return False, "데이터 부족"

    closes = [b.close for b in bars_so_far]
    vols   = [b.volume for b in bars_so_far]
    rsi    = calc_rsi(closes)
    _, _, macd_sig = calc_macd(closes)
    gap_pct = (stock.get("open_price", closes[0]) - stock["prev_close"]) / stock["prev_close"] * 100

    # Gate 1: RSI 하드 실패
    rsi_limit = 90 if gap_pct >= 5 else 85
    if rsi > rsi_limit:
        return False, f"RSI {rsi:.0f} > {rsi_limit}"

    # Gate 2: 시간 게이트
    if m < ENTRY_START_MIN:
        return False, f"09:10 이전 ({min_idx_to_time(m)})"
    if m >= BUY_CUTOFF_MIN:
        return False, "12:00 이후 매수 마감"
    if slot == "pullback" and m >= 300:
        return False, "pullback 14:00 이후 차단"

    # Gate 3: MACD 방향 (leader/breakout은 sell 시 차단)
    if macd_sig in ("sell", "sell_pre") and slot in ("leader", "breakout"):
        return False, f"MACD {macd_sig}"

    # Gate 4: leader 11:00 이후 MACD 강세 필요
    if slot == "leader" and m >= 120 and macd_sig not in ("buy_pre", "buy"):
        return False, f"11:00↑ leader MACD 불충분 ({macd_sig})"

    # Gate 5: 진입 점수
    score = _entry_score(closes, vols, stock, rsi, macd_sig, gap_pct)
    if score < 50:
        return False, f"진입점수 {score}pt < 50"

    pct = 100 if score >= 72 else 75
    return True, f"OK (점수 {score}pt / {pct}% 투입) RSI={rsi:.0f} MACD={macd_sig}"


def _entry_score(closes: list[float], vols: list[int], stock: dict,
                 rsi: float, macd_sig: str, gap_pct: float) -> int:
    score = 0

    # 거래량 (0-30): vol_ratio 기반
    vr = calc_vol_ratio(vols)
    if vr >= 3.0:   score += 30
    elif vr >= 2.0: score += 20
    elif vr >= 1.5: score += 12
    elif vr >= 1.0: score += 6

    # RSI (0-20)
    if 50 <= rsi <= 70:    score += 20
    elif rsi < 50:         score += 12
    elif rsi <= 78:        score += 8

    # OBV 대체: 현재가 vs 당일 시가
    if closes and stock.get("open_price"):
        cur = closes[-1]
        if cur >= stock["open_price"]:       score += 20
        elif cur >= stock["open_price"] * 0.98: score += 10

    # StochRSI 대체 (0-15)
    if rsi <= 80:   score += 15
    elif rsi <= 85: score += 8

    # MACD (0-15)
    macd_pts = {"buy": 15, "buy_pre": 10, "neutral": 5, "sell_pre": 2, "sell": 0}
    score += macd_pts.get(macd_sig, 0)

    return min(100, score)


# ══════════════════════════════════════════════════════════════
# 6. 포지션 관리
# ══════════════════════════════════════════════════════════════

@dataclass
class Position:
    ticker:      str
    name:        str
    slot:        str
    entry_price: float
    quantity:    int
    invested:    float
    entry_min:   int

    stop_loss:   float = 0.0
    peak_price:  float = 0.0
    tp1_done:    bool  = False
    tp2_done:    bool  = False
    realized_pnl: float = 0.0   # TP1/TP2 부분 매도로 이미 실현된 손익

    is_closed:   bool  = False
    close_price: float = 0.0
    close_min:   int   = 0
    close_reason: str  = ""
    final_pnl:   float = 0.0    # 최종 청산 시 손익 (realized_pnl 포함)

    def __post_init__(self):
        self.stop_loss  = self.entry_price * (1 - TRAILING_INITIAL_STOP / 100)
        self.peak_price = self.entry_price

    @property
    def total_pnl(self) -> float:
        return self.final_pnl


@dataclass
class Event:
    min_idx:    int
    ticker:     str
    name:       str
    slot:       str
    event_type: str
    price:      float
    detail:     str
    pnl:        float = 0.0


# ══════════════════════════════════════════════════════════════
# 7. 메인 시뮬레이션
# ══════════════════════════════════════════════════════════════

def run_simulation() -> tuple[list[Event], list[Position], dict[str, list[Bar]]]:
    print(f"[시뮬레이션] {SIM_DATE} — 실제 1분봉 데이터 사용")

    # 실제 1분봉 취득
    all_bars: dict[str, list[Bar]] = {}
    for stock in CANDIDATES:
        ticker = stock["ticker"]
        bars = fetch_bars(ticker)
        all_bars[ticker] = bars
        if bars:
            stock["open_price"] = bars[0].open
            gap = (bars[0].open - stock["prev_close"]) / stock["prev_close"] * 100
            print(f"  {stock['name']}({ticker}): {len(bars)}봉 "
                  f"시가={bars[0].open:,.0f} 종가={bars[-1].close:,.0f} "
                  f"갭{gap:+.1f}%")

    # 분 인덱스별 bar 매핑
    bar_map: dict[str, dict[int, Bar]] = {}
    for ticker, bars in all_bars.items():
        bar_map[ticker] = {}
        for b in bars:
            idx = ts_to_min_idx(b.ts)
            bar_map[ticker][idx] = b

    events:    list[Event]    = []
    positions: dict[str, Position] = {}
    closed:    list[Position] = []
    slot_filled: dict[str, bool] = {s["slot"]: False for s in CANDIDATES}

    # 슬롯 배정 이벤트 (09:07)
    for stock in CANDIDATES:
        ticker = stock["ticker"]
        b = bar_map[ticker].get(SLOT_ASSIGN_MIN)
        price = b.close if b else stock.get("open_price", 0)
        gap = (stock.get("open_price", price) - stock["prev_close"]) / stock["prev_close"] * 100
        events.append(Event(
            min_idx=SLOT_ASSIGN_MIN, ticker=ticker, name=stock["name"],
            slot=stock["slot"], event_type="SLOT", price=price,
            detail=f"슬롯 배정: {stock['slot'].upper()} | 갭 {gap:+.1f}% | {stock['signal']}",
        ))

    # 분 단위 루프 09:00~15:30
    for m in range(391):
        for stock in CANDIDATES:
            ticker = stock["ticker"]
            slot   = stock["slot"]
            bmap   = bar_map[ticker]

            b = bmap.get(m)
            if b is None:
                continue

            cur_price = b.close

            # 이전 분까지의 bar 리스트
            bars_so_far = [bmap[i] for i in sorted(bmap) if i <= m]
            closes = [x.close for x in bars_so_far]
            vols   = [x.volume for x in bars_so_far]

            pos = positions.get(ticker)

            # ── 포지션 없음: 진입 체크 ──────────────────────────
            if pos is None and not slot_filled[slot]:
                ok, reason = check_entry_gates(stock, bars_so_far, m, slot)
                if not ok:
                    if m % 30 == 0 and m >= ENTRY_START_MIN:
                        events.append(Event(
                            min_idx=m, ticker=ticker, name=stock["name"],
                            slot=slot, event_type="GATE_FAIL",
                            price=cur_price, detail=reason,
                        ))
                else:
                    qty = max(1, int(INVEST_PER_SLOT / cur_price))
                    invested = qty * cur_price
                    _, _, msig = calc_macd(closes)
                    rsi = calc_rsi(closes)
                    pos = Position(
                        ticker=ticker, name=stock["name"], slot=slot,
                        entry_price=cur_price, quantity=qty,
                        invested=invested, entry_min=m,
                    )
                    positions[ticker] = pos
                    slot_filled[slot] = True
                    events.append(Event(
                        min_idx=m, ticker=ticker, name=stock["name"],
                        slot=slot, event_type="BUY", price=cur_price,
                        detail=(f"매수 {qty:,}주 × {cur_price:,.0f}원"
                                f" = {invested:,.0f}원 | RSI={rsi:.0f} MACD={msig}"),
                    ))

            # ── 포지션 있음: 감시 ────────────────────────────────
            elif pos is not None and not pos.is_closed:
                _, _, msig = calc_macd(closes)
                pnl_pct = (cur_price - pos.entry_price) / pos.entry_price * 100

                # 피크 업데이트 (고가 기준)
                if b.high > pos.peak_price:
                    pos.peak_price = b.high

                # 트레일링 스톱 업데이트
                if pnl_pct >= TRAILING_TRIGGER:
                    floor = TRAILING_TIGHT_FLOOR if msig in ("sell_pre", "sell") else TRAILING_FLOOR
                    new_stop = pos.peak_price * (1 - floor / 100)
                    if new_stop > pos.stop_loss:
                        pos.stop_loss = new_stop

                # 스톱 체크 — 저가가 손절선 아래면 손절선 가격에서 체결
                if b.low <= pos.stop_loss:
                    exec_price = pos.stop_loss  # 스톱 레벨에서 정확히 체결
                    ep = exec_price
                    stop_pnl = (ep - pos.entry_price) * pos.quantity
                    pos.realized_pnl += stop_pnl
                    pos.is_closed   = True
                    pos.close_price = ep
                    pos.close_min   = m
                    pos.close_reason = "STOP"
                    pos.final_pnl   = pos.realized_pnl
                    closed.append(pos)
                    del positions[ticker]
                    stop_pct = (ep - pos.entry_price) / pos.entry_price * 100
                    events.append(Event(
                        min_idx=m, ticker=ticker, name=stock["name"],
                        slot=slot, event_type="STOP", price=ep,
                        detail=(f"손절 {stop_pct:+.1f}%"
                                f" | 청산손익 {stop_pnl:+,.0f}원"
                                f" | 누적손익 {pos.final_pnl:+,.0f}원"),
                        pnl=pos.final_pnl,
                    ))

                # TP1 (+5%)
                elif not pos.tp1_done and pnl_pct >= TAKE_PROFIT_1:
                    if msig in ("sell_pre", "sell"):
                        sell_qty = pos.quantity // 3
                        if sell_qty > 0:
                            partial_pnl = (cur_price - pos.entry_price) * sell_qty
                            pos.quantity    -= sell_qty
                            pos.realized_pnl += partial_pnl
                            pos.tp1_done = True
                            events.append(Event(
                                min_idx=m, ticker=ticker, name=stock["name"],
                                slot=slot, event_type="TP1", price=cur_price,
                                detail=(f"1차 익절 1/3({sell_qty}주)"
                                        f" MACD={msig}"
                                        f" | 부분손익 {partial_pnl:+,.0f}원"),
                                pnl=partial_pnl,
                            ))
                    else:
                        lock_stop = pos.entry_price * 1.01
                        if lock_stop > pos.stop_loss:
                            pos.stop_loss = lock_stop
                        pos.tp1_done = True
                        events.append(Event(
                            min_idx=m, ticker=ticker, name=stock["name"],
                            slot=slot, event_type="TP1", price=cur_price,
                            detail=(f"+{pnl_pct:.1f}% — MACD {msig} 강세"
                                    f" 익절 보류 / 손절선→{lock_stop:,.0f}원"),
                        ))

                # TP2 (+10%)
                elif not pos.tp2_done and pos.tp1_done and pnl_pct >= TAKE_PROFIT_2:
                    if msig in ("sell_pre", "sell"):
                        sell_qty = pos.quantity // 2
                        if sell_qty > 0:
                            partial_pnl = (cur_price - pos.entry_price) * sell_qty
                            pos.quantity    -= sell_qty
                            pos.realized_pnl += partial_pnl
                            pos.tp2_done = True
                            events.append(Event(
                                min_idx=m, ticker=ticker, name=stock["name"],
                                slot=slot, event_type="TP2", price=cur_price,
                                detail=(f"2차 익절 1/2({sell_qty}주)"
                                        f" MACD={msig}"
                                        f" | 부분손익 {partial_pnl:+,.0f}원"),
                                pnl=partial_pnl,
                            ))
                    else:
                        pos.tp2_done = True
                        events.append(Event(
                            min_idx=m, ticker=ticker, name=stock["name"],
                            slot=slot, event_type="TP2", price=cur_price,
                            detail=f"+{pnl_pct:.1f}% — MACD {msig} 강세 익절 보류",
                        ))

        # 15:20 타임컷
        if m == TIMECUT_MIN:
            for ticker, pos in list(positions.items()):
                if pos.is_closed:
                    continue
                b = bar_map[ticker].get(m)
                ep = b.close if b else pos.entry_price
                tc_pnl = (ep - pos.entry_price) * pos.quantity
                pos.realized_pnl += tc_pnl
                pos.is_closed  = True
                pos.close_price = ep
                pos.close_min  = m
                pos.close_reason = "TIMECUT"
                pos.final_pnl  = pos.realized_pnl
                closed.append(pos)
                events.append(Event(
                    min_idx=m, ticker=ticker, name=pos.name, slot=pos.slot,
                    event_type="TIMECUT", price=ep,
                    detail=f"15:20 타임컷 | 청산손익 {tc_pnl:+,.0f}원 | 누적손익 {pos.final_pnl:+,.0f}원",
                    pnl=pos.final_pnl,
                ))
            positions.clear()

    return events, closed, all_bars


# ══════════════════════════════════════════════════════════════
# 8. HTML 시각화
# ══════════════════════════════════════════════════════════════

EVENT_COLORS = {
    "SLOT":      "#4a9eff",
    "GATE_FAIL": "#555",
    "BUY":       "#26a69a",
    "TP1":       "#66bb6a",
    "TP2":       "#aed581",
    "STOP":      "#ef5350",
    "TIMECUT":   "#ff7043",
}
EVENT_ICONS = {
    "SLOT": "📌", "GATE_FAIL": "🚫", "BUY": "🟢",
    "TP1": "🟡", "TP2": "💛", "STOP": "🔴", "TIMECUT": "⏱",
}
SLOT_LABELS = {
    "leader":   "🏆 Leader — 갭업돌파",
    "breakout": "💥 Breakout — 거래량폭발",
    "pullback": "📉 Pullback — 눌림반등",
}
SLOT_BG = {
    "leader":   "#1a2a3a",
    "breakout": "#1a2e1a",
    "pullback": "#2a1a3a",
}


def _price_svg(bars: list[Bar], pos: Position | None,
               events_for: list[Event]) -> str:
    if not bars:
        return ""
    W, H = 840, 90
    PAD_L, PAD_R, PAD_T, PAD_B = 4, 4, 6, 36

    closes = [b.close for b in bars]
    mn, mx = min(min(b.low for b in bars), mn_stop := 0), max(b.high for b in bars)
    mn = min(b.low for b in bars)
    if pos:
        mn = min(mn, pos.stop_loss * 0.998)
    rng = mx - mn if mx > mn else 1.0

    n = len(bars)

    def px(i: int) -> float:
        return PAD_L + (W - PAD_L - PAD_R) * i / max(n - 1, 1)

    def py(v: float) -> float:
        return PAD_T + (H - PAD_T - PAD_B) * (1 - (v - mn) / rng)

    # 가격 라인
    points = " ".join(f"{px(i):.1f},{py(b.close):.1f}" for i, b in enumerate(bars))

    # 12:00 컷 수직선
    cutoff_x = px(min(BUY_CUTOFF_MIN, n - 1))
    lines = (
        f'<line x1="{cutoff_x:.1f}" y1="{PAD_T}" x2="{cutoff_x:.1f}" y2="{H-PAD_B}" '
        f'stroke="#ffd54f" stroke-width="0.8" stroke-dasharray="3,3" opacity="0.6"/>'
    )

    # 매수가 & 초기 손절선
    if pos:
        ep_y  = py(pos.entry_price)
        sl_y  = py(pos.entry_price * (1 - TRAILING_INITIAL_STOP / 100))
        lines += (
            f'<line x1="{PAD_L}" y1="{ep_y:.1f}" x2="{W-PAD_R}" y2="{ep_y:.1f}" '
            f'stroke="#26a69a" stroke-width="1" stroke-dasharray="5,3" opacity="0.8"/>'
            f'<line x1="{PAD_L}" y1="{sl_y:.1f}" x2="{W-PAD_R}" y2="{sl_y:.1f}" '
            f'stroke="#ef5350" stroke-width="0.8" stroke-dasharray="4,3" opacity="0.7"/>'
        )

    # 이벤트 마커
    markers = []
    for ev in events_for:
        if ev.event_type in ("GATE_FAIL", "SLOT"):
            continue
        idx = min(ev.min_idx, n - 1)
        x = px(idx)
        y = py(ev.price)
        c = EVENT_COLORS.get(ev.event_type, "#aaa")
        markers.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="{c}" '
            f'stroke="#0f1117" stroke-width="1.5">'
            f'<title>{min_idx_to_time(ev.min_idx)} {ev.event_type}: {ev.price:,.0f}원'
            f'{" | "+f"{ev.pnl:+,.0f}원" if ev.pnl else ""}</title>'
            f'</circle>'
        )

    # X 축 시간 레이블 (09, 10, 11, 12, 13, 14, 15)
    x_labels = ""
    for hh in range(9, 16):
        mi_idx = (hh - 9) * 60
        if mi_idx < n:
            x = px(mi_idx)
            x_labels += (
                f'<text x="{x:.1f}" y="{H-4}" text-anchor="middle" '
                f'font-size="9" fill="#666">{hh:02d}:00</text>'
            )

    return (
        f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" '
        f'style="width:100%;height:{H}px;display:block;">'
        f'<rect width="{W}" height="{H}" fill="#0d0f1a" rx="4"/>'
        f'{lines}'
        f'<polyline points="{points}" fill="none" stroke="#4a9eff" stroke-width="1.5"/>'
        f'{"".join(markers)}'
        f'{x_labels}'
        f'</svg>'
    )


def build_html(events: list[Event], closed: list[Position],
               all_bars: dict[str, list[Bar]]) -> str:

    total_pnl = sum(p.total_pnl for p in closed)
    total_invested = sum(p.invested for p in closed)
    ret_pct_invested = total_pnl / total_invested * 100 if total_invested else 0
    ret_pct_account  = total_pnl / TOTAL_CASH * 100

    ticker_events: dict[str, list[Event]] = {}
    for ev in events:
        ticker_events.setdefault(ev.ticker, []).append(ev)

    # 요약 카드
    pnl_color = "#26a69a" if total_pnl >= 0 else "#ef5350"
    summary = f"""
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:10px;margin-bottom:24px">
  <div style="background:#1e3a5f;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.77em">시뮬레이션 날짜</div>
    <div style="font-size:1.1em;font-weight:700;color:#e6edf3">2026-04-30 (목)</div>
  </div>
  <div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.77em">총 손익 (누적 실현)</div>
    <div style="font-size:1.2em;font-weight:700;color:{pnl_color}">
      {"+" if total_pnl>=0 else ""}{total_pnl:,.0f}원
    </div>
  </div>
  <div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.77em">투자 대비 수익률</div>
    <div style="font-size:1.2em;font-weight:700;color:{pnl_color}">
      {"+" if ret_pct_invested>=0 else ""}{ret_pct_invested:.2f}%
      <span style="font-size:0.7em;color:#555">(투자금 {total_invested/1e6:.2f}M)</span>
    </div>
  </div>
  <div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.77em">계좌 수익률 (10M 기준)</div>
    <div style="font-size:1.2em;font-weight:700;color:{pnl_color}">
      {"+" if ret_pct_account>=0 else ""}{ret_pct_account:.2f}%
    </div>
  </div>
  <div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.77em">데이터 소스</div>
    <div style="font-size:0.95em;font-weight:600;color:#ccd6f6">네이버 금융 실제 1분봉</div>
  </div>
</div>"""

    # 범례
    legend = """
<div style="display:flex;gap:14px;flex-wrap:wrap;margin-bottom:18px;font-size:0.81em;color:#8892b0">
  <span>🟢 BUY 매수</span>
  <span>🔴 STOP 손절 (스톱레벨 체결)</span>
  <span>🟡 TP1 +5% 익절</span>
  <span>💛 TP2 +10% 익절</span>
  <span>⏱ TIMECUT 15:20 청산</span>
  <span style="color:#26a69a">— 매수가</span>
  <span style="color:#ef5350">--- 초기손절선(-2%)</span>
  <span style="color:#ffd54f">⋮ 12:00 매수마감</span>
</div>"""

    # 슬롯 섹션
    sections = []
    for stock in CANDIDATES:
        ticker = stock["ticker"]
        slot   = stock["slot"]
        bars   = all_bars.get(ticker, [])
        evs    = ticker_events.get(ticker, [])

        closed_pos = next((p for p in closed if p.ticker == ticker), None)
        total_pos_pnl = closed_pos.total_pnl if closed_pos else 0.0
        entry_p = closed_pos.entry_price if closed_pos else 0.0
        close_p = closed_pos.close_price if closed_pos else 0.0
        qty     = closed_pos.quantity    if closed_pos else 0
        invested = closed_pos.invested   if closed_pos else 0.0
        pnl_pct = (close_p - entry_p) / entry_p * 100 if entry_p else 0.0
        pc = "#26a69a" if total_pos_pnl >= 0 else "#ef5350"
        sign = "+" if total_pos_pnl >= 0 else ""

        svg = _price_svg(bars, closed_pos, evs)

        rows = []
        for ev in evs:
            if ev.event_type == "GATE_FAIL" and ev.min_idx % 30 != 0:
                continue
            c = EVENT_COLORS.get(ev.event_type, "#aaa")
            ico = EVENT_ICONS.get(ev.event_type, "•")
            pnl_cell = ""
            if ev.pnl != 0:
                pc2 = "#26a69a" if ev.pnl >= 0 else "#ef5350"
                s2 = "+" if ev.pnl >= 0 else ""
                pnl_cell = f'<span style="color:{pc2};font-weight:700">{s2}{ev.pnl:,.0f}원</span>'
            rows.append(
                f'<tr>'
                f'<td style="color:#aaa;white-space:nowrap;padding:3px 8px">{min_idx_to_time(ev.min_idx)}</td>'
                f'<td style="color:{c};padding:3px 8px;white-space:nowrap">{ico} {ev.event_type}</td>'
                f'<td style="padding:3px 8px;white-space:nowrap">{ev.price:,.0f}원</td>'
                f'<td style="padding:3px 8px;color:#ccc;font-size:0.9em">{ev.detail}</td>'
                f'<td style="padding:3px 8px">{pnl_cell}</td>'
                f'</tr>'
            )

        open_p = bars[0].open if bars else 0
        day_chg = (bars[-1].close - open_p) / open_p * 100 if open_p and bars else 0

        section = f"""
<div style="background:{SLOT_BG[slot]};border:1px solid #2a2d3e;border-radius:8px;
            margin-bottom:22px;overflow:hidden">
  <div style="padding:12px 16px;display:flex;justify-content:space-between;
              align-items:center;border-bottom:1px solid #2a2d3e;flex-wrap:wrap;gap:8px">
    <div>
      <div style="font-size:0.77em;color:#8892b0;letter-spacing:1px">{SLOT_LABELS[slot]}</div>
      <div style="font-size:1.2em;font-weight:700;color:#e6edf3">
        {stock["name"]}
        <span style="color:#555;font-size:0.75em;margin-left:6px">{ticker}</span>
      </div>
    </div>
    <div style="text-align:right">
      <div style="color:#8892b0;font-size:0.8em">
        당일: 시가 {open_p:,.0f} → 종가 {bars[-1].close if bars else 0:,.0f}원
        ({day_chg:+.1f}%)
      </div>
      {"" if not closed_pos else f'''
      <div style="font-size:0.85em;color:#aaa">
        매수 {entry_p:,.0f}원 ({closed_pos.entry_min}분={min_idx_to_time(closed_pos.entry_min)})
        × {closed_pos.quantity + (qty - closed_pos.quantity)}주
        = {invested:,.0f}원
      </div>
      <div style="font-size:1.1em;font-weight:700;color:{pc}">
        {sign}{total_pos_pnl:,.0f}원
        <span style="font-size:0.8em">
          ({sign}{pnl_pct:.2f}% 최종청산기준 / 계좌비중 {total_pos_pnl/TOTAL_CASH*100:+.2f}%)
        </span>
      </div>'''}
    </div>
  </div>
  <div style="padding:6px 12px">{svg}</div>
  <div style="padding:6px 16px 12px;overflow-x:auto">
    <table style="width:100%;border-collapse:collapse;font-size:0.84em;color:#ccd6f6">
      <thead>
        <tr style="color:#8892b0;border-bottom:1px solid #2a2d3e">
          <th style="text-align:left;padding:3px 8px">시각</th>
          <th style="text-align:left;padding:3px 8px">이벤트</th>
          <th style="text-align:left;padding:3px 8px">가격</th>
          <th style="text-align:left;padding:3px 8px">내용</th>
          <th style="text-align:left;padding:3px 8px">손익</th>
        </tr>
      </thead>
      <tbody>{"".join(rows)}</tbody>
    </table>
  </div>
</div>"""
        sections.append(section)

    # 수익률 검증 테이블
    verify = f"""
<div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;
            padding:16px 20px;margin-top:20px;font-size:0.83em">
  <div style="color:#ccd6f6;font-weight:600;margin-bottom:10px">📊 손익 검증 (TP 부분매도 포함)</div>
  <table style="width:100%;border-collapse:collapse;color:#8892b0">
    <thead>
      <tr style="border-bottom:1px solid #2a2d3e;color:#aaa">
        <th style="text-align:left;padding:4px 10px">종목/슬롯</th>
        <th style="text-align:right;padding:4px 10px">투자금</th>
        <th style="text-align:right;padding:4px 10px">TP실현</th>
        <th style="text-align:right;padding:4px 10px">최종청산</th>
        <th style="text-align:right;padding:4px 10px">총손익</th>
        <th style="text-align:right;padding:4px 10px">수익률</th>
      </tr>
    </thead>
    <tbody>"""

    for pos in closed:
        tp_pnl = sum(e.pnl for e in events
                     if e.ticker == pos.ticker and e.event_type in ("TP1", "TP2") and e.pnl)
        final_close_pnl = pos.total_pnl - tp_pnl
        pc2 = "#26a69a" if pos.total_pnl >= 0 else "#ef5350"
        ret = pos.total_pnl / pos.invested * 100 if pos.invested else 0
        sign2 = "+" if pos.total_pnl >= 0 else ""
        verify += (
            f'<tr style="border-bottom:1px solid #1a1d2e">'
            f'<td style="padding:4px 10px">{pos.name} ({pos.slot})</td>'
            f'<td style="padding:4px 10px;text-align:right">{pos.invested:,.0f}원</td>'
            f'<td style="padding:4px 10px;text-align:right;color:#66bb6a">'
            f'{"+" if tp_pnl>=0 else ""}{tp_pnl:,.0f}원</td>'
            f'<td style="padding:4px 10px;text-align:right;color:#aaa">'
            f'{"+" if final_close_pnl>=0 else ""}{final_close_pnl:,.0f}원</td>'
            f'<td style="padding:4px 10px;text-align:right;font-weight:700;color:{pc2}">'
            f'{sign2}{pos.total_pnl:,.0f}원</td>'
            f'<td style="padding:4px 10px;text-align:right;color:{pc2}">'
            f'{sign2}{ret:.2f}%</td>'
            f'</tr>'
        )

    verify += f"""
      <tr style="border-top:2px solid #3a3d4e;font-weight:700;color:#e6edf3">
        <td style="padding:6px 10px">합계</td>
        <td style="padding:6px 10px;text-align:right">{total_invested:,.0f}원</td>
        <td style="padding:6px 10px;text-align:right;color:#66bb6a">
          {sum(e.pnl for e in events if e.event_type in ("TP1","TP2") and e.pnl):+,.0f}원</td>
        <td style="padding:6px 10px;text-align:right;color:#aaa">-</td>
        <td style="padding:6px 10px;text-align:right;color:{pnl_color}">
          {"+" if total_pnl>=0 else ""}{total_pnl:,.0f}원</td>
        <td style="padding:6px 10px;text-align:right;color:{pnl_color}">
          {"+" if ret_pct_invested>=0 else ""}{ret_pct_invested:.2f}%
          (계좌 {"+" if ret_pct_account>=0 else ""}{ret_pct_account:.2f}%)</td>
      </tr>
    </tbody>
  </table>
</div>"""

    methodology = """
<div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;
            padding:16px 20px;margin-top:14px;font-size:0.82em;color:#8892b0;line-height:1.7">
  <div style="color:#ccd6f6;font-weight:600;margin-bottom:6px">📋 시뮬레이션 방법론</div>
  <ul style="margin:0;padding-left:18px">
    <li><b>데이터</b>: 네이버 금융 API 실제 1분봉 (openPrice / highPrice / lowPrice / currentPrice)</li>
    <li><b>슬롯 배정</b>: 09:07 기준 실제 일봉 패턴으로 Leader/Breakout/Pullback 수동 배정</li>
    <li><b>진입</b>: 09:10 이후 5-gate 통과 시 매수 (RSI/MACD/시간/진입점수)</li>
    <li><b>스톱 체결</b>: 분봉 저가(lowPrice)가 손절선 이하이면 <b>손절선 가격에서 체결</b> (슬리피지 없음)</li>
    <li><b>트레일링</b>: 피크는 분봉 고가(highPrice) 기준 업데이트, 스톱은 피크×(1-2.5%)</li>
    <li><b>수익률</b>: 투자 대비(슬롯 투자금 합) + 계좌 대비(10M) 두 가지 모두 표시</li>
    <li><b>TP 부분매도</b>: 실현 손익은 pos.realized_pnl에 누적 → 최종 total_pnl에 반영</li>
    <li><b>슬롯당 투자금</b>: 300만원 (총 계좌 1,000만원 기준)</li>
  </ul>
</div>"""

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>DQT 시뮬레이션 — 2026-04-30</title>
<style>
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ background:#0f1117; color:#ccd6f6;
       font-family:"Pretendard","Noto Sans KR",sans-serif;
       padding:24px; max-width:1100px; margin:0 auto; }}
h1 {{ font-size:1.5em; font-weight:700; color:#e6edf3; margin-bottom:4px; }}
.sub {{ color:#8892b0; font-size:0.84em; margin-bottom:22px; }}
td,th {{ vertical-align:top; }}
</style>
</head>
<body>
<h1>DQT 시뮬레이션 — 2026-04-30 (목)</h1>
<div class="sub">v2.3.0 로직 | 네이버 금융 실제 1분봉 | 생성: {now}</div>
{summary}
{legend}
{"".join(sections)}
{verify}
{methodology}
</body>
</html>"""


# ══════════════════════════════════════════════════════════════
# 9. 실행
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    events, closed, all_bars = run_simulation()

    total_pnl = sum(p.total_pnl for p in closed)
    total_inv  = sum(p.invested for p in closed)

    print("\n=== 최종 결과 ===")
    for pos in sorted(closed, key=lambda p: p.total_pnl, reverse=True):
        tp_pnl = sum(e.pnl for e in events
                     if e.ticker == pos.ticker and e.event_type in ("TP1","TP2") and e.pnl)
        ret = pos.total_pnl / pos.invested * 100 if pos.invested else 0
        s = "+" if pos.total_pnl >= 0 else ""
        print(f"  {pos.slot:10s} {pos.name}({pos.ticker}): "
              f"매수 {pos.entry_price:,.0f} → 청산 {pos.close_price:,.0f} "
              f"({s}{ret:.2f}%) "
              f"TP실현 {tp_pnl:+,.0f} | 최종 {s}{pos.total_pnl:,.0f}원 [{pos.close_reason}]")

    s = "+" if total_pnl >= 0 else ""
    print(f"  {'─'*60}")
    print(f"  총 손익: {s}{total_pnl:,.0f}원")
    print(f"  투자 수익률: {s}{total_pnl/total_inv*100:.2f}% (투자금 {total_inv/1e6:.2f}M)")
    print(f"  계좌 수익률: {s}{total_pnl/TOTAL_CASH*100:.2f}% (계좌 {TOTAL_CASH/1e6:.0f}M)")

    out = ROOT / "docs" / "trading_journal" / "simulation_2026-04-30.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    html = build_html(events, closed, all_bars)
    out.write_text(html, encoding="utf-8")
    print(f"\n[HTML] {out} ({len(html):,} bytes)")
