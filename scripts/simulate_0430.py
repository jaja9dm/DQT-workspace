"""
simulate_0430.py
2026-04-30 (목) DQT v2.3.0 시뮬레이션

실제 일봉 OHLCV (FinanceDataReader) + Brownian Bridge 1분봉 합성
→ 09:07 슬롯 배정 → 분 단위 진입/청산 시뮬레이션
→ docs/trading_journal/simulation_2026-04-30.html 생성

실행:
  cd /Users/dean/Documents/workspace-DQT
  source venv/bin/activate && python3 scripts/simulate_0430.py
"""
from __future__ import annotations

import math
import os
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, time as dtime
from pathlib import Path

# ── 경로 설정 ─────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    import FinanceDataReader as fdr
    import pandas as pd
except ImportError:
    print("FinanceDataReader 또는 pandas 미설치. venv 활성화 후 실행하세요.")
    sys.exit(1)

# ══════════════════════════════════════════════════════════════
# 1. 시뮬레이션 파라미터
# ══════════════════════════════════════════════════════════════
SIM_DATE        = "2026-04-30"
INVEST_PER_SLOT = 3_000_000       # 슬롯당 투자금 300만원
TOTAL_CASH      = 10_000_000      # 계좌 총 자산 1,000만원

# v2.3.0 트레일링 스톱 파라미터
TRAILING_INITIAL_STOP = 2.0       # 초기 손절선 -2%
TRAILING_TRIGGER      = 3.0       # 트레일링 상향 시작 +3%
TRAILING_FLOOR        = 2.5       # 트레일링 간격 -2.5%
TRAILING_TIGHT_FLOOR  = 1.25      # MACD sell_pre 시 절반으로 타이트

TAKE_PROFIT_1 = 5.0               # +5% → 1/3 매도 (MACD bearish) or 보류
TAKE_PROFIT_2 = 10.0              # +10% → 1/3 매도

BUY_CUTOFF_MIN = 180              # 09:00 기준 180분 = 12:00 → 신규 매수 마감

# 시장 시간 (분 인덱스, 0=09:00)
OPEN_MIN  = 0    # 09:00
CLOSE_MIN = 390  # 15:30

# 슬롯 배정 타이밍
SLOT_ASSIGN_MIN = 7   # 09:07

# 진입 시도 시작 타이밍 (슬롯 배정 + 오프닝 안정화)
ENTRY_START_MIN = 10  # 09:10


# ══════════════════════════════════════════════════════════════
# 2. 실제 April 30 후보 종목 (일봉 OHLCV 기반)
#    FDR 조회 + Brownian Bridge 1분봉 합성
# ══════════════════════════════════════════════════════════════
# 당일 실제 데이터:
#   Leader    : LG전자 (066570) — gap +3.3%, close +0.4% from open
#   Breakout  : 산일전기 (062040) — +19.6% intraday (연속 상한가 분위기)
#   Pullback  : 두산로보틱스 (454910) — gap +17.4%, then -8.2% from open (패턴)

CANDIDATES = [
    {
        "ticker": "066570",
        "name": "LG전자",
        "slot": "leader",
        "signal": "gap_up_breakout",
        "prev_close": 135800,
        "open": 140300,
        "high": 151700,
        "low": 138800,
        "close": 140900,
        "volume": 4_599_708,
        # 실제 패턴: 갭업 후 오전 11~12시경 고가(+8%), 이후 되돌려 종가 +0.4%
        "pattern": "gap_hold",
    },
    {
        "ticker": "062040",
        "name": "산일전기",
        "slot": "breakout",
        "signal": "volume_surge",
        "prev_close": 221000,
        "open": 222500,
        "high": 281500,
        "low": 218500,
        "close": 266000,
        "volume": 2_540_710,
        # 실제 패턴: 장 전반 강한 상승 추세, 오후 2시경 고가 후 소폭 조정해 종가
        "pattern": "surge",
    },
    {
        "ticker": "454910",
        "name": "두산로보틱스",
        "slot": "pullback",
        "signal": "gap_up_breakout",
        "prev_close": 102200,
        "open": 120000,
        "high": 120700,
        "low": 106400,
        "close": 110200,
        "volume": 1_839_947,
        # 실제 패턴: 갭업 오픈 직후 고가, 이후 지속 하락 → 저가 후 소폭 반등
        "pattern": "gap_fail",
    },
]

# ══════════════════════════════════════════════════════════════
# 3. Brownian Bridge 1분봉 생성
# ══════════════════════════════════════════════════════════════

def _bb_segment(start: float, end: float,
                hi: float, lo: float,
                n: int, seed: int, sigma_mult: float = 1.2) -> list[float]:
    """Brownian Bridge 단일 구간 — start → end, n개 포인트 (start 포함 안 함)."""
    if n <= 0:
        return []
    rng = random.Random(seed)
    log_range = math.log(hi / lo) if hi > lo else 0.01
    sigma = log_range / (2 * math.sqrt(n)) * sigma_mult

    increments = [rng.gauss(0, sigma) for _ in range(n)]
    W: list[float] = []
    s = 0.0
    for dx in increments:
        s += dx
        W.append(s)

    total_log = math.log(end / start) if start > 0 and end > 0 else 0.0
    pts = []
    for i in range(n):
        frac = (i + 1) / n
        bridge = W[i] - frac * W[-1]
        lp = math.log(max(start, 1)) + total_log * frac + bridge
        price = math.exp(lp)
        # strict clamp — no tolerance beyond declared hi/lo
        price = max(lo, min(hi, price))
        pts.append(price)
    if pts:
        pts[-1] = end
    return pts


def make_1min_path(open_p: float, close_p: float,
                   high_p: float, low_p: float,
                   n: int = 390, seed: int = 42,
                   pattern: str = "normal") -> list[float]:
    """
    Brownian Bridge로 open→close 1분봉 경로 생성.
    pattern:
      "normal"   — 기본 단일 구간
      "surge"    — 오프닝 직후 강한 상승 추세 (breakout 종목)
      "gap_hold" — 갭업 오픈 후 오전 고가 → 오후 되돌림 (leader)
      "gap_fail" — 갭업 오픈 직후 고가 → 지속 하락 (pullback 진입 대상)
    """
    # ── surge: 오픈 직후부터 꾸준한 강세 상승
    if pattern == "surge":
        # 오픈~60분: open → open*1.06 (강한 초기 상승, 하방은 open 이상 유지)
        early = open_p * 1.06
        p1 = _bb_segment(open_p, early, open_p * 1.08, open_p, 60, seed, 0.3)
        # 60분~300분: early → high
        p2 = _bb_segment(early, high_p, high_p, open_p, 240, seed + 1, 0.8)
        # 300분~390분: high → close
        p3 = _bb_segment(high_p, close_p, high_p, close_p * 0.97, 90, seed + 2, 0.9)
        return [open_p] + p1 + p2 + p3

    # ── gap_hold: 갭업 안정 → 오전 고가 → 오후 되돌림
    if pattern == "gap_hold":
        # 오픈~90분: open → mid (완만한 상승, 시가 아래로 안 감)
        mid = (open_p + high_p) / 2
        p1 = _bb_segment(open_p, mid, mid * 1.02, open_p, 90, seed, 0.7)
        # 90분~180분: mid → high
        p2 = _bb_segment(mid, high_p, high_p, open_p, 90, seed + 1, 0.9)
        # 180분~390분: high → close
        p3 = _bb_segment(high_p, close_p, high_p, low_p, 210, seed + 2, 1.1)
        return [open_p] + p1 + p2 + p3

    # ── gap_fail: 오픈 직후 고가 → 지속 하락 → 반등 종가
    if pattern == "gap_fail":
        # 오픈~10분: open → high (추가 급등)
        p1 = _bb_segment(open_p, high_p, high_p, open_p, 10, seed, 0.3)
        # 10분~320분: high → low (단조 하락, 상방 고가 이상 없음)
        p2 = _bb_segment(high_p, low_p, high_p, low_p, 310, seed + 1, 0.8)
        # 320분~390분: low → close (소폭 반등)
        p3 = _bb_segment(low_p, close_p, close_p * 1.01, low_p, 70, seed + 2, 0.8)
        return [open_p] + p1 + p2 + p3

    # ── normal: 기본 단일 Brownian Bridge
    return [open_p] + _bb_segment(open_p, close_p, high_p, low_p, n, seed, 1.4)


# ══════════════════════════════════════════════════════════════
# 4. 보조 지표 계산 (롤링 윈도우)
# ══════════════════════════════════════════════════════════════

def calc_rsi(prices: list[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains  = [max(d, 0) for d in deltas[-period:]]
    losses = [max(-d, 0) for d in deltas[-period:]]
    avg_g  = sum(gains) / period
    avg_l  = sum(losses) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))


def calc_macd(prices: list[float]) -> tuple[float, float, str]:
    """MACD 라인, 시그널, 신호 반환."""
    if len(prices) < 26:
        return 0.0, 0.0, "neutral"

    def ema(data: list[float], period: int) -> float:
        k = 2 / (period + 1)
        v = data[0]
        for x in data[1:]:
            v = x * k + v * (1 - k)
        return v

    ema12 = ema(prices[-26:], 12)
    ema26 = ema(prices[-26:], 26)
    macd_line = ema12 - ema26

    # 히스토그램 계산을 위해 이전 MACD도 계산
    if len(prices) >= 27:
        ema12_p = ema(prices[-27:-1], 12)
        ema26_p = ema(prices[-27:-1], 26)
        macd_prev = ema12_p - ema26_p
    else:
        macd_prev = macd_line

    signal_line = ema([macd_line], 9) if len(prices) < 35 else ema(
        [ema(prices[i-26:i], 12) - ema(prices[i-26:i], 26)
         for i in range(26, len(prices))],
        9
    )

    hist = macd_line - signal_line
    hist_prev = macd_prev - signal_line

    if hist > 0 and hist > hist_prev:
        sig = "buy"
    elif hist > 0:
        sig = "buy_pre"
    elif hist < 0 and hist < hist_prev:
        sig = "sell"
    else:
        sig = "sell_pre"

    return macd_line, signal_line, sig


def calc_volume_ratio(volumes: list[float]) -> float:
    if len(volumes) < 2:
        return 1.0
    avg = sum(volumes[:-1]) / len(volumes[:-1])
    return volumes[-1] / avg if avg > 0 else 1.0


# ══════════════════════════════════════════════════════════════
# 5. 게이트 / 진입 체크
# ══════════════════════════════════════════════════════════════

def check_entry_gates(stock: dict, prices: list[float],
                      min_idx: int, slot: str) -> tuple[bool, str]:
    """
    v2.3.0 5-gate 진입 체크 (시뮬레이션 간소화).
    Gate 1: RSI 하드 실패 (>85)
    Gate 2: 체결강도 / 거래량비 (시뮬에서는 추정)
    Gate 3: MACD 방향
    Gate 4: 시간 (09:10~12:00)
    Gate 5: 진입 점수 ≥ 50
    Returns (ok, reason)
    """
    if len(prices) < 2:
        return False, "데이터 부족"

    rsi  = calc_rsi(prices)
    _, _, macd_sig = calc_macd(prices)

    # ── Gate 1: RSI 하드 실패 ──────────────────────────────
    gap_pct = (stock["open"] - stock["prev_close"]) / stock["prev_close"] * 100
    rsi_hard_fail = 90 if gap_pct >= 5 else 85
    if rsi > rsi_hard_fail:
        return False, f"RSI {rsi:.0f} > {rsi_hard_fail} (하드 실패)"

    # ── Gate 2: 시간 게이트 ────────────────────────────────
    if min_idx < ENTRY_START_MIN:
        return False, f"09:10 이전 진입 차단 ({min_idx_to_time(min_idx)})"
    if min_idx >= BUY_CUTOFF_MIN:
        return False, "12:00 이후 매수 마감"

    # pullback은 14:00 이전만
    if slot == "pullback" and min_idx >= 300:
        return False, "pullback 14:00 이후 차단"

    # ── Gate 3: MACD 방향 ──────────────────────────────────
    if macd_sig in ("sell", "sell_pre") and slot != "pullback":
        return False, f"MACD {macd_sig} — 진입 차단"

    # ── Gate 4: 갭업 슬롯은 MACD buy_pre/buy 필요 (11:00 이후) ──
    if slot == "leader" and min_idx >= 120 and macd_sig not in ("buy_pre", "buy"):
        return False, f"11:00 이후 leader MACD {macd_sig} 불충분"

    # ── Gate 5: 진입 점수 ──────────────────────────────────
    score = _entry_score(prices, stock, rsi, macd_sig, min_idx)
    if score < 50:
        return False, f"진입점수 {score}pt < 50 차단"

    ratio = 1.0 if score >= 72 else 0.75
    return True, f"진입 OK (점수 {score}pt, {ratio*100:.0f}% 투입)"


def _entry_score(prices: list[float], stock: dict,
                 rsi: float, macd_sig: str, min_idx: int) -> int:
    """간소화 진입 점수 계산 (max 100)."""
    score = 0

    # 거래량 점수 (0-30)
    # 시뮬에서는 gap 크기로 추정
    gap_pct = abs((stock["open"] - stock["prev_close"]) / stock["prev_close"] * 100)
    vol_score = min(30, int(gap_pct * 3))
    score += vol_score

    # RSI 점수 (0-20)
    if 50 <= rsi <= 70:
        score += 20
    elif 45 <= rsi < 50 or 70 < rsi <= 78:
        score += 12
    elif 40 <= rsi < 45 or 78 < rsi <= 83:
        score += 5

    # OBV 점수 (0-20) — 현재가 vs 시가 방향으로 추정
    cur = prices[-1]
    if cur >= stock["open"]:
        score += 20
    elif cur >= stock["open"] * 0.98:
        score += 10

    # StochRSI 점수 (0-15) — RSI로 대체
    if rsi <= 80:
        score += 15
    elif rsi <= 85:
        score += 8

    # MACD 점수 (0-15)
    if macd_sig == "buy":
        score += 15
    elif macd_sig == "buy_pre":
        score += 10
    elif macd_sig == "neutral":
        score += 5

    return min(100, score)


# ══════════════════════════════════════════════════════════════
# 6. 포지션 관리 클래스
# ══════════════════════════════════════════════════════════════

@dataclass
class Position:
    ticker: str
    name: str
    slot: str
    entry_price: float
    quantity: int
    invested: float
    entry_min: int              # 진입 분 인덱스

    stop_loss: float = 0.0      # 현재 손절선 가격
    peak_price: float = 0.0     # 최고가 (트레일링용)
    tp1_done: bool = False       # 1차 익절 완료
    tp2_done: bool = False       # 2차 익절 완료
    is_closed: bool = False
    close_price: float = 0.0
    close_min: int = 0
    close_reason: str = ""
    pnl: float = 0.0

    def __post_init__(self):
        self.stop_loss = self.entry_price * (1 - TRAILING_INITIAL_STOP / 100)
        self.peak_price = self.entry_price


# ══════════════════════════════════════════════════════════════
# 7. 이벤트 로그
# ══════════════════════════════════════════════════════════════

@dataclass
class Event:
    min_idx: int
    ticker: str
    name: str
    slot: str
    event_type: str   # "BUY" / "SELL" / "STOP" / "TP1" / "TP2" / "TIMECUT" / "GATE_FAIL"
    price: float
    detail: str
    pnl: float = 0.0


# ══════════════════════════════════════════════════════════════
# 8. 분 인덱스 → 시각 변환
# ══════════════════════════════════════════════════════════════

def min_idx_to_time(m: int) -> str:
    h = 9 + m // 60
    mi = m % 60
    return f"{h:02d}:{mi:02d}"


# ══════════════════════════════════════════════════════════════
# 9. 메인 시뮬레이션
# ══════════════════════════════════════════════════════════════

def run_simulation() -> tuple[list[Event], list[Position], list[dict]]:
    print(f"[시뮬레이션] {SIM_DATE} 시작")

    # 1분봉 경로 생성
    price_paths: dict[str, list[float]] = {}
    for s in CANDIDATES:
        path = make_1min_path(
            open_p=s["open"],
            close_p=s["close"],
            high_p=s["high"],
            low_p=s["low"],
            n=CLOSE_MIN,
            seed=int(s["ticker"]),
            pattern=s.get("pattern", "normal"),
        )
        price_paths[s["ticker"]] = path
        print(f"  {s['name']}({s['ticker']}): {len(path)-1}분봉 생성 "
              f"시가={s['open']:,} 종가={s['close']:,}")

    events: list[Event] = []
    positions: dict[str, Position] = {}
    closed: list[Position] = []

    # 슬롯별 진입 완료 여부
    slot_filled: dict[str, bool] = {s["slot"]: False for s in CANDIDATES}
    reentry_count: dict[str, int] = {}  # 재진입 횟수

    # 09:07 슬롯 배정 이벤트
    for stock in CANDIDATES:
        gap_pct = (stock["open"] - stock["prev_close"]) / stock["prev_close"] * 100
        events.append(Event(
            min_idx=SLOT_ASSIGN_MIN,
            ticker=stock["ticker"],
            name=stock["name"],
            slot=stock["slot"],
            event_type="SLOT",
            price=price_paths[stock["ticker"]][SLOT_ASSIGN_MIN],
            detail=f"슬롯 배정: {stock['slot'].upper()} | 갭 {gap_pct:+.1f}% | {stock['signal']}",
        ))

    # 분 단위 루프
    for m in range(CLOSE_MIN + 1):
        t = min_idx_to_time(m)

        for stock in CANDIDATES:
            ticker = stock["ticker"]
            slot   = stock["slot"]
            path   = price_paths[ticker]
            if m >= len(path):
                continue
            cur_price = path[m]
            hist      = path[:m+1]

            pos = positions.get(ticker)

            # ── 포지션 없음: 진입 체크 ──────────────────────
            if pos is None and not slot_filled[slot]:
                ok, reason = check_entry_gates(stock, hist, m, slot)
                if not ok:
                    if m % 30 == 0 and m >= ENTRY_START_MIN:  # 30분마다 로그
                        events.append(Event(
                            min_idx=m, ticker=ticker, name=stock["name"],
                            slot=slot, event_type="GATE_FAIL",
                            price=cur_price, detail=reason,
                        ))
                else:
                    qty = max(1, int(INVEST_PER_SLOT / cur_price))
                    invested = qty * cur_price
                    pos = Position(
                        ticker=ticker, name=stock["name"], slot=slot,
                        entry_price=cur_price, quantity=qty,
                        invested=invested, entry_min=m,
                    )
                    positions[ticker] = pos
                    slot_filled[slot] = True
                    _, _, msig = calc_macd(hist)
                    rsi = calc_rsi(hist)
                    events.append(Event(
                        min_idx=m, ticker=ticker, name=stock["name"],
                        slot=slot, event_type="BUY",
                        price=cur_price,
                        detail=f"매수 {qty:,}주 × {cur_price:,.0f}원 = {invested:,.0f}원 | RSI={rsi:.0f} MACD={msig}",
                    ))

            # ── 포지션 있음: 감시 ────────────────────────────
            elif pos is not None and not pos.is_closed:
                pnl_pct = (cur_price - pos.entry_price) / pos.entry_price * 100
                _, _, msig = calc_macd(hist)

                # 피크 업데이트
                if cur_price > pos.peak_price:
                    pos.peak_price = cur_price

                # 트레일링 스톱 업데이트
                if pnl_pct >= TRAILING_TRIGGER:
                    floor = TRAILING_TIGHT_FLOOR if msig in ("sell_pre", "sell") else TRAILING_FLOOR
                    new_stop = pos.peak_price * (1 - floor / 100)
                    if new_stop > pos.stop_loss:
                        pos.stop_loss = new_stop

                # 손절 체크
                if cur_price <= pos.stop_loss:
                    pnl = (cur_price - pos.entry_price) * pos.quantity
                    pos.is_closed = True
                    pos.close_price = cur_price
                    pos.close_min = m
                    pos.close_reason = "STOP"
                    pos.pnl = pnl
                    closed.append(pos)
                    del positions[ticker]
                    events.append(Event(
                        min_idx=m, ticker=ticker, name=stock["name"],
                        slot=slot, event_type="STOP",
                        price=cur_price,
                        detail=f"손절 {pnl_pct:+.1f}% | 손익 {pnl:+,.0f}원",
                        pnl=pnl,
                    ))

                # 1차 익절 (+5%)
                elif not pos.tp1_done and pnl_pct >= TAKE_PROFIT_1:
                    if msig in ("sell_pre", "sell"):
                        sell_qty = pos.quantity // 3
                        if sell_qty > 0:
                            pnl_partial = (cur_price - pos.entry_price) * sell_qty
                            pos.quantity -= sell_qty
                            pos.tp1_done = True
                            events.append(Event(
                                min_idx=m, ticker=ticker, name=stock["name"],
                                slot=slot, event_type="TP1",
                                price=cur_price,
                                detail=f"1차 익절 1/3({sell_qty}주) MACD={msig} | 부분손익 {pnl_partial:+,.0f}원",
                                pnl=pnl_partial,
                            ))
                    else:
                        # MACD 강세 → 보류, 손절선 매수가+1% 이상 상향
                        lock_stop = pos.entry_price * 1.01
                        if lock_stop > pos.stop_loss:
                            pos.stop_loss = lock_stop
                        pos.tp1_done = True
                        events.append(Event(
                            min_idx=m, ticker=ticker, name=stock["name"],
                            slot=slot, event_type="TP1",
                            price=cur_price,
                            detail=f"+{pnl_pct:.1f}% 달성 — MACD {msig} 강세, 익절 보류 / 손절선 상향 → {lock_stop:,.0f}원",
                        ))

                # 2차 익절 (+10%)
                elif not pos.tp2_done and pnl_pct >= TAKE_PROFIT_2:
                    if msig in ("sell_pre", "sell"):
                        sell_qty = pos.quantity // 2
                        if sell_qty > 0:
                            pnl_partial = (cur_price - pos.entry_price) * sell_qty
                            pos.quantity -= sell_qty
                            pos.tp2_done = True
                            events.append(Event(
                                min_idx=m, ticker=ticker, name=stock["name"],
                                slot=slot, event_type="TP2",
                                price=cur_price,
                                detail=f"2차 익절 1/2({sell_qty}주) MACD={msig} | 부분손익 {pnl_partial:+,.0f}원",
                                pnl=pnl_partial,
                            ))
                    else:
                        pos.tp2_done = True
                        events.append(Event(
                            min_idx=m, ticker=ticker, name=stock["name"],
                            slot=slot, event_type="TP2",
                            price=cur_price,
                            detail=f"+{pnl_pct:.1f}% 달성 — MACD {msig} 강세, 익절 보류",
                        ))

        # 15:20 타임컷 — 모든 잔여 포지션 청산
        if m == 380:
            for ticker, pos in list(positions.items()):
                if not pos.is_closed:
                    cur_price = price_paths[ticker][min(m, len(price_paths[ticker])-1)]
                    pnl = (cur_price - pos.entry_price) * pos.quantity
                    pos.is_closed = True
                    pos.close_price = cur_price
                    pos.close_min = m
                    pos.close_reason = "TIMECUT"
                    pos.pnl = pnl
                    closed.append(pos)
                    events.append(Event(
                        min_idx=m,
                        ticker=pos.ticker, name=pos.name, slot=pos.slot,
                        event_type="TIMECUT",
                        price=cur_price,
                        detail=f"15:20 타임컷 전량 청산 | 손익 {pnl:+,.0f}원",
                        pnl=pnl,
                    ))
            positions.clear()

    # 미청산 잔여 포지션 강제 마감 (15:30)
    for ticker, pos in list(positions.items()):
        m = CLOSE_MIN
        cur_price = price_paths[ticker][-1]
        pnl = (cur_price - pos.entry_price) * pos.quantity
        pos.is_closed = True
        pos.close_price = cur_price
        pos.close_min = m
        pos.close_reason = "CLOSE"
        pos.pnl = pnl
        closed.append(pos)
        events.append(Event(
            min_idx=m,
            ticker=pos.ticker, name=pos.name, slot=pos.slot,
            event_type="TIMECUT",
            price=cur_price,
            detail=f"장 마감 청산 | 손익 {pnl:+,.0f}원",
            pnl=pnl,
        ))

    return events, closed, price_paths


# ══════════════════════════════════════════════════════════════
# 10. HTML 시각화 생성
# ══════════════════════════════════════════════════════════════

EVENT_COLORS = {
    "SLOT":      "#4a9eff",
    "GATE_FAIL": "#666",
    "BUY":       "#26a69a",
    "TP1":       "#66bb6a",
    "TP2":       "#aed581",
    "STOP":      "#ef5350",
    "TIMECUT":   "#ff7043",
    "SELL":      "#ff7043",
}

SLOT_LABELS = {
    "leader":   "🏆 Leader (갭업돌파)",
    "breakout": "💥 Breakout (거래량폭발)",
    "pullback": "📉 Pullback (눌림반등)",
}

SLOT_COLORS = {
    "leader":   "#1e3a5f",
    "breakout": "#1e4a2e",
    "pullback": "#3a1e4a",
}


def _price_path_svg(path: list[float], entry_min: int | None,
                    events_for_ticker: list[Event],
                    width: int = 800, height: int = 80) -> str:
    """종목별 미니 가격 경로 SVG."""
    pts = path[:CLOSE_MIN+1]
    if not pts:
        return ""
    mn, mx = min(pts), max(pts)
    rng = mx - mn if mx > mn else 1.0

    def px(v: float) -> float:
        return width - width * (v - mn) / rng  # 뒤집어서 Y축 정방향

    def py(v: float) -> float:
        pad = 8
        return pad + (height - 2*pad) * (1 - (v - mn) / rng)

    n = len(pts)
    xs = [i / (n - 1) * width for i in range(n)]

    # polyline 포인트
    points = " ".join(f"{xs[i]:.1f},{py(pts[i]):.1f}" for i in range(n))

    # 이벤트 마커
    markers = []
    for ev in events_for_ticker:
        if ev.event_type in ("GATE_FAIL", "SLOT"):
            continue
        if ev.min_idx < n:
            x = ev.min_idx / (CLOSE_MIN) * width
            y = py(ev.price)
            color = EVENT_COLORS.get(ev.event_type, "#fff")
            label = ev.event_type
            markers.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" '
                f'stroke="#0f1117" stroke-width="1">'
                f'<title>{min_idx_to_time(ev.min_idx)} {label}: {ev.price:,.0f}원</title>'
                f'</circle>'
            )

    # 손절선 (entry 이후 트레일링은 복잡하므로 초기 손절선만 표시)
    stop_line = ""
    buy_events = [e for e in events_for_ticker if e.event_type == "BUY"]
    if buy_events:
        ep = buy_events[0].price
        sl = ep * (1 - TRAILING_INITIAL_STOP / 100)
        sl_y = py(sl)
        stop_line = (
            f'<line x1="0" y1="{sl_y:.1f}" x2="{width}" y2="{sl_y:.1f}" '
            f'stroke="#ef5350" stroke-width="0.8" stroke-dasharray="3,3" opacity="0.7"/>'
        )

    # 매수가 수평선
    entry_line = ""
    if buy_events:
        ep = buy_events[0].price
        ep_y = py(ep)
        entry_line = (
            f'<line x1="0" y1="{ep_y:.1f}" x2="{width}" y2="{ep_y:.1f}" '
            f'stroke="#26a69a" stroke-width="0.8" stroke-dasharray="4,2" opacity="0.8"/>'
        )

    # 12:00 컷 수직선
    cutoff_x = BUY_CUTOFF_MIN / CLOSE_MIN * width
    cutoff_line = (
        f'<line x1="{cutoff_x:.1f}" y1="0" x2="{cutoff_x:.1f}" y2="{height}" '
        f'stroke="#ffd54f" stroke-width="0.8" stroke-dasharray="3,3" opacity="0.5"/>'
    )

    svg = f'''<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:{height}px;display:block;">
  <rect width="{width}" height="{height}" fill="#12141f" rx="4"/>
  {cutoff_line}
  {stop_line}
  {entry_line}
  <polyline points="{points}" fill="none" stroke="#4a9eff" stroke-width="1.5"/>
  {"".join(markers)}
</svg>'''
    return svg


def build_html(events: list[Event], closed: list[Position],
               price_paths: dict[str, list[float]]) -> str:
    # 결과 요약
    total_pnl = sum(p.pnl for p in closed)
    total_invested = sum(p.invested for p in closed)
    ret_pct = total_pnl / total_invested * 100 if total_invested else 0

    # 종목별 이벤트 분류
    ticker_events: dict[str, list[Event]] = {}
    for ev in events:
        ticker_events.setdefault(ev.ticker, []).append(ev)

    # 타임라인 HTML (슬롯별)
    slot_sections = []
    for stock in CANDIDATES:
        ticker = stock["ticker"]
        slot   = stock["slot"]
        path   = price_paths[ticker]
        evs    = ticker_events.get(ticker, [])

        closed_pos = next((p for p in closed if p.ticker == ticker), None)
        pnl = closed_pos.pnl if closed_pos else 0.0
        entry_p = closed_pos.entry_price if closed_pos else 0.0
        close_p = closed_pos.close_price if closed_pos else 0.0
        pnl_pct = (close_p - entry_p) / entry_p * 100 if entry_p else 0.0
        pnl_color = "#26a69a" if pnl >= 0 else "#ef5350"
        pnl_sign = "+" if pnl >= 0 else ""

        svg = _price_path_svg(path, None, evs)

        # 이벤트 타임라인 rows
        timeline_rows = []
        for ev in evs:
            if ev.event_type == "GATE_FAIL" and ev.min_idx % 30 != 0:
                continue
            color = EVENT_COLORS.get(ev.event_type, "#aaa")
            icon  = {"BUY":"🟢","STOP":"🔴","TP1":"🟡","TP2":"💛",
                     "TIMECUT":"⏱","SELL":"🔶","SLOT":"📌","GATE_FAIL":"🚫"}.get(ev.event_type,"•")
            pnl_cell = ""
            if ev.pnl != 0:
                pc = "#26a69a" if ev.pnl >= 0 else "#ef5350"
                pnl_cell = f'<span style="color:{pc};font-weight:700">{pnl_sign if ev.pnl>=0 else ""}{ev.pnl:,.0f}원</span>'
            timeline_rows.append(
                f'<tr>'
                f'<td style="color:#aaa;white-space:nowrap;padding:3px 8px">{min_idx_to_time(ev.min_idx)}</td>'
                f'<td style="color:{color};padding:3px 8px">{icon} {ev.event_type}</td>'
                f'<td style="padding:3px 8px">{ev.price:,.0f}원</td>'
                f'<td style="padding:3px 8px;color:#ccc">{ev.detail}</td>'
                f'<td style="padding:3px 8px">{pnl_cell}</td>'
                f'</tr>'
            )

        slot_bg  = SLOT_COLORS.get(slot, "#1a1a2e")
        slot_lbl = SLOT_LABELS.get(slot, slot)
        section = f'''
<div style="background:{slot_bg};border:1px solid #2a2d3e;border-radius:8px;margin-bottom:24px;overflow:hidden">
  <div style="padding:12px 16px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #2a2d3e">
    <div>
      <span style="font-size:0.8em;color:#8892b0;letter-spacing:1px">{slot_lbl}</span><br>
      <span style="font-size:1.2em;font-weight:700;color:#e6edf3">{stock["name"]}</span>
      <span style="color:#666;margin-left:8px">{ticker}</span>
    </div>
    <div style="text-align:right">
      <div style="color:#aaa;font-size:0.82em">시가 {stock["open"]:,} → 종가 {stock["close"]:,}원</div>
      <div style="font-size:1.1em;font-weight:700;color:{pnl_color}">{pnl_sign}{pnl:,.0f}원 ({pnl_sign}{pnl_pct:.2f}%)</div>
    </div>
  </div>
  <div style="padding:8px 16px">{svg}</div>
  <div style="padding:8px 16px;overflow-x:auto">
    <table style="width:100%;border-collapse:collapse;font-size:0.85em;color:#ccd6f6">
      <thead>
        <tr style="color:#8892b0;border-bottom:1px solid #2a2d3e">
          <th style="text-align:left;padding:4px 8px">시각</th>
          <th style="text-align:left;padding:4px 8px">이벤트</th>
          <th style="text-align:left;padding:4px 8px">가격</th>
          <th style="text-align:left;padding:4px 8px">상세</th>
          <th style="text-align:left;padding:4px 8px">손익</th>
        </tr>
      </thead>
      <tbody>{"".join(timeline_rows)}</tbody>
    </table>
  </div>
</div>'''
        slot_sections.append(section)

    # 전체 요약 카드
    summary_cards = f'''
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:28px">
  <div style="background:#1e3a5f;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.78em">시뮬레이션 날짜</div>
    <div style="font-size:1.1em;font-weight:700;color:#e6edf3">2026-04-30 (목)</div>
  </div>
  <div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.78em">총 손익</div>
    <div style="font-size:1.2em;font-weight:700;color:{"#26a69a" if total_pnl>=0 else "#ef5350"}">
      {"+" if total_pnl>=0 else ""}{total_pnl:,.0f}원
    </div>
  </div>
  <div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.78em">수익률</div>
    <div style="font-size:1.2em;font-weight:700;color:{"#26a69a" if ret_pct>=0 else "#ef5350"}">
      {"+" if ret_pct>=0 else ""}{ret_pct:.2f}%
    </div>
  </div>
  <div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.78em">총 거래 슬롯</div>
    <div style="font-size:1.2em;font-weight:700;color:#ccd6f6">{len(closed)}개</div>
  </div>
  <div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:14px 16px">
    <div style="color:#8892b0;font-size:0.78em">DQT 버전</div>
    <div style="font-size:1.1em;font-weight:700;color:#8892b0">v2.3.0</div>
  </div>
</div>'''

    # 범례
    legend = '''
<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px;font-size:0.82em;color:#8892b0">
  <span>🟢 BUY 매수</span>
  <span>🔴 STOP 손절</span>
  <span>🟡 TP1 1차익절(+5%)</span>
  <span>💛 TP2 2차익절(+10%)</span>
  <span>⏱ TIMECUT 타임컷</span>
  <span>🚫 GATE_FAIL 게이트차단</span>
  <span style="color:#ffd54f">— 12:00 매수마감</span>
  <span style="color:#26a69a">— 매수가</span>
  <span style="color:#ef5350">--- 초기 손절선(-2%)</span>
</div>'''

    # 가정 및 방법론 노트
    methodology = '''
<div style="background:#12141f;border:1px solid #2a2d3e;border-radius:8px;padding:16px 20px;margin-top:24px;font-size:0.83em;color:#8892b0;line-height:1.7">
  <div style="color:#ccd6f6;font-weight:600;margin-bottom:8px">📋 시뮬레이션 가정 및 방법론</div>
  <ul style="margin:0;padding-left:20px">
    <li><b>가격 경로</b>: FinanceDataReader 실제 일봉(시가·고가·저가·종가) + Brownian Bridge 1분봉 합성</li>
    <li><b>슬롯 배정</b>: 09:07 기준 — Leader(갭업돌파) / Breakout(거래량폭발) / Pullback(눌림반등)</li>
    <li><b>진입 조건</b>: RSI ≤85 / MACD 방향 / 진입점수 ≥50pt / 09:10~12:00 시간 게이트</li>
    <li><b>트레일링 스톱</b>: 초기 -2% → +3% 수익 시 상향, 피크 대비 -2.5% 유지 (MACD sell_pre 시 -1.25%)</li>
    <li><b>분할 익절</b>: +5% 달성 시 MACD bearish이면 1/3 매도, bullish이면 보류 + 손절선 상향</li>
    <li><b>타임컷</b>: 15:20 전량 청산</li>
    <li><b>슬롯당 투자금</b>: 300만원 (총 계좌 1,000만원의 30%)</li>
    <li><b>주의</b>: 실제 거래는 DB hot list + KIS API 실시간 데이터 기반. 이 시뮬레이션은 합성 경로로 방향성 검증용</li>
  </ul>
</div>'''

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = f'''<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>DQT 시뮬레이션 — 2026-04-30</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f1117; color: #ccd6f6; font-family: "Pretendard", "Noto Sans KR", sans-serif; padding: 24px; max-width: 1100px; margin: 0 auto; }}
  h1 {{ font-size: 1.5em; font-weight: 700; color: #e6edf3; margin-bottom: 6px; }}
  .sub {{ color: #8892b0; font-size: 0.85em; margin-bottom: 24px; }}
  table td, table th {{ vertical-align: top; }}
</style>
</head>
<body>
<h1>DQT 시뮬레이션 — 2026-04-30 (목)</h1>
<div class="sub">v2.3.0 로직 기준 | Brownian Bridge 합성 가격 경로 | 생성: {now_str}</div>

{summary_cards}
{legend}
{"".join(slot_sections)}
{methodology}
</body>
</html>'''
    return html


# ══════════════════════════════════════════════════════════════
# 11. 실행 진입점
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")

    events, closed, price_paths = run_simulation()

    total_pnl = sum(p.pnl for p in closed)
    print(f"\n[결과 요약]")
    for p in closed:
        sign = "+" if p.pnl >= 0 else ""
        pnl_pct = (p.close_price - p.entry_price) / p.entry_price * 100 if p.entry_price else 0
        print(f"  {p.slot:12s} {p.name}({p.ticker}): "
              f"매수 {p.entry_price:,.0f} → 청산 {p.close_price:,.0f} "
              f"({sign}{pnl_pct:.1f}%) {sign}{p.pnl:,.0f}원 [{p.close_reason}]")
    sign = "+" if total_pnl >= 0 else ""
    print(f"  ─────────────────────────────────────────────")
    print(f"  총 손익: {sign}{total_pnl:,.0f}원")

    out_path = ROOT / "docs" / "trading_journal" / "simulation_2026-04-30.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    html = build_html(events, closed, price_paths)
    out_path.write_text(html, encoding="utf-8")
    print(f"\n[HTML] {out_path} 생성 완료 ({len(html):,} bytes)")
