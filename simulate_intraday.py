"""
simulate_intraday.py
오늘(2026-04-13) hot list 기반 장중 시뮬레이션

- 9:00 시가 매수 → 장 중 가격 움직임 시뮬레이션
- 손절(-5%) / 1차익절(+5%) / 2차익절(+10%) / 트레일링스톱 / 타임컷 자동 실행
- 각 체결 이벤트마다 텔레그램 알림
- 78개 봉(5분봉) → 봉당 1.5초 (총 약 2분으로 압축)

실행:
  python3 simulate_intraday.py
"""
from __future__ import annotations

import math
import random
import sys
import time
from datetime import datetime


# ── 설정 ─────────────────────────────────────────────────────
INTERVAL_SLEEP   = 1.5      # 5분봉 1개 = 1.5초 실제
N_INTERVALS      = 78       # 9:00~15:30 = 390분 / 5분
INVEST_PER_STOCK = 10_000_000  # 종목당 투자금 1,000만원

# 포지션 감시 파라미터
TRAILING_INITIAL_STOP = 5.0    # 초기 손절선 -5%
TRAILING_TRIGGER      = 10.0   # 트레일링 상향 시작 수익 +10%
TRAILING_FLOOR        = 5.0    # 트레일링 간격 5%
TAKE_PROFIT_1         = 5.0    # 1차 익절 +5%  → 1/3 매도
TAKE_PROFIT_2         = 10.0   # 2차 익절 +10% → 1/3 매도

# 오늘(2026-04-13) 실제 OHLCV + hot list
HOT_LIST = [
    {"ticker": "217590", "name": "티엠씨",     "open": 25100,  "high": 31450,  "low": 24450,  "close": 31450,  "signal": "momentum",    "reason": "가격30%+RSI63+MACD강세"},
    {"ticker": "018470", "name": "조일알미늄", "open":  1600,  "high":  1889,  "low":  1568,  "close":  1831,  "signal": "volume_surge", "reason": "거래량3.3배+가격22.2%+RSI73"},
    {"ticker": "257720", "name": "실리콘투",   "open": 42000,  "high": 46850,  "low": 41950,  "close": 46400,  "signal": "volume_surge", "reason": "거래량급증+가격9.3%+BB돌파"},
    {"ticker": "010820", "name": "퍼스텍",     "open": 11800,  "high": 11920,  "low": 10810,  "close": 11490,  "signal": "momentum",     "reason": "MACD강세+BB돌파+RSI74"},
    {"ticker": "009150", "name": "삼성전기",   "open": 555000, "high": 579000, "low": 554000, "close": 565000, "signal": "momentum",     "reason": "MACD히스토최대+BB돌파+RSI72"},
]

SIGNAL_EMOJI = {
    "momentum":    "🚀",
    "volume_surge":"📊",
    "breakout":    "💥",
}


# ── 텔레그램 ─────────────────────────────────────────────────
def _notify(text: str) -> None:
    try:
        from src.utils.notifier import notify
        notify(text)
    except Exception as e:
        print(f"  [텔레그램 실패] {e}")


# ── Brownian Bridge 가격 경로 생성 ───────────────────────────
def make_price_path(open_p: float, close_p: float,
                    high_p: float, low_p: float,
                    n: int, seed: int) -> list[float]:
    """
    Brownian Bridge로 open→close 경로 생성.
    실제 high/low 범위 안에서 자연스러운 움직임.
    """
    rng = random.Random(seed)

    log_range = math.log(high_p / low_p)
    sigma = log_range / (2 * math.sqrt(n)) * 1.6   # 실제 변동성 반영

    # 누적 랜덤워크
    increments = [rng.gauss(0, sigma) for _ in range(n)]
    W = []
    s = 0.0
    for dx in increments:
        s += dx
        W.append(s)

    total_log = math.log(close_p / open_p)

    # Bridge: W(t) - t/n * W(n) → 0으로 수렴
    path = [open_p]
    for i in range(n):
        frac = (i + 1) / n
        bridge = W[i] - frac * W[-1]
        lp = math.log(open_p) + total_log * frac + bridge
        price = math.exp(lp)
        # 실제 범위 내 클리핑 (약간 여유 허용)
        price = max(low_p * 0.98, min(high_p * 1.02, price))
        path.append(price)

    path[-1] = close_p  # 종가 고정
    return path


# ── 포지션 클래스 ────────────────────────────────────────────
class Position:
    def __init__(self, stock: dict):
        self.ticker   = stock["ticker"]
        self.name     = stock["name"]
        self.signal   = stock["signal"]
        self.reason   = stock["reason"]

        entry = stock["open"]
        qty   = max(1, int(INVEST_PER_STOCK / entry))

        self.entry_price     = entry
        self.quantity        = qty
        self.partial_sold    = 0   # 익절 횟수
        self.trailing_floor  = entry * (1 - TRAILING_INITIAL_STOP / 100)
        self.highest_price   = entry
        self.sold            = False
        self.pnl_pct         = 0.0
        self.events: list[str] = []

    @property
    def invest_amt(self) -> int:
        return int(self.entry_price * self.quantity)

    def current_pnl(self, price: float) -> float:
        return (price / self.entry_price - 1) * 100

    def update_trailing(self, price: float) -> None:
        """트레일링 손절선 업데이트 (수익 >= TRIGGER% 시 상향)."""
        self.highest_price = max(self.highest_price, price)
        gain = self.current_pnl(price)
        if gain >= TRAILING_TRIGGER:
            candidate = price * (1 - TRAILING_FLOOR / 100)
            if candidate > self.trailing_floor:
                self.trailing_floor = candidate


# ── 시간 포맷 ────────────────────────────────────────────────
def sim_time(step: int) -> str:
    """step 0 = 09:00, step 77 = 15:25"""
    minutes = 9 * 60 + step * 5
    h, m = divmod(minutes, 60)
    return f"{h:02d}:{m:02d}"


# ── 메인 시뮬레이션 ──────────────────────────────────────────
def main():
    print("=" * 60)
    print("  DQT 장중 시뮬레이션 — 2026-04-13")
    print(f"  종목: {len(HOT_LIST)}개  |  봉당 {INTERVAL_SLEEP}초  |  총 {N_INTERVALS}봉")
    print("=" * 60)

    # 가격 경로 미리 생성
    paths: dict[str, list[float]] = {}
    for i, s in enumerate(HOT_LIST):
        paths[s["ticker"]] = make_price_path(
            s["open"], s["close"], s["high"], s["low"], N_INTERVALS, seed=42 + i
        )

    # 포지션 초기화
    positions = [Position(s) for s in HOT_LIST]

    # ── 매수 알림 ────────────────────────────────────────────
    buy_lines = ["🔔 <b>[DQT 시뮬] 09:00 매수 체결</b>", ""]
    for pos in positions:
        emoji = SIGNAL_EMOJI.get(pos.signal, "⚡")
        buy_lines.append(
            f"{emoji} <b>{pos.ticker} {pos.name}</b>  [{pos.signal}]\n"
            f"   매수가 {pos.entry_price:,.0f}원  {pos.quantity:,}주  (투자금 {pos.invest_amt:,.0f}원)\n"
            f"   신호: {pos.reason}\n"
            f"   초기손절선 {pos.trailing_floor:,.0f}원 (-{TRAILING_INITIAL_STOP:.0f}%)"
        )
    buy_lines += [
        "",
        f"💰 총 투자금: {sum(p.invest_amt for p in positions):,.0f}원",
        f"<i>실제 장중 OHLCV 기반 Brownian Bridge 시뮬레이션</i>",
    ]
    print("\n".join(buy_lines).replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>",""))
    _notify("\n".join(buy_lines))

    # ── 장중 루프 ────────────────────────────────────────────
    active_positions = list(positions)
    last_status_step = -1  # 30분마다 상태 알림

    for step in range(N_INTERVALS):
        t = sim_time(step)
        time.sleep(INTERVAL_SLEEP)

        events_this_step = []

        for pos in list(active_positions):
            if pos.sold:
                continue

            price = paths[pos.ticker][step + 1]
            pos.update_trailing(price)
            pnl = pos.current_pnl(price)

            # ── 트레일링 스톱 발동 ────────────────────────────
            if price <= pos.trailing_floor:
                qty_sell = pos.quantity
                pos.sold = True
                pos.pnl_pct = pnl
                active_positions.remove(pos)

                action_type = "손절" if pnl < 0 else "트레일링스톱"
                emoji = "🔴" if pnl < 0 else "🔻"
                msg = (
                    f"{emoji} <b>[{action_type}] {pos.ticker} {pos.name}</b>\n"
                    f"   현재가 {price:,.0f}원 ≤ 손절선 {pos.trailing_floor:,.0f}원\n"
                    f"   손익 <b>{pnl:+.2f}%</b>  {qty_sell:,}주 전량 매도\n"
                    f"   <i>[{t}]</i>"
                )
                events_this_step.append(msg)
                pos.events.append(f"{t} {action_type} {pnl:+.2f}%")
                continue

            # ── 2차 익절 (+10%) ──────────────────────────────
            if pnl >= TAKE_PROFIT_2 and pos.partial_sold >= 1 and pos.quantity > 0:
                qty_sell = max(1, pos.quantity // 3)
                pos.quantity -= qty_sell
                pos.partial_sold += 1
                msg = (
                    f"💰 <b>[2차익절] {pos.ticker} {pos.name}</b>\n"
                    f"   현재가 {price:,.0f}원  손익 <b>{pnl:+.2f}%</b>\n"
                    f"   {qty_sell:,}주 매도 (1/3)  잔여 {pos.quantity:,}주\n"
                    f"   트레일링 손절선→ {pos.trailing_floor:,.0f}원\n"
                    f"   <i>[{t}]</i>"
                )
                events_this_step.append(msg)
                pos.events.append(f"{t} 2차익절 {pnl:+.2f}%")

            # ── 1차 익절 (+5%) ───────────────────────────────
            elif pnl >= TAKE_PROFIT_1 and pos.partial_sold == 0 and pos.quantity > 0:
                qty_sell = max(1, pos.quantity // 3)
                pos.quantity -= qty_sell
                pos.partial_sold += 1
                msg = (
                    f"✅ <b>[1차익절] {pos.ticker} {pos.name}</b>\n"
                    f"   현재가 {price:,.0f}원  손익 <b>{pnl:+.2f}%</b>\n"
                    f"   {qty_sell:,}주 매도 (1/3)  잔여 {pos.quantity:,}주\n"
                    f"   <i>[{t}]</i>"
                )
                events_this_step.append(msg)
                pos.events.append(f"{t} 1차익절 {pnl:+.2f}%")

        # 이벤트 텔레그램 발송
        for msg in events_this_step:
            print(msg.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>",""))
            _notify(msg)

        # ── 30분마다 상태 알림 ────────────────────────────────
        if step % 6 == 5 and active_positions:  # 6봉 = 30분
            status_lines = [f"📊 <b>[{t} 포지션 현황]</b>", ""]
            for pos in active_positions:
                price = paths[pos.ticker][step + 1]
                pnl = pos.current_pnl(price)
                arrow = "▲" if pnl >= 0 else "▼"
                status_lines.append(
                    f"  {arrow} {pos.ticker} {pos.name}: "
                    f"{price:,.0f}원  <b>{pnl:+.2f}%</b>  "
                    f"잔여{pos.quantity:,}주  손절선{pos.trailing_floor:,.0f}"
                )
            print("\n".join(status_lines).replace("<b>","").replace("</b>",""))
            _notify("\n".join(status_lines))

    # ── 15:30 장 마감 청산 ───────────────────────────────────
    remaining = [p for p in positions if not p.sold and p.quantity > 0]
    if remaining:
        close_lines = ["📉 <b>[15:30 장 마감] 미청산 포지션 전량 정리</b>", ""]
        for pos in remaining:
            price = pos.entry_price  # 종가 직접 사용
            # 실제 종가 사용
            stock_data = next(s for s in HOT_LIST if s["ticker"] == pos.ticker)
            price = stock_data["close"]
            pnl = pos.current_pnl(price)
            pos.pnl_pct = pnl
            arrow = "▲" if pnl >= 0 else "▼"
            close_lines.append(
                f"  {arrow} {pos.ticker} {pos.name}: "
                f"종가 {price:,.0f}원  <b>{pnl:+.2f}%</b>  {pos.quantity:,}주 청산"
            )
            pos.events.append(f"15:30 장마감청산 {pnl:+.2f}%")
        print("\n".join(close_lines).replace("<b>","").replace("</b>",""))
        _notify("\n".join(close_lines))

    # ── 최종 결과 ────────────────────────────────────────────
    time.sleep(0.5)
    total_invest = sum(p.invest_amt for p in positions)
    results = []
    for pos in positions:
        stock_data = next(s for s in HOT_LIST if s["ticker"] == pos.ticker)
        final_pnl = pos.pnl_pct if pos.sold else pos.current_pnl(stock_data["close"])
        profit_amt = int(pos.invest_amt * final_pnl / 100)
        results.append((pos, final_pnl, profit_amt))

    total_profit = sum(r[2] for r in results)
    total_pnl_pct = total_profit / total_invest * 100

    lines = [
        "📋 <b>DQT 시뮬레이션 결과 — 2026-04-13</b>",
        "",
    ]
    for pos, pnl, profit in sorted(results, key=lambda x: x[1], reverse=True):
        arrow = "▲" if pnl >= 0 else "▼"
        ev_str = " → ".join(pos.events) if pos.events else "이벤트없음"
        lines.append(
            f"{arrow} <b>{pos.ticker} {pos.name}</b>: <b>{pnl:+.2f}%</b>  ({profit:+,.0f}원)\n"
            f"   {ev_str}"
        )

    total_emoji = "📈" if total_pnl_pct >= 0 else "📉"
    lines += [
        "",
        f"{total_emoji} <b>총 손익: {total_pnl_pct:+.2f}%  ({total_profit:+,.0f}원)</b>",
        f"   투자금 {total_invest:,.0f}원",
        f"<i>2026-04-13 시뮬레이션 완료</i>",
    ]
    print("\n".join(lines).replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>",""))
    _notify("\n".join(lines))
    print("\n시뮬레이션 완료. 텔레그램 발송됨.")


if __name__ == "__main__":
    main()
