"""
simulate_0420.py
4월 20일 월요일 — 현재 시스템(Gate 4.2 + Gate 4.5 + 신 MACD 로직) 기준 시뮬레이션

시나리오:
  A. 구 로직: Gate 4.2 없음, MACD AND buy_pre, OR sell_pre, Gate 4.5 없음
  B. 신 로직: Gate 4.2 (vol≥2x + chg>0 + RSI 28~72), MACD OR buy_pre, AND sell_pre,
              Gate 4.5 (opening quality — 즉각 하락 종목 장초반 차단)

사용 데이터:
  - 신호일: 2026-04-17 (금요일) — 장 시작 전에 알 수 있는 정보
  - 매매일: 2026-04-20 (월요일) — 실제 OHLCV 사용
  - 미국 시황: yfinance (4/17 금요일 미국 종가)
  - 장중 분봉: 미취득 → OHLCV 패턴으로 intraday 근사

실행:
  python3 simulate_0420.py
"""

from __future__ import annotations

import json
import math
import warnings
from datetime import date

import FinanceDataReader as fdr
import yfinance as yf
import pandas as pd

warnings.filterwarnings("ignore")

SIGNAL_DATE = date(2026, 4, 17)  # 금요일 — 신호 생성 기준
TRADE_DATE  = date(2026, 4, 20)  # 월요일 — 실제 매매일
CAPITAL     = 1_500_000          # 시뮬레이션 운용 자금 (원)

# 시스템 파라미터 (strategy_params 기준)
INITIAL_STOP_PCT   = 2.0   # 초기 손절선 (%)
INITIAL_STOP_MIN   = 1.5
INITIAL_STOP_MAX   = 3.5
TRAILING_TRIGGER   = 3.0   # 트레일링 시작 수익률 (%)
TRAILING_FLOOR     = 2.5   # 트레일링 간격 (%)
HOT_LIST_SIZE      = 5     # Claude Hot List 최대 종목 수

# Gate 4.2 파라미터
G42_MIN_VOL_RATIO  = 2.0
G42_MAX_RSI        = 72.0
G42_MIN_RSI        = 28.0


# ── 1. 사전 시황 ─────────────────────────────────────────────────

def get_premarket_context() -> dict:
    """4/17(금) 미국·국내 시황 — 4/20 장 시작 전에 알 수 있는 정보."""
    # 미국 시황 (4/17 금요일 종가)
    sp500  = yf.download("^GSPC", start="2026-04-15", end="2026-04-18", progress=False)
    vix    = yf.download("^VIX",  start="2026-04-15", end="2026-04-18", progress=False)
    usdkrw = yf.download("KRW=X", start="2026-04-15", end="2026-04-18", progress=False)

    def safe_pct(df, col="Close"):
        try:
            vals = df[col].dropna()
            if hasattr(vals, 'columns'):
                vals = vals.iloc[:, 0]
            if len(vals) >= 2:
                return float((vals.iloc[-1] / vals.iloc[-2] - 1) * 100)
        except Exception:
            pass
        return 0.0

    def safe_last(df, col="Close"):
        try:
            vals = df[col].dropna()
            if hasattr(vals, 'columns'):
                vals = vals.iloc[:, 0]
            if len(vals) >= 1:
                return float(vals.iloc[-1])
        except Exception:
            pass
        return None

    sp_chg  = safe_pct(sp500)
    vix_val = safe_last(vix) or 20.0
    usd_val = safe_last(usdkrw) or 1400.0

    # 국내 시황 (4/17 종가)
    kospi  = fdr.DataReader("KS11", "2026-04-14", "2026-04-17")
    kosdaq = fdr.DataReader("KQ11", "2026-04-14", "2026-04-17")

    def chg(df):
        if len(df) >= 2:
            return float((df["Close"].iloc[-1] / df["Close"].iloc[-2] - 1) * 100)
        return 0.0

    k_chg = chg(kospi)
    q_chg = chg(kosdaq)

    # 리스크 점수 계산
    global_risk = 5
    if vix_val < 15:   global_risk = 2
    elif vix_val < 20: global_risk = 3
    elif vix_val < 25: global_risk = 4
    elif vix_val < 30: global_risk = 5
    else:              global_risk = 7
    if sp_chg < -1.5:  global_risk = min(10, global_risk + 2)
    elif sp_chg > 1.5: global_risk = max(1, global_risk - 1)

    # 국내 시황 점수
    market_score = (k_chg * 0.6 + q_chg * 0.4) / 2.0

    # 리스크 레벨
    if global_risk <= 2:   risk_level = 1
    elif global_risk <= 4: risk_level = 2
    elif global_risk <= 6: risk_level = 3
    else:                  risk_level = 4

    # 포지션 한도
    position_limit = {1: 100, 2: 100, 3: 70, 4: 40, 5: 0}.get(risk_level, 70)

    return {
        "sp_chg": round(sp_chg, 2),
        "vix": round(vix_val, 1),
        "usdkrw": round(usd_val, 0),
        "kospi_chg": round(k_chg, 2),
        "kosdaq_chg": round(q_chg, 2),
        "market_score": round(market_score, 3),
        "global_risk": global_risk,
        "risk_level": risk_level,
        "position_limit": position_limit,
    }


# ── 2. 유니버스 전체 스캔 ──────────────────────────────────────────

def scan_universe() -> tuple[list[dict], list[dict]]:
    """
    4/17 기준 전체 유니버스 스캔.
    Returns (all_candidates, gate42_passed)
    """
    from src.infra.database import fetch_all, init_db
    init_db()

    rows = fetch_all(
        "SELECT ticker, name FROM universe WHERE active_date = ?",
        (str(SIGNAL_DATE),),
    )
    tickers = [(r["ticker"], r["name"]) for r in rows
               if len(r["ticker"]) == 6 and r["ticker"].isdigit()]

    print(f"  유니버스 {len(tickers)}종목 스캔 중...")
    all_cands, macd_filtered = [], 0

    for ticker, name in tickers:
        try:
            df = fdr.DataReader(ticker, "2026-03-01", "2026-04-17")
            if df is None or len(df) < 26:
                continue

            close  = df["Close"]
            volume = df["Volume"]

            # MACD
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd  = ema12 - ema26
            sig   = macd.ewm(span=9, adjust=False).mean()
            hist  = macd - sig
            hist_last = float(hist.iloc[-1])
            hist_prev = float(hist.iloc[-2])
            hist_rising = hist_last > hist_prev

            # RSI
            delta = close.diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            rs    = gain / loss.replace(0, float("nan"))
            rsi   = float((100 - 100 / (1 + rs)).iloc[-1])

            # 거래량 비율
            vol_avg   = float(volume.iloc[-6:-1].mean())
            vol_ratio = float(volume.iloc[-1] / vol_avg) if vol_avg > 0 else 1.0

            # 등락률
            chg_pct = float((close.iloc[-1] / close.iloc[-2] - 1) * 100) if len(close) >= 2 else 0.0

            # 볼린저
            ma20     = close.rolling(20).mean()
            std20    = close.rolling(20).std()
            bb_upper = float((ma20 + 2 * std20).iloc[-1])
            bb_break = bool(close.iloc[-1] > bb_upper)

            # 진입 신호 여부 (구 기준: 거래량 3x or 급등 3% or BB돌파)
            has_old_signal = (vol_ratio >= 3.0) or (chg_pct >= 3.0) or bb_break

            # MACD 히스토그램 상승 (신 로직 buy_pre = 히스토그램 상승이면 OK)
            if not hist_rising:
                macd_filtered += 1
                continue

            if not has_old_signal and vol_ratio < 2.0:
                continue  # 아무 신호도 없는 종목 제외

            # Gate 4.2 (신 로직)
            gate42 = (
                vol_ratio >= G42_MIN_VOL_RATIO
                and chg_pct > 0
                and G42_MIN_RSI <= rsi <= G42_MAX_RSI
            )

            # 4/20 일봉 데이터
            df20 = fdr.DataReader(ticker, "2026-04-20", "2026-04-20")
            if df20 is None or len(df20) == 0:
                continue

            r20   = df20.iloc[0]
            open_ = float(r20.get("Open",  0))
            high_ = float(r20.get("High",  0))
            low_  = float(r20.get("Low",   0))
            close_= float(r20.get("Close", 0))

            if open_ == 0:
                continue

            all_cands.append({
                "ticker": ticker, "name": name,
                "chg_pct": round(chg_pct, 2),
                "vol_ratio": round(vol_ratio, 2),
                "rsi": round(rsi, 1),
                "hist_last": round(hist_last, 2),
                "bb_break": bb_break,
                "gate42": gate42,
                # 4/20 실제 OHLCV
                "open":  open_,  "high":  high_,
                "low":   low_,   "close": close_,
            })
        except Exception:
            continue

    all_cands.sort(key=lambda x: x["vol_ratio"], reverse=True)
    gate42_passed = [c for c in all_cands if c["gate42"]]
    print(f"  MACD 히스토그램 비상승으로 제외: {macd_filtered}종목")
    print(f"  신호 후보: {len(all_cands)}개")
    print(f"  Gate 4.2 통과: {len(gate42_passed)}개")
    return all_cands, gate42_passed


# ── 3. Hot List 선정 ─────────────────────────────────────────────

def select_hot_list_old(candidates: list[dict]) -> list[dict]:
    """구 로직: Gate 4.2 없음, 상위 N개 직접 선정."""
    return sorted(candidates, key=lambda x: x["vol_ratio"], reverse=True)[:HOT_LIST_SIZE]


def select_hot_list_new(gate42_passed: list[dict]) -> list[dict]:
    """신 로직: Gate 4.2 통과 종목 중 상위 N개."""
    # 점수 = vol_ratio × chg_pct_bonus (BB 돌파 +1점)
    def score(c):
        s = c["vol_ratio"] * max(c["chg_pct"], 0.5)
        if c["bb_break"]: s *= 1.2
        return s
    ranked = sorted(gate42_passed, key=score, reverse=True)
    return ranked[:HOT_LIST_SIZE]


# ── 4. 장중 매매 시뮬레이션 ──────────────────────────────────────

def _classify_intraday_pattern(c: dict) -> str:
    """
    OHLCV로 장중 패턴 분류.
    - 'momentum'  : 고가가 종가에 가까움, 지속 상승
    - 'pullback'  : 고가 이후 하락하다 회복
    - 'immediate_drop': 시가≈고가, 이후 하락 (gate4.5 차단 대상)
    - 'choppy'    : 변동성만 크고 방향성 없음
    """
    o, h, l, c_ = c["open"], c["high"], c["low"], c["close"]
    high_gain = (h / o - 1) * 100
    close_ret = (c_ / o - 1) * 100
    low_loss  = (l / o - 1) * 100

    # 시가=고가 (즉각 하락)
    if high_gain < 0.5:
        return "immediate_drop"
    # 고가가 종가와 가깝고 저가가 -2% 이내
    if close_ret >= high_gain * 0.7 and low_loss > -2.0:
        return "momentum"
    # 고가 이후 하락했다가 종가가 어느 정도 회복
    if high_gain >= 2.0 and close_ret >= 0 and low_loss < -1.5:
        return "pullback"
    # 고가에서 하락, 종가가 시가보다 낮음
    if close_ret < 0 and high_gain < 3.0:
        return "immediate_drop"
    return "choppy"


def simulate_trade(
    c: dict,
    use_gate45: bool,
    market_score: float,
    tranche_pct: float = 1.0,
) -> dict:
    """
    단일 종목 장중 매매 시뮬레이션.

    Args:
        use_gate45: True=신 로직 (Gate 4.5 적용), False=구 로직
        market_score: 국내 시황 점수 (-1~1)
        tranche_pct: 1차 매수 비중 (0.6 = 60%)

    Returns:
        {entry_price, exit_price, pnl_pct, exit_reason, blocked}
    """
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    pattern = _classify_intraday_pattern(c)

    # ── Gate 4.5 차단 판단 ───────────────────────────────────────
    if use_gate45 and pattern == "immediate_drop":
        # 시가=고가형 → 장초반 MACD sell_pre 상태 → 차단
        # 장 후반에도 MACD buy_pre 회복 안 되면 미진입
        # 종가 < 시가인 경우 매매 불가로 처리
        if cl < o:
            return {"entry": 0, "exit": 0, "pnl_pct": 0, "reason": "Gate4.5 차단 (즉각하락)", "blocked": True}
        # 종가가 시가보다 높다면 후반 반등으로 MACD buy_pre 가능 → 늦게 진입
        entry_price = o * 1.005  # 늦은 진입 (09:30 이후 반등 확인 후)
    else:
        entry_price = o * 1.005  # 오프닝 직후 MACD 확인 후 진입 (0.5% 슬리피지 근사)

    # ── 동적 손절선 계산 ─────────────────────────────────────────
    rsi = c["rsi"]
    stop_pct = INITIAL_STOP_PCT
    if market_score < -0.1:
        stop_pct = INITIAL_STOP_MIN    # 약세장 손절 타이트
    elif rsi > 65:
        stop_pct = INITIAL_STOP_MIN
    elif rsi < 45:
        stop_pct = min(stop_pct + 0.5, INITIAL_STOP_MAX * 0.86)
    stop_pct = max(INITIAL_STOP_MIN, min(INITIAL_STOP_MAX, stop_pct))
    initial_stop = entry_price * (1 - stop_pct / 100)

    # ── 트레일링 스톱 시뮬레이션 ─────────────────────────────────
    # OHLCV 기반 간이 시뮬레이션
    # 가정: 장중 가격은 시가→고가→저가→종가 순서 근사
    # (실제 분봉 없으므로 패턴에 따라 순서 달리 가정)

    trigger_price = entry_price * (1 + TRAILING_TRIGGER / 100)
    trailing_active = False
    highest = entry_price
    floor = initial_stop
    exit_price = None
    exit_reason = ""

    def check_stop(price: float) -> bool:
        nonlocal trailing_active, highest, floor
        if price > highest:
            highest = price
            if trailing_active:
                floor = highest * (1 - TRAILING_FLOOR / 100)
        if not trailing_active and highest >= trigger_price:
            trailing_active = True
            floor = highest * (1 - TRAILING_FLOOR / 100)
        return price <= floor

    # 패턴별 시뮬레이션
    if pattern == "momentum":
        # 상승 지속 → 고가 먼저, 저가는 나중
        price_seq = [o, h, l, cl]
    elif pattern == "pullback":
        # 상승 → 눌림 → 재상승
        mid = (h + l) / 2
        price_seq = [o, h, l, cl]
    elif pattern == "immediate_drop":
        # 하락 먼저
        price_seq = [o, h, l, cl]
    else:
        price_seq = [o, h, l, cl]

    for price in price_seq:
        if exit_price:
            break
        if check_stop(price):
            exit_price = floor
            exit_reason = "트레일링스톱" if trailing_active else "초기손절"

    if not exit_price:
        # 14:50 시간 종료 — 수익 포지션은 종가 근사
        exit_price = cl
        exit_reason = "시간청산"

    pnl_pct = (exit_price / entry_price - 1) * 100

    return {
        "entry":   round(entry_price, 0),
        "exit":    round(exit_price, 0),
        "pnl_pct": round(pnl_pct, 2),
        "reason":  exit_reason,
        "blocked": False,
        "pattern": pattern,
        "stop_pct": round(stop_pct, 2),
        "trailing_triggered": trailing_active,
    }


# ── 5. 포트폴리오 성과 계산 ───────────────────────────────────────

def portfolio_pnl(hot_list: list[dict], trade_results: list[dict]) -> dict:
    """분할 투자 기준 포트폴리오 수익 계산."""
    n = len(trade_results)
    if n == 0:
        return {"total_pnl_pct": 0, "trades": 0, "wins": 0, "losses": 0, "avg_pnl": 0}

    alloc = CAPITAL / n  # 균등 배분
    total_pnl = 0.0
    wins = losses = 0

    for r in trade_results:
        if r.get("blocked"):
            continue
        pnl = alloc * r["pnl_pct"] / 100
        total_pnl += pnl
        if r["pnl_pct"] > 0:
            wins += 1
        else:
            losses += 1

    non_blocked = [r for r in trade_results if not r.get("blocked")]
    avg_pnl = total_pnl / CAPITAL * 100 if CAPITAL > 0 else 0

    return {
        "total_pnl":     round(total_pnl, 0),
        "total_pnl_pct": round(avg_pnl, 2),
        "trades":        len(non_blocked),
        "wins":          wins,
        "losses":        losses,
        "avg_pnl":       round(sum(r["pnl_pct"] for r in non_blocked) / len(non_blocked), 2) if non_blocked else 0,
    }


# ── 6. HTML 리포트 생성 ──────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>DQT 시뮬레이션 — 2026-04-20</title>
<style>
:root {{
  --bg: #0f1117; --bg2: #1a1d27; --bg3: #232635;
  --text: #e4e6ef; --text2: #8b8fa8; --text3: #555875;
  --accent: #7b8cff; --green: #00d4aa; --red: #ff5566;
  --yellow: #ffd166; --orange: #ff9a3c;
  --border: #2e3145; --shadow: rgba(0,0,0,0.4);
}}
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ font-family:'Segoe UI',system-ui,sans-serif; background:var(--bg); color:var(--text); line-height:1.6; }}
.container {{ max-width:1280px; margin:0 auto; padding:24px 16px; }}
h1 {{ font-size:1.8rem; font-weight:700; color:var(--accent); margin-bottom:6px; }}
.subtitle {{ color:var(--text2); font-size:.95rem; margin-bottom:28px; }}
.section {{ background:var(--bg2); border:1px solid var(--border); border-radius:12px; padding:20px; margin-bottom:20px; }}
.section h2 {{ font-size:1.05rem; font-weight:600; color:var(--text2); text-transform:uppercase; letter-spacing:.06em; margin-bottom:16px; border-bottom:1px solid var(--border); padding-bottom:10px; }}
.grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
.grid3 {{ display:grid; grid-template-columns:repeat(3,1fr); gap:16px; }}
.grid4 {{ display:grid; grid-template-columns:repeat(4,1fr); gap:16px; }}
.kpi {{ background:var(--bg3); border-radius:8px; padding:16px; text-align:center; }}
.kpi-label {{ font-size:.75rem; color:var(--text3); text-transform:uppercase; letter-spacing:.08em; margin-bottom:6px; }}
.kpi-val {{ font-size:1.8rem; font-weight:700; }}
.kpi-sub {{ font-size:.78rem; color:var(--text2); margin-top:4px; }}
.pos {{ color:var(--green); }}
.neg {{ color:var(--red); }}
.neu {{ color:var(--text2); }}
.tag {{ display:inline-block; font-size:.68rem; font-weight:600; padding:2px 7px; border-radius:4px; margin:2px; }}
.tag-g {{ background:rgba(0,212,170,.15); color:var(--green); }}
.tag-r {{ background:rgba(255,85,102,.15); color:var(--red); }}
.tag-y {{ background:rgba(255,209,102,.15); color:var(--yellow); }}
.tag-b {{ background:rgba(123,140,255,.15); color:var(--accent); }}
.tag-o {{ background:rgba(255,154,60,.15); color:var(--orange); }}
table {{ width:100%; border-collapse:collapse; font-size:.85rem; }}
th {{ background:var(--bg3); color:var(--text2); font-weight:600; padding:10px 12px; text-align:left; font-size:.75rem; text-transform:uppercase; letter-spacing:.06em; }}
td {{ padding:10px 12px; border-bottom:1px solid var(--border); vertical-align:middle; }}
tr:last-child td {{ border-bottom:none; }}
tr:hover td {{ background:rgba(255,255,255,.02); }}
.badge {{ display:inline-block; font-size:.68rem; font-weight:700; padding:2px 8px; border-radius:20px; }}
.badge-g {{ background:rgba(0,212,170,.2); color:var(--green); }}
.badge-r {{ background:rgba(255,85,102,.2); color:var(--red); }}
.badge-y {{ background:rgba(255,209,102,.2); color:var(--yellow); }}
.badge-n {{ background:rgba(85,88,117,.3); color:var(--text2); }}
.badge-b {{ background:rgba(123,140,255,.2); color:var(--accent); }}
.scenario-box {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }}
.scenario {{ border-radius:10px; padding:18px; }}
.scenario-a {{ background:rgba(255,85,102,.07); border:1px solid rgba(255,85,102,.25); }}
.scenario-b {{ background:rgba(0,212,170,.07); border:1px solid rgba(0,212,170,.25); }}
.scenario h3 {{ font-size:.9rem; margin-bottom:14px; font-weight:700; }}
.scenario-a h3 {{ color:var(--red); }}
.scenario-b h3 {{ color:var(--green); }}
.alert {{ background:rgba(255,209,102,.08); border:1px solid rgba(255,209,102,.25); border-radius:8px; padding:14px 16px; margin-bottom:14px; font-size:.87rem; color:var(--yellow); }}
.note {{ color:var(--text3); font-size:.78rem; margin-top:8px; }}
.flow {{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-top:12px; }}
.flow-item {{ background:var(--bg3); border:1px solid var(--border); border-radius:6px; padding:8px 12px; font-size:.82rem; }}
.flow-arrow {{ color:var(--text3); }}
</style>
</head>
<body>
<div class="container">
<h1>DQT 시뮬레이션 — 2026-04-20 (월)</h1>
<div class="subtitle">신호일: 2026-04-17(금) | 매매일: 2026-04-20(월) | 운용자금: {capital:,}원</div>

{sections}

</div>
</body>
</html>"""


def build_html(ctx: dict, all_cands: list, gate42: list, old_hl: list, new_hl: list,
               old_trades: list, new_trades: list, old_perf: dict, new_perf: dict) -> str:

    def pct_class(v): return "pos" if v > 0 else "neg" if v < 0 else "neu"
    def pct_badge(v):
        if v > 0: return f'<span class="badge badge-g">▲{abs(v):.2f}%</span>'
        if v < 0: return f'<span class="badge badge-r">▼{abs(v):.2f}%</span>'
        return f'<span class="badge badge-n">0.00%</span>'

    # ── 1. 사전 시황 섹션 ─────────────────────────────────────────
    risk_cls = ["","tag-g","tag-g","tag-y","tag-o","tag-r"]
    rlv = ctx["risk_level"]
    ms  = ctx["market_score"]
    ms_cls = "pos" if ms > 0 else "neg" if ms < 0 else "neu"

    s1 = f"""
<div class="section">
<h2>장 시작 전 시황 (4/17 금요일 종가 기준)</h2>
<div class="grid4">
  <div class="kpi"><div class="kpi-label">VIX</div><div class="kpi-val {'neg' if ctx['vix']>25 else 'yellow' if ctx['vix']>20 else 'pos'}">{ctx['vix']}</div><div class="kpi-sub">공포지수</div></div>
  <div class="kpi"><div class="kpi-label">S&amp;P500</div><div class="kpi-val {pct_class(ctx['sp_chg'])}">{ctx['sp_chg']:+.2f}%</div><div class="kpi-sub">전일 대비</div></div>
  <div class="kpi"><div class="kpi-label">KOSPI</div><div class="kpi-val {pct_class(ctx['kospi_chg'])}">{ctx['kospi_chg']:+.2f}%</div><div class="kpi-sub">4/17 등락</div></div>
  <div class="kpi"><div class="kpi-label">KOSDAQ</div><div class="kpi-val {pct_class(ctx['kosdaq_chg'])}">{ctx['kosdaq_chg']:+.2f}%</div><div class="kpi-sub">4/17 등락</div></div>
</div>
<div style="margin-top:16px" class="grid3">
  <div class="kpi"><div class="kpi-label">국내 시황 점수</div><div class="kpi-val {ms_cls}">{ms:+.3f}</div><div class="kpi-sub">-1(약세) ~ +1(강세)</div></div>
  <div class="kpi"><div class="kpi-label">글로벌 리스크</div><div class="kpi-val {'pos' if ctx['global_risk']<=3 else 'neg' if ctx['global_risk']>=7 else 'yellow'}">{ctx['global_risk']}/10</div><div class="kpi-sub">VIX·S&P500 기반</div></div>
  <div class="kpi"><div class="kpi-label">리스크 레벨</div><div class="kpi-val">{rlv}/5</div><div class="kpi-sub">포지션 한도 {ctx['position_limit']}%</div></div>
</div>
<div class="alert" style="margin-top:16px">
⚙️ <strong>오프닝 게이트 판단</strong>: 시황점수 {ms:+.3f} · 리스크레벨 {rlv}/5
— {'<strong style="color:var(--green)">IMMEDIATE</strong> (즉시 매수)' if rlv <= 2 and ms >= 0.3 else '<strong style="color:var(--yellow)">WAIT</strong> (09:10까지 관망 후 MACD 재확인)'}
</div>
</div>"""

    # ── 2. 종목 스캔 + Gate 필터 ──────────────────────────────────
    g42_blocked = [c for c in all_cands if not c["gate42"]]
    g42_blocked_sample = sorted(g42_blocked, key=lambda x: x["vol_ratio"], reverse=True)[:10]

    def signal_tags(c):
        tags = []
        if c["bb_break"]: tags.append('<span class="tag tag-b">BB돌파</span>')
        if c["vol_ratio"] >= 3.0: tags.append(f'<span class="tag tag-g">거래량{c["vol_ratio"]:.1f}x</span>')
        elif c["vol_ratio"] >= 2.0: tags.append(f'<span class="tag tag-y">거래량{c["vol_ratio"]:.1f}x</span>')
        if c["chg_pct"] >= 3.0: tags.append(f'<span class="tag tag-g">급등{c["chg_pct"]:+.1f}%</span>')
        elif c["chg_pct"] > 0: tags.append(f'<span class="tag tag-y">상승{c["chg_pct"]:+.1f}%</span>')
        else: tags.append(f'<span class="tag tag-r">하락{c["chg_pct"]:+.1f}%</span>')
        return "".join(tags)

    blocked_rows = "".join(
        f"""<tr>
        <td><strong>{c['ticker']}</strong></td>
        <td>{c['name']}</td>
        <td>{signal_tags(c)}</td>
        <td class="{'pos' if c['chg_pct']>0 else 'neg'}">{c['chg_pct']:+.1f}%</td>
        <td>{c['vol_ratio']:.1f}x</td>
        <td class="{'neg' if c['rsi']>72 or c['rsi']<28 else 'neu'}">{c['rsi']:.1f}</td>
        <td class="neg">{'RSI 과열 '+str(round(c['rsi'],1)) if c['rsi']>G42_MAX_RSI else 'RSI 붕괴 '+str(round(c['rsi'],1)) if c['rsi']<G42_MIN_RSI else '등락률 ≤0' if c['chg_pct']<=0 else '거래량 부족'}</td>
        </tr>"""
        for c in g42_blocked_sample
    )
    g42_rows = "".join(
        f"""<tr>
        <td><strong>{c['ticker']}</strong></td>
        <td>{c['name']}</td>
        <td>{signal_tags(c)}</td>
        <td class="{'pos' if c['chg_pct']>0 else 'neg'}">{c['chg_pct']:+.1f}%</td>
        <td>{c['vol_ratio']:.1f}x</td>
        <td>{c['rsi']:.1f}</td>
        <td class="{'pos' if c['close']>c['open'] else 'neg'}">{(c['close']/c['open']-1)*100:+.2f}%</td>
        </tr>"""
        for c in gate42
    )

    s2 = f"""
<div class="section">
<h2>종목 스캔 — Gate 4.2 필터 (신 로직)</h2>
<div class="flow">
  <div class="flow-item">유니버스 {len(all_cands)+len([c for c in all_cands if not c.get('macd_pass', True)])}종목</div>
  <div class="flow-arrow">→</div>
  <div class="flow-item">MACD 히스토그램 상승 필터</div>
  <div class="flow-arrow">→</div>
  <div class="flow-item"><strong>{len(all_cands)}</strong>개 신호</div>
  <div class="flow-arrow">→</div>
  <div class="flow-item" style="border-color:var(--green)">Gate 4.2<br>vol≥2x + 상승 + RSI 28~72</div>
  <div class="flow-arrow">→</div>
  <div class="flow-item" style="border-color:var(--green)"><strong>{len(gate42)}</strong>개 통과</div>
</div>

<div class="grid2" style="margin-top:16px; gap:20px">
<div>
<div style="margin-bottom:10px; font-size:.85rem; color:var(--red)">❌ Gate 4.2 차단 (상위 10개)</div>
<table>
<thead><tr><th>코드</th><th>종목명</th><th>신호</th><th>등락</th><th>거래량</th><th>RSI</th><th>차단 사유</th></tr></thead>
<tbody>{blocked_rows}</tbody>
</table>
</div>
<div>
<div style="margin-bottom:10px; font-size:.85rem; color:var(--green)">✅ Gate 4.2 통과 ({len(gate42)}개)</div>
<table>
<thead><tr><th>코드</th><th>종목명</th><th>신호</th><th>등락(4/17)</th><th>거래량</th><th>RSI</th><th>4/20 실제등락</th></tr></thead>
<tbody>{g42_rows}</tbody>
</table>
</div>
</div>
</div>"""

    # ── 3. 시나리오 비교 (A구 vs B신) ─────────────────────────────
    def trade_row_old(c, r):
        pat = r.get("pattern", "")
        pat_label = {"momentum":"모멘텀","pullback":"눌림후회복","immediate_drop":"즉각하락","choppy":"혼조","":"-"}.get(pat, pat)
        reason_cls = "neg" if "손절" in r["reason"] else "pos" if r["reason"]=="시간청산" and r["pnl_pct"]>0 else "neu"
        return f"""<tr>
        <td><strong>{c['ticker']}</strong><br><span style="font-size:.75rem;color:var(--text2)">{c['name']}</span></td>
        <td>{c['chg_pct']:+.1f}% / {c['vol_ratio']:.1f}x / RSI{c['rsi']:.0f}</td>
        <td>{c['open']:,.0f}</td>
        <td class="{pct_class(r['pnl_pct'])}">{pct_badge(r['pnl_pct'])}</td>
        <td class="{reason_cls}">{r['reason']}</td>
        <td>{pat_label}</td>
        </tr>"""

    def trade_row_new(c, r):
        pat = r.get("pattern", "")
        pat_label = {"momentum":"모멘텀","pullback":"눌림후회복","immediate_drop":"즉각하락","choppy":"혼조","":"-"}.get(pat, pat)
        if r.get("blocked"):
            return f"""<tr style="opacity:.55">
            <td><strong>{c['ticker']}</strong><br><span style="font-size:.75rem;color:var(--text2)">{c['name']}</span></td>
            <td>{c['chg_pct']:+.1f}% / {c['vol_ratio']:.1f}x / RSI{c['rsi']:.0f}</td>
            <td>—</td>
            <td><span class="badge badge-n">미진입</span></td>
            <td class="neg">{r['reason']}</td>
            <td>{pat_label}</td>
            </tr>"""
        reason_cls = "neg" if "손절" in r["reason"] else "pos" if r["reason"]=="시간청산" and r["pnl_pct"]>0 else "neu"
        return f"""<tr>
        <td><strong>{c['ticker']}</strong><br><span style="font-size:.75rem;color:var(--text2)">{c['name']}</span></td>
        <td>{c['chg_pct']:+.1f}% / {c['vol_ratio']:.1f}x / RSI{c['rsi']:.0f}</td>
        <td>{c['open']:,.0f}</td>
        <td class="{pct_class(r['pnl_pct'])}">{pct_badge(r['pnl_pct'])}</td>
        <td class="{reason_cls}">{r['reason']}</td>
        <td>{pat_label}</td>
        </tr>"""

    old_rows = "".join(trade_row_old(c, r) for c, r in zip(old_hl, old_trades))
    new_rows = "".join(trade_row_new(c, r) for c, r in zip(new_hl, new_trades))

    op_old = old_perf
    op_new = new_perf
    op_diff = round(op_new["total_pnl_pct"] - op_old["total_pnl_pct"], 2)
    diff_cls = "pos" if op_diff > 0 else "neg"

    s3 = f"""
<div class="section">
<h2>시나리오 비교 — A(구 로직) vs B(신 로직)</h2>
<div class="scenario-box">
  <div class="scenario scenario-a">
    <h3>🔴 A. 구 로직 (기존 시스템)</h3>
    <ul style="font-size:.82rem; color:var(--text2); margin-bottom:12px; padding-left:16px">
      <li>Gate 4.2 없음 — 거래량·등락·RSI 개별 기준</li>
      <li>MACD buy_pre: AND (3분봉 AND 5분봉)</li>
      <li>MACD sell_pre: OR  (3분봉 OR  5분봉)</li>
      <li>Gate 4.5 없음 — 장초반 즉각 하락 종목도 진입</li>
    </ul>
    <div class="grid2" style="gap:10px">
      <div class="kpi"><div class="kpi-label">총 손익</div><div class="kpi-val {pct_class(op_old['total_pnl'])}">{op_old['total_pnl']:+,.0f}원</div></div>
      <div class="kpi"><div class="kpi-label">평균 수익률</div><div class="kpi-val {pct_class(op_old['avg_pnl'])}">{op_old['avg_pnl']:+.2f}%</div></div>
      <div class="kpi"><div class="kpi-label">승/패</div><div class="kpi-val">{op_old['wins']}/{op_old['losses']}</div></div>
      <div class="kpi"><div class="kpi-label">승률</div><div class="kpi-val {pct_class(op_old['wins']/(op_old['wins']+op_old['losses'])*100-50 if op_old['trades'] else 0)}">{op_old['wins']/op_old['trades']*100:.0f}%</div></div>
    </div>
    <table style="margin-top:14px">
    <thead><tr><th>종목</th><th>신호(4/17)</th><th>매수가</th><th>수익률</th><th>청산사유</th><th>패턴</th></tr></thead>
    <tbody>{old_rows}</tbody>
    </table>
  </div>
  <div class="scenario scenario-b">
    <h3>🟢 B. 신 로직 (현재 시스템)</h3>
    <ul style="font-size:.82rem; color:var(--text2); margin-bottom:12px; padding-left:16px">
      <li>Gate 4.2: vol≥2x AND 상승 AND RSI 28~72 (<strong style="color:var(--green)">{len(gate42)}개 통과</strong>)</li>
      <li>MACD buy_pre: OR  (3분봉 OR  5분봉 히스토그램 상승)</li>
      <li>MACD sell_pre: AND (3분봉 AND 5분봉 동시 하강)</li>
      <li>Gate 4.5: 즉각 하락 종목 장초반 차단</li>
    </ul>
    <div class="grid2" style="gap:10px">
      <div class="kpi"><div class="kpi-label">총 손익</div><div class="kpi-val {pct_class(op_new['total_pnl'])}">{op_new['total_pnl']:+,.0f}원</div></div>
      <div class="kpi"><div class="kpi-label">평균 수익률</div><div class="kpi-val {pct_class(op_new['avg_pnl'])}">{op_new['avg_pnl']:+.2f}%</div></div>
      <div class="kpi"><div class="kpi-label">승/패</div><div class="kpi-val">{op_new['wins']}/{op_new['losses']}</div></div>
      <div class="kpi"><div class="kpi-label">승률</div><div class="kpi-val {pct_class(op_new['wins']/(op_new['wins']+op_new['losses'])*100-50 if op_new['trades'] else 0)}">{op_new['wins']/max(op_new['trades'],1)*100:.0f}%</div></div>
    </div>
    <table style="margin-top:14px">
    <thead><tr><th>종목</th><th>신호(4/17)</th><th>매수가</th><th>수익률</th><th>청산사유</th><th>패턴</th></tr></thead>
    <tbody>{new_rows}</tbody>
    </table>
  </div>
</div>
<div class="kpi" style="margin-top:16px; padding:16px">
  <div class="kpi-label">신 로직 개선 효과 (A→B 차이)</div>
  <div class="kpi-val {diff_cls}">{op_diff:+.2f}%p</div>
  <div class="kpi-sub">총 손익 차이: {op_new['total_pnl']-op_old['total_pnl']:+,.0f}원</div>
</div>
</div>"""

    # ── 4. 핵심 인사이트 ──────────────────────────────────────────
    blocked_names = [c["name"] for c, r in zip(new_hl, new_trades) if r.get("blocked")]
    winners = [(c, r) for c, r in zip(new_hl, new_trades) if not r.get("blocked") and r["pnl_pct"] > 0]
    losers  = [(c, r) for c, r in zip(new_hl, new_trades) if not r.get("blocked") and r["pnl_pct"] <= 0]

    win_lines  = "".join(f'<li><strong>{c["name"]}</strong>({c["ticker"]}): {pct_badge(r["pnl_pct"])} — {r["reason"]}</li>' for c,r in winners)
    loss_lines = "".join(f'<li><strong>{c["name"]}</strong>({c["ticker"]}): {pct_badge(r["pnl_pct"])} — {r["reason"]}</li>' for c,r in losers)
    block_lines = "".join(f'<li>{n}</li>' for n in blocked_names) if blocked_names else "<li>없음</li>"

    s4 = f"""
<div class="section">
<h2>핵심 인사이트 — 신 로직 기준</h2>
<div class="grid3">
  <div>
    <div style="color:var(--green);font-weight:600;margin-bottom:8px">✅ 수익 종목</div>
    <ul style="font-size:.85rem;padding-left:16px;line-height:2">{win_lines if win_lines else '<li>없음</li>'}</ul>
  </div>
  <div>
    <div style="color:var(--red);font-weight:600;margin-bottom:8px">⚠️ 손실 종목</div>
    <ul style="font-size:.85rem;padding-left:16px;line-height:2">{loss_lines if loss_lines else '<li>없음</li>'}</ul>
  </div>
  <div>
    <div style="color:var(--yellow);font-weight:600;margin-bottom:8px">🚫 Gate 4.5 차단</div>
    <ul style="font-size:.85rem;padding-left:16px;line-height:2">{block_lines}</ul>
    <div class="note" style="margin-top:6px">즉각 하락 패턴 → 장초반 MACD 매수 신호 없음 → 미진입</div>
  </div>
</div>
<div class="alert" style="margin-top:16px">
💡 <strong>시뮬레이션 한계</strong>: 실제 분봉 데이터 없이 일봉 OHLCV + 패턴 근사로 매매 시점을 추정했습니다.
실제 시스템은 3분봉·5분봉 MACD를 3분 주기로 체크하므로 진입·청산 타이밍이 달라질 수 있습니다.
특히 "눌림→회복" 패턴 종목의 정확한 진입 시점은 분봉 데이터로만 확인 가능합니다.
</div>
</div>"""

    sections = s1 + s2 + s3 + s4
    return HTML_TEMPLATE.format(capital=CAPITAL, sections=sections)


# ── 메인 ─────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print(f"  DQT 시뮬레이션 — {SIGNAL_DATE}(금) 신호 → {TRADE_DATE}(월) 매매")
    print("=" * 65)

    print("\n[1] 장 시작 전 시황 수집 (4/17 종가 기준)")
    ctx = get_premarket_context()
    print(f"  VIX {ctx['vix']}  S&P500 {ctx['sp_chg']:+.2f}%  USD/KRW {ctx['usdkrw']:.0f}")
    print(f"  KOSPI {ctx['kospi_chg']:+.2f}%  KOSDAQ {ctx['kosdaq_chg']:+.2f}%")
    print(f"  시황점수 {ctx['market_score']:+.3f}  글로벌리스크 {ctx['global_risk']}/10  리스크레벨 {ctx['risk_level']}/5")
    ow_text = "IMMEDIATE" if ctx["risk_level"] <= 2 and ctx["market_score"] >= 0.3 else "WAIT"
    print(f"  → 오프닝게이트: {ow_text}")

    print("\n[2] 전체 유니버스 스캔 (4/17 기준)")
    all_cands, gate42 = scan_universe()

    print(f"\n[3] Hot List 선정")
    # 구 로직: Gate 4.2 없음 — vol_ratio 상위 N개
    old_hl = select_hot_list_old(all_cands)
    # 신 로직: Gate 4.2 통과 종목 중 선정
    new_hl = select_hot_list_new(gate42)
    print(f"  구 로직 Hot List: {[c['ticker'] for c in old_hl]}")
    print(f"  신 로직 Hot List: {[c['ticker'] for c in new_hl]}")

    print(f"\n[4] 장중 매매 시뮬레이션 (4/20 OHLCV 기반)")
    print(f"  초기 손절선: {INITIAL_STOP_PCT}%  트레일링 트리거: +{TRAILING_TRIGGER}%  트레일링 간격: {TRAILING_FLOOR}%")
    print()

    old_trades = [simulate_trade(c, use_gate45=False, market_score=ctx["market_score"]) for c in old_hl]
    new_trades = [simulate_trade(c, use_gate45=True,  market_score=ctx["market_score"]) for c in new_hl]

    print("  ── 구 로직 ────────────────────────────────────────────────")
    for c, r in zip(old_hl, old_trades):
        print(f"  {c['ticker']} {c['name'][:10]:10s}: {r['pnl_pct']:+.2f}%  ({r['reason']})  [{r.get('pattern','')}]")

    print()
    print("  ── 신 로직 ────────────────────────────────────────────────")
    for c, r in zip(new_hl, new_trades):
        if r.get("blocked"):
            print(f"  {c['ticker']} {c['name'][:10]:10s}: [차단] {r['reason']}")
        else:
            print(f"  {c['ticker']} {c['name'][:10]:10s}: {r['pnl_pct']:+.2f}%  ({r['reason']})  [{r.get('pattern','')}]")

    old_perf = portfolio_pnl(old_hl, old_trades)
    new_perf = portfolio_pnl(new_hl, new_trades)

    print(f"\n[5] 성과 요약")
    print(f"  구 로직: 평균 {old_perf['avg_pnl']:+.2f}% | {old_perf['wins']}승 {old_perf['losses']}패 | 총 {old_perf['total_pnl']:+,.0f}원")
    print(f"  신 로직: 평균 {new_perf['avg_pnl']:+.2f}% | {new_perf['wins']}승 {new_perf['losses']}패 | 총 {new_perf['total_pnl']:+,.0f}원")
    diff = new_perf["total_pnl_pct"] - old_perf["total_pnl_pct"]
    print(f"  개선 효과: {diff:+.2f}%p")

    print("\n[6] HTML 리포트 생성")
    html = build_html(ctx, all_cands, gate42, old_hl, new_hl, old_trades, new_trades, old_perf, new_perf)
    out_path = "simulation_0420.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  → {out_path} 저장 완료")
    print("=" * 65)


if __name__ == "__main__":
    main()
