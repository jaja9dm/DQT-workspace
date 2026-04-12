"""
simulate_friday.py
금요일(2026-04-10) 하루 시뮬레이션 — MACD 필터 + 오프닝 게이트 포함.

실제 시스템이 그날 돌았다면 어떤 종목을 샀고, 수익은 얼마였는지 계산.

실행:
  python3 simulate_friday.py
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import FinanceDataReader as fdr
import pandas as pd

from src.config.settings import settings
from src.infra.database import init_db
from src.infra.universe import UniverseManager
from src.utils.logger import get_logger
from src.utils.macd import is_daily_macd_bullish
from src.utils.notifier import notify

logger = get_logger("simulate")
SIGNAL_DATE = date(2026, 4, 9)   # 목요일 — 이 날 데이터로 신호 탐지
TRADE_DATE  = date(2026, 4, 10)  # 금요일 — 시가 매수 → 종가 매도


# ── 1. 글로벌 시황 (yfinance) ────────────────────────────────

def get_global_context() -> dict:
    import yfinance as yf
    # 목요일 종가 기준 (금요일 9시 전에 알 수 있는 정보)
    sp500 = yf.download("^GSPC", start="2026-04-07", end="2026-04-10", progress=False)
    vix   = yf.download("^VIX",  start="2026-04-07", end="2026-04-10", progress=False)
    usdkrw= yf.download("KRW=X", start="2026-04-07", end="2026-04-10", progress=False)

    sp_chg = float(sp500["Close"].pct_change().iloc[-1] * 100) if len(sp500) >= 2 else 0.0
    vix_val= float(vix["Close"].iloc[-1]) if len(vix) >= 1 else 20.0
    usd_val= float(usdkrw["Close"].iloc[-1]) if len(usdkrw) >= 1 else 1400.0

    print(f"  S&P500: {sp_chg:+.2f}%  VIX: {vix_val:.1f}  USD/KRW: {usd_val:.0f}")
    return {"sp500_change": sp_chg, "vix": vix_val, "usdkrw": usd_val}


# ── 2. 국내 시황 ─────────────────────────────────────────────

def get_domestic_context() -> dict:
    # 목요일 종가 기준
    kospi  = fdr.DataReader("KS11", "2026-04-07", "2026-04-09")
    kosdaq = fdr.DataReader("KQ11", "2026-04-07", "2026-04-09")

    def chg(df):
        if len(df) >= 2:
            return float((df["Close"].iloc[-1] / df["Close"].iloc[-2] - 1) * 100)
        return 0.0

    k_chg = chg(kospi)
    q_chg = chg(kosdaq)
    print(f"  KOSPI: {k_chg:+.2f}%  KOSDAQ: {q_chg:+.2f}%")
    return {"kospi_change": k_chg, "kosdaq_change": q_chg}


# ── 3. 종목 스캔 (금요일 데이터 기준) ───────────────────────

def scan_universe() -> list[dict]:
    init_db()
    um = UniverseManager()
    # 오늘(일요일) 유니버스를 재사용 (종목 목록만 필요)
    tickers = um.get_today()
    if not tickers:
        um.rebuild()
        tickers = um.get_today()

    print(f"  총 {len(tickers)}종목 스캔 중...")

    # 종목명 맵
    from src.infra.database import fetch_all
    rows = fetch_all("SELECT ticker, name FROM universe WHERE active_date = ?", (str(date.today()),))
    name_map = {r["ticker"]: r["name"] for r in rows}

    results = []
    macd_filtered_out = 0
    for ticker in tickers:
        try:
            # 목요일까지만 수집 (금요일 9시에 알 수 있는 정보)
            df = fdr.DataReader(ticker, "2026-03-01", "2026-04-09")
            if df is None or len(df) < 20:
                continue

            close = df["Close"]
            volume = df["Volume"]

            # ── 일봉 MACD 필터 ──────────────────────────────────
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd_line   = ema12 - ema26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            hist        = macd_line - signal_line

            daily_macd_ok = is_daily_macd_bullish(
                macd_val=float(macd_line.iloc[-1]),
                signal_val=float(signal_line.iloc[-1]),
                hist_val=float(hist.iloc[-1]),
                prev_hist_val=float(hist.iloc[-2]),
            )
            if not daily_macd_ok:
                macd_filtered_out += 1
                continue  # 일봉 MACD 비강세 → 제외

            # 등락률 (목요일 기준)
            change_pct = float((close.iloc[-1] / close.iloc[-2] - 1) * 100) if len(close) >= 2 else 0.0
            # 거래량 비율 (5일 평균 대비, 목요일 기준)
            vol_avg = volume.iloc[-6:-1].mean()
            vol_ratio = float(volume.iloc[-1] / vol_avg) if vol_avg > 0 else 1.0

            # RSI(14)
            delta = close.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain / loss.replace(0, float("nan"))
            rsi = float((100 - (100 / (1 + rs))).iloc[-1])

            # 볼린저밴드
            ma20 = close.rolling(20).mean()
            std20 = close.rolling(20).std()
            bb_upper = float((ma20 + 2 * std20).iloc[-1])
            is_breakout = bool(close.iloc[-1] > bb_upper)

            is_volume_surge = vol_ratio >= 3.0
            is_price_surge  = change_pct >= 3.0

            if is_volume_surge or is_price_surge or is_breakout:
                # 금요일 시가/종가 별도 조회
                try:
                    df_fri = fdr.DataReader(ticker, "2026-04-10", "2026-04-10")
                    fri_open  = float(df_fri["Open"].iloc[0])  if len(df_fri) > 0 else 0.0
                    fri_close = float(df_fri["Close"].iloc[0]) if len(df_fri) > 0 else 0.0
                except Exception:
                    fri_open = fri_close = 0.0

                results.append({
                    "ticker": ticker,
                    "name": name_map.get(ticker, ticker),
                    "change_pct": round(change_pct, 2),   # 목요일 등락률
                    "vol_ratio": round(vol_ratio, 1),
                    "rsi": round(rsi, 1),
                    "is_breakout": is_breakout,
                    "is_volume_surge": is_volume_surge,
                    "is_price_surge": is_price_surge,
                    "daily_macd_hist": round(float(hist.iloc[-1]), 4),
                    "daily_macd_ok": True,
                    "open":  fri_open,   # 금요일 시가 (매수가)
                    "close": fri_close,  # 금요일 종가 (매도가)
                    "thu_close": float(close.iloc[-1]),  # 목요일 종가
                })
        except Exception:
            continue

    results.sort(key=lambda x: x["change_pct"], reverse=True)
    print(f"  일봉 MACD 필터로 제외: {macd_filtered_out}종목")
    print(f"  신호 종목 (MACD 통과): {len(results)}개")
    return results


# ── 4. Claude Hot List 선정 ──────────────────────────────────

def select_hot_list(candidates: list[dict], global_ctx: dict, domestic_ctx: dict) -> list[dict]:
    import anthropic
    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    top = candidates[:20]
    prompt = f"""
2026-04-10(금) 국내 주식 시뮬레이션 분석.

글로벌: S&P500 {global_ctx['sp500_change']:+.2f}%, VIX {global_ctx['vix']:.1f}, USD/KRW {global_ctx['usdkrw']:.0f}
국내: KOSPI {domestic_ctx['kospi_change']:+.2f}%, KOSDAQ {domestic_ctx['kosdaq_change']:+.2f}%

신호 발생 종목 (상위 20개):
{json.dumps(top, ensure_ascii=False, indent=2)}

위 종목 중 단타 매수 추천 5개를 선정해주세요.
RSI > 75이거나 이미 급등 후 고점 가능성이 있는 종목은 제외.

JSON 형식으로만 응답:
{{"hot_list": ["종목코드1", "종목코드2", ...]}}
"""
    try:
        resp = client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=256,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        selected = result.get("hot_list", [])
        return [c for c in candidates if c["ticker"] in selected]
    except Exception as e:
        print(f"  Claude 오류: {e} — 상위 5개 자동 선정")
        return candidates[:5]


# ── 5. 오프닝 게이트 판단 (9:00 기준) ──────────────────────────

def evaluate_opening_gate(global_ctx: dict, domestic_ctx: dict) -> dict:
    """
    09:00 오프닝 게이트: 시장이 '진짜 좋은' 상황인지 Claude가 판단.
    좋으면 즉시 매수, 아니면 9:10까지 대기.
    """
    import anthropic
    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    # 금요일 시장 상황 시뮬레이션
    # 실제론 9시 전에 알 수 있는 정보만 사용
    vix     = global_ctx["vix"]
    sp_chg  = global_ctx["sp500_change"]
    usd_krw = global_ctx["usdkrw"]
    k_chg   = domestic_ctx["kospi_change"]
    q_chg   = domestic_ctx["kosdaq_change"]

    # market_score 간이 계산 (국내 시황팀 로직 모사)
    market_score = (k_chg * 0.6 + q_chg * 0.4) / 2.0

    # global_risk_score 간이 계산
    global_risk = 5
    if vix < 15:     global_risk = 2
    elif vix < 20:   global_risk = 3
    elif vix < 25:   global_risk = 4
    elif vix < 30:   global_risk = 5
    else:            global_risk = 7

    if sp_chg < -1.5: global_risk = min(10, global_risk + 2)
    elif sp_chg > 1.5: global_risk = max(1, global_risk - 1)

    # risk_level 간이 계산
    if global_risk <= 2: risk_level = 1
    elif global_risk <= 4: risk_level = 2
    elif global_risk <= 6: risk_level = 3
    else: risk_level = 4

    prompt = f"""당신은 국내 주식 퀀트 트레이더입니다.
지금 오전 9:00, 장이 막 열렸습니다. 오늘 즉시 매수해도 될지 판단하세요.

## 현재 시장 상황 (목요일 종가 기준)
- S&P500 전일 변동: {sp_chg:+.2f}%
- VIX: {vix:.1f}
- USD/KRW: {usd_krw:.0f}
- KOSPI 전일 변동: {k_chg:+.2f}%
- KOSDAQ 전일 변동: {q_chg:+.2f}%
- 국내 시황 점수: {market_score:+.2f}
- 글로벌 리스크: {global_risk}/10
- 리스크 레벨: {risk_level}/5

## 즉시 매수 기준 (모두 충족 시)
- 글로벌 리스크 ≤ 3
- 리스크 레벨 ≤ 2
- 국내 시황 점수 ≥ +0.3

## 판단
위 기준을 충족하면 "immediate" (9:00 즉시 매수),
아니면 "wait" (9:10까지 관망).

JSON만 응답:
{{"decision": "immediate"|"wait", "reason": "<근거 30자 이내>", "risk_level": {risk_level}, "global_risk": {global_risk}, "market_score": {market_score:.2f}}}"""

    try:
        resp = client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=128,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        result["market_score"] = market_score
        result["global_risk"]  = global_risk
        result["risk_level"]   = risk_level
        return result
    except Exception as e:
        print(f"  오프닝 게이트 Claude 오류: {e}")
        return {
            "decision": "wait",
            "reason": "판단 불가 — 관망",
            "market_score": market_score,
            "global_risk": global_risk,
            "risk_level": risk_level,
        }


# ── 6. 수익률 계산 ───────────────────────────────────────────

def calc_pnl(hot_list: list[dict]) -> list[dict]:
    """시가 매수 → 종가 매도 수익률."""
    results = []
    for s in hot_list:
        buy_price  = s["open"]   # 시가 매수
        sell_price = s["close"]  # 종가 매도
        if buy_price > 0:
            pnl = (sell_price / buy_price - 1) * 100
            results.append({**s, "pnl_pct": round(pnl, 2)})
    return results


# ── 메인 ─────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  DQT 시뮬레이션 — 목요일({SIGNAL_DATE}) 신호 → 금요일({TRADE_DATE}) 매매")
    print("=" * 60)

    print("\n[1] 글로벌 시황 수집")
    global_ctx = get_global_context()

    print("\n[2] 국내 시황 수집")
    domestic_ctx = get_domestic_context()

    print("\n[3] 종목 스캔")
    candidates = scan_universe()
    if not candidates:
        print("  신호 종목 없음 — 종료")
        return

    print("\n[4] 오프닝 게이트 판단 (09:00 기준)")
    gate = evaluate_opening_gate(global_ctx, domestic_ctx)
    gate_emoji = "🟢" if gate["decision"] == "immediate" else "🟡"
    print(f"  {gate_emoji} 결정: {gate['decision'].upper()} — {gate['reason']}")
    print(f"     (리스크레벨 {gate['risk_level']}/5 | 글로벌리스크 {gate['global_risk']}/10 | 시황점수 {gate['market_score']:+.2f})")
    if gate["decision"] == "wait":
        print("  → 9:10까지 관망 후 재판단 (시뮬레이션에서는 9:10 시가로 매수)")

    print("\n[5] Claude Hot List 선정 (일봉 MACD 통과 종목 대상)")
    hot_list = select_hot_list(candidates, global_ctx, domestic_ctx)
    print(f"  선정: {[s['ticker'] for s in hot_list]}")

    print(f"\n[6] 시뮬레이션 결과")
    print(f"    신호 기준: 목요일({SIGNAL_DATE}) 종가")
    print(f"    매수: 금요일({TRADE_DATE}) 시가  |  매도: 금요일 종가")
    print(f"    오프닝 게이트: {gate['decision'].upper()} ({gate['reason']})")
    pnl_list = calc_pnl(hot_list)
    total_pnl = sum(p["pnl_pct"] for p in pnl_list) / len(pnl_list) if pnl_list else 0.0

    print()
    for p in sorted(pnl_list, key=lambda x: x["pnl_pct"], reverse=True):
        emoji = "📈" if p["pnl_pct"] >= 0 else "📉"
        signal = []
        if p["is_volume_surge"]: signal.append(f"거래량{p['vol_ratio']}x")
        if p["is_price_surge"]:  signal.append(f"급등{p['change_pct']:+.1f}%")
        if p["is_breakout"]:     signal.append("BB돌파")
        sig_str = "/".join(signal)
        print(f"  {emoji} {p['ticker']} {p['name']}")
        print(f"       목요일신호: {sig_str}  RSI {p['rsi']}  일봉MACD히스트 {p.get('daily_macd_hist', 'N/A')}")
        print(f"       금요일 시가 {p['open']:,.0f}원 → 종가 {p['close']:,.0f}원  {p['pnl_pct']:+.2f}%")

    print()
    avg_emoji = "📈" if total_pnl >= 0 else "📉"
    print(f"  {avg_emoji} 평균 수익률: {total_pnl:+.2f}%")
    print("=" * 60)

    # 텔레그램 발송
    lines = [
        "🧪 <b>DQT 시뮬레이션 v2 — MACD필터 + 오프닝게이트</b>",
        f"📅 신호: {SIGNAL_DATE} | 매매: {TRADE_DATE}",
        f"목요일 KOSPI {domestic_ctx['kospi_change']:+.2f}% | VIX {global_ctx['vix']:.1f}",
        f"{gate_emoji} 오프닝게이트: {gate['decision'].upper()} — {gate['reason']}",
        "",
        "<b>📋 추천 종목 (일봉MACD통과 | 금요일 시가→종가)</b>",
    ]
    for p in sorted(pnl_list, key=lambda x: x["pnl_pct"], reverse=True):
        e = "▲" if p["pnl_pct"] >= 0 else "▼"
        signal = []
        if p["is_volume_surge"]: signal.append(f"거래량{p['vol_ratio']}x")
        if p["is_price_surge"]:  signal.append(f"급등{p['change_pct']:+.1f}%")
        if p["is_breakout"]:     signal.append("BB돌파")
        lines.append(f"  {e} {p['ticker']} {p['name']}: {p['pnl_pct']:+.2f}%  ({'/'.join(signal)})")
    lines.append(f"\n{avg_emoji} <b>평균: {total_pnl:+.2f}%</b>")
    notify("\n".join(lines))
    print("\n텔레그램 발송 완료!")


if __name__ == "__main__":
    main()
