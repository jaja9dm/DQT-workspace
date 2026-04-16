"""
engine.py — 연구소 엔진

역할:
  장 마감 후 전략별 성과를 분석하고, 백테스트로 임계값을 조정하여
  active_strategies 테이블을 업데이트한다.
  Claude Opus를 사용해 심층 전략 분석을 수행한다.

실행 시점:
  - 장 마감 배치: 17:00 이후 (리포트팀 이후 실행)
  - 주기: 매일 (일일 전략 업데이트) + 주 1회 심층 백테스트

모델: claude-sonnet-4-6 (비용 최적화 — 심층 분석 충분)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import FinanceDataReader as fdr
import anthropic
import pandas as pd

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)
_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

# 기본 전략 정의 (시스템 시작 시 없으면 초기화)
_DEFAULT_STRATEGIES = [
    {
        "strategy_id": "volume_surge_momentum",
        "name": "거래량 급등 모멘텀",
        "conditions": json.dumps({
            "volume_ratio_min": 3.0,
            "price_change_min": 0.0,
            "rsi_max": 70,
            "bb_position_max": 0.9,
        }, ensure_ascii=False),
        "parameters": json.dumps({
            "volume_ratio_threshold": 3.0,
            "price_surge_pct": 3.0,
            "stop_loss_pct": 5.0,
            "take_profit_1_pct": 5.0,
            "take_profit_2_pct": 10.0,
        }, ensure_ascii=False),
    },
    {
        "strategy_id": "bb_breakout",
        "name": "볼린저밴드 상단 돌파",
        "conditions": json.dumps({
            "bb_breakout": True,
            "rsi_min": 55,
            "macd_hist_positive": True,
        }, ensure_ascii=False),
        "parameters": json.dumps({
            "bb_std": 2.0,
            "bb_period": 20,
            "stop_loss_pct": 3.0,
            "take_profit_1_pct": 5.0,
        }, ensure_ascii=False),
    },
    {
        "strategy_id": "macd_momentum",
        "name": "MACD 모멘텀 전환",
        "conditions": json.dumps({
            "macd_hist_positive": True,
            "above_ma20": True,
            "rsi_range": [40, 65],
        }, ensure_ascii=False),
        "parameters": json.dumps({
            "macd_fast": 12,
            "macd_slow": 26,
            "macd_signal": 9,
            "stop_loss_pct": 5.0,
        }, ensure_ascii=False),
    },
]


class ResearchEngine:
    """연구소 엔진 — 배치 전용 (스케줄러 호출)."""

    def run(self, deep: bool = False) -> dict:
        """
        일일 전략 성과 분석 및 파라미터 업데이트.

        Args:
            deep: True이면 6개월 백테스트 포함 (주 1회)

        Returns:
            분석 결과 딕셔너리
        """
        logger.info(f"연구소 분석 시작 (deep={deep})")

        # 전략 초기화 (없으면 기본값 삽입)
        _init_strategies()

        # 1. 최근 30일 전략별 성과 집계
        perf = _collect_strategy_performance()

        # 2. Claude Opus 심층 분석
        recommendations = _ask_claude_opus(perf, deep=deep)

        # 3. 전략 파라미터 업데이트
        updated = _apply_recommendations(recommendations)

        # 4. 주간 백테스트 (deep 모드)
        backtest_summary = {}
        if deep:
            backtest_summary = _run_backtest()

        result = {
            "date": str(date.today()),
            "strategies_analyzed": len(perf),
            "strategies_updated": updated,
            "recommendations": recommendations,
            "backtest": backtest_summary,
        }

        # 5. 텔레그램 알림 (주요 변경사항만)
        if updated > 0 or backtest_summary:
            _send_research_report(result)

        logger.info(f"연구소 분석 완료 — {updated}개 전략 업데이트")
        return result


# ──────────────────────────────────────────────
# 전략 성과 집계
# ──────────────────────────────────────────────

def _collect_strategy_performance() -> list[dict]:
    """최근 30일 trades 테이블에서 전략별 성과 집계."""
    strategies = fetch_all("SELECT * FROM active_strategies WHERE status = 'active'")
    result = []

    for strat in strategies:
        sid = strat["strategy_id"]
        name = strat["name"]

        # 해당 전략 signal_source로 기록된 거래 (signal_source = strategy_id 약속)
        trades = fetch_all(
            """
            SELECT pnl_pct, pnl, action, date FROM trades
            WHERE signal_source LIKE ? AND status = 'filled'
              AND date >= date('now', '-30 days')
              AND action IN ('sell', 'stop_loss', 'take_profit', 'time_cut')
            ORDER BY date DESC
            """,
            (f"%{sid}%",),
        )

        if not trades:
            result.append({
                "strategy_id": sid,
                "name": name,
                "trade_count": 0,
                "win_rate": 0.0,
                "avg_pnl_pct": 0.0,
                "profit_factor": 0.0,
                "current_params": json.loads(strat["parameters"] or "{}"),
            })
            continue

        win = sum(1 for t in trades if float(t["pnl_pct"] or 0) > 0)
        total = len(trades)
        avg_pnl = sum(float(t["pnl_pct"] or 0) for t in trades) / total

        gains = [float(t["pnl"] or 0) for t in trades if float(t["pnl"] or 0) > 0]
        losses = [abs(float(t["pnl"] or 0)) for t in trades if float(t["pnl"] or 0) < 0]
        pf = sum(gains) / sum(losses) if losses else 0.0

        result.append({
            "strategy_id": sid,
            "name": name,
            "trade_count": total,
            "win_rate": round(win / total * 100, 1),
            "avg_pnl_pct": round(avg_pnl, 3),
            "profit_factor": round(pf, 2),
            "current_params": json.loads(strat["parameters"] or "{}"),
        })

    return result


# ──────────────────────────────────────────────
# Claude Opus 심층 분석
# ──────────────────────────────────────────────

def _ask_claude_opus(perf: list[dict], deep: bool = False) -> list[dict]:
    """
    Claude Opus에 전략별 성과를 보내 파라미터 조정 권고를 받는다.

    Returns:
        [{"strategy_id", "action": "keep"|"adjust"|"deprecate", "params", "reason"}]
    """
    if not perf:
        return []

    perf_text = "\n".join([
        f"- {p['name']} ({p['strategy_id']}): "
        f"거래 {p['trade_count']}건 | 승률 {p['win_rate']:.1f}% | "
        f"평균손익 {p['avg_pnl_pct']:+.2f}% | 손익비 {p['profit_factor']:.2f} | "
        f"현재파라미터: {json.dumps(p['current_params'], ensure_ascii=False)}"
        for p in perf
    ])

    prompt = f"""당신은 퀀트 전략 연구원입니다.
아래 전략별 최근 30일 성과를 분석하고, 각 전략의 파라미터 조정 여부를 판단하세요.

## 전략별 성과
{perf_text}

## 판단 기준
- 승률 < 40% & 거래 ≥ 5건: 파라미터 조정 또는 deprecated 고려
- 손익비 < 1.0 & 거래 ≥ 5건: 손절/익절 비율 재검토
- 거래 0건: 신호 조건이 너무 엄격 — 완화 검토
- 승률 > 60% & 손익비 > 1.5: 현행 유지 (keep)

## 응답 형식 (JSON만)
{{
  "recommendations": [
    {{
      "strategy_id": "<id>",
      "action": "<keep|adjust|deprecate>",
      "reason": "<판단 근거 30자 이내>",
      "params": {{<조정된 파라미터 딕셔너리, keep이면 현행 그대로>}}
    }}
  ]
}}"""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,  # Sonnet — 비용 최적화
            max_tokens=1024,
            temperature=settings.CLAUDE_TEMPERATURE,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw).get("recommendations", [])
    except Exception as e:
        logger.error(f"Claude 분석 오류: {e}")
        return [{"strategy_id": p["strategy_id"], "action": "keep",
                 "reason": "분석 실패 — 현행 유지", "params": p["current_params"]}
                for p in perf]


# ──────────────────────────────────────────────
# 파라미터 적용
# ──────────────────────────────────────────────

def _apply_recommendations(recommendations: list[dict]) -> int:
    """Claude 권고에 따라 active_strategies 테이블 업데이트."""
    updated = 0
    for rec in recommendations:
        sid = rec.get("strategy_id")
        action = rec.get("action", "keep")
        params = rec.get("params", {})
        reason = rec.get("reason", "")

        if action == "deprecate":
            execute(
                "UPDATE active_strategies SET status='deprecated', updated_at=? WHERE strategy_id=?",
                (datetime.now().isoformat(), sid),
            )
            logger.info(f"전략 deprecated: {sid} — {reason}")
            updated += 1
        elif action == "adjust" and params:
            execute(
                "UPDATE active_strategies SET parameters=?, updated_at=? WHERE strategy_id=?",
                (json.dumps(params, ensure_ascii=False), datetime.now().isoformat(), sid),
            )
            logger.info(f"전략 파라미터 조정: {sid} — {reason}")
            updated += 1

    return updated


# ──────────────────────────────────────────────
# 백테스트 (deep 모드, 주 1회)
# ──────────────────────────────────────────────

def _run_backtest() -> dict:
    """
    거래량 급등 모멘텀 전략 6개월 백테스트.
    KOSPI 200 상위 20종목 대상으로 단순화 실행.
    """
    logger.info("6개월 백테스트 시작")
    end = date.today()
    start = end - timedelta(days=180)

    # 테스트 대상: 유니버스 중 최근 거래 종목
    rows = fetch_all(
        "SELECT DISTINCT ticker FROM trades WHERE date >= ? LIMIT 20",
        (str(start),),
    )
    tickers = [r["ticker"] for r in rows]
    if not tickers:
        logger.info("백테스트 대상 종목 없음")
        return {}

    results = []
    for ticker in tickers[:10]:  # 시간 제한: 최대 10종목
        try:
            df = fdr.DataReader(ticker, start, end)
            if df.empty or len(df) < 60:
                continue

            # 단순 거래량 급등 전략 백테스트
            closes = df["Close"].astype(float)
            volumes = df["Volume"].astype(float)
            avg_vol = volumes.rolling(20).mean()
            vol_ratio = volumes / avg_vol

            signals = vol_ratio >= 3.0  # 거래량 3배 이상
            pnl_list = []

            for i in range(20, len(df) - 5):
                if signals.iloc[i]:
                    entry = closes.iloc[i]
                    exit_price = closes.iloc[i + 5]  # 5일 후 청산 단순화
                    pnl_list.append((exit_price - entry) / entry * 100)

            if pnl_list:
                win = sum(1 for p in pnl_list if p > 0)
                results.append({
                    "ticker": ticker,
                    "trades": len(pnl_list),
                    "win_rate": round(win / len(pnl_list) * 100, 1),
                    "avg_pnl": round(sum(pnl_list) / len(pnl_list), 3),
                })
        except Exception as e:
            logger.debug(f"백테스트 오류 [{ticker}]: {e}")

    if not results:
        return {}

    avg_wr = sum(r["win_rate"] for r in results) / len(results)
    avg_pnl = sum(r["avg_pnl"] for r in results) / len(results)

    summary = {
        "period": f"{start} ~ {end}",
        "tickers": len(results),
        "avg_win_rate": round(avg_wr, 1),
        "avg_pnl_pct": round(avg_pnl, 3),
        "details": results,
    }

    # 백테스트 결과 win_rate를 active_strategies에 반영
    execute(
        "UPDATE active_strategies SET win_rate=?, updated_at=? WHERE strategy_id=?",
        (avg_wr / 100, datetime.now().isoformat(), "volume_surge_momentum"),
    )

    logger.info(f"백테스트 완료 — 평균 승률 {avg_wr:.1f}% | 평균 손익 {avg_pnl:+.3f}%")
    return summary


# ──────────────────────────────────────────────
# 텔레그램 리포트
# ──────────────────────────────────────────────

def _send_research_report(result: dict) -> None:
    recs = result.get("recommendations", [])
    adjusted = [r for r in recs if r.get("action") == "adjust"]
    deprecated = [r for r in recs if r.get("action") == "deprecate"]

    lines = [f"🔬 <b>연구소 일일 리포트 — {result['date']}</b>", ""]

    if adjusted:
        lines.append(f"📐 파라미터 조정: {len(adjusted)}건")
        for r in adjusted:
            lines.append(f"  • {r['strategy_id']}: {r.get('reason','')}")

    if deprecated:
        lines.append(f"🗑 전략 비활성화: {len(deprecated)}건")
        for r in deprecated:
            lines.append(f"  • {r['strategy_id']}: {r.get('reason','')}")

    bt = result.get("backtest", {})
    if bt:
        lines.append(
            f"\n📊 백테스트 ({bt.get('period','')}) — "
            f"승률 {bt.get('avg_win_rate',0):.1f}% | "
            f"평균손익 {bt.get('avg_pnl_pct',0):+.3f}%"
        )

    notify("\n".join(lines))


# ──────────────────────────────────────────────
# 전략 초기화
# ──────────────────────────────────────────────

def _init_strategies() -> None:
    """active_strategies가 비어있으면 기본 전략 삽입."""
    rows = fetch_all("SELECT COUNT(*) as cnt FROM active_strategies")
    if rows and int(rows[0]["cnt"]) > 0:
        return

    for strat in _DEFAULT_STRATEGIES:
        execute(
            """
            INSERT OR IGNORE INTO active_strategies
                (strategy_id, name, conditions, parameters, status)
            VALUES (?, ?, ?, ?, 'active')
            """,
            (strat["strategy_id"], strat["name"],
             strat["conditions"], strat["parameters"]),
        )
    logger.info(f"기본 전략 {len(_DEFAULT_STRATEGIES)}개 초기화 완료")
