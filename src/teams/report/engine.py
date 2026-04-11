"""
engine.py — 리포트팀 엔진

역할:
  장 마감 후 당일 거래 이력과 포지션 스냅샷을 집계하여
  일일 성과 리포트를 생성하고 텔레그램으로 발송한다.

실행 시점:
  - 장 마감 배치: 15:35 이후 (스케줄러가 호출, 또는 수동 호출)
  - 주기적 실행 없음 — 배치 전용 엔진

리포트 항목:
  - 당일 총 손익 (%, 금액)
  - 거래 건수 / 승률 / 손익비
  - 종목별 손익 내역
  - 리스크 레벨 이력
  - Hot List 선정 vs 실제 성과 비교
  - 텔레그램 발송
"""

from __future__ import annotations

import json
from datetime import date, datetime

from src.infra.database import fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify_daily_report, notify_error

logger = get_logger(__name__)


class ReportEngine:
    """리포트팀 엔진 — 배치 전용 (스케줄러 또는 수동 호출)."""

    def run(self, target_date: date | None = None) -> dict:
        """
        일일 성과 리포트 생성 및 텔레그램 발송.

        Args:
            target_date: 집계 대상 날짜 (None이면 오늘)

        Returns:
            리포트 딕셔너리
        """
        target = target_date or date.today()
        logger.info(f"일일 리포트 생성 시작 — {target}")

        try:
            report = _build_report(target)
            notify_daily_report(report)
            logger.info(
                f"리포트 발송 완료 — "
                f"손익 {report['total_pnl_pct']:+.2f}% | "
                f"거래 {report['trade_count']}건 | "
                f"승률 {report['win_rate']:.1f}%"
            )
            return report
        except Exception as e:
            logger.error(f"리포트 생성 오류: {e}", exc_info=True)
            notify_error("리포트팀", str(e))
            return {}


# ──────────────────────────────────────────────
# 리포트 데이터 수집 및 계산
# ──────────────────────────────────────────────

def _build_report(target: date) -> dict:
    """trades + position_snapshot + risk_status DB에서 일일 성과 집계."""
    date_str = str(target)

    # 1. 당일 거래 이력
    trades = fetch_all(
        """
        SELECT ticker, name, action, exec_price, quantity,
               tranche, status, pnl, pnl_pct, signal_source, created_at
        FROM trades
        WHERE date = ? AND status IN ('filled', 'pending')
        ORDER BY created_at
        """,
        (date_str,),
    )

    # 2. 종목별 손익 집계
    position_pnl: dict[str, dict] = {}
    trade_count = 0
    win_count = 0
    loss_count = 0
    total_pnl = 0.0

    for t in trades:
        ticker = t["ticker"]
        action = t["action"]
        pnl_pct = float(t["pnl_pct"] or 0)
        pnl_amt = float(t["pnl"] or 0)

        if action in ("sell", "stop_loss", "take_profit", "time_cut"):
            trade_count += 1
            total_pnl += pnl_amt

            if ticker not in position_pnl:
                position_pnl[ticker] = {
                    "ticker": ticker,
                    "name": t["name"] or "",
                    "pnl_pct": pnl_pct,
                    "pnl_amt": pnl_amt,
                    "action": action,
                }
            else:
                position_pnl[ticker]["pnl_amt"] += pnl_amt
                position_pnl[ticker]["pnl_pct"] = (
                    position_pnl[ticker]["pnl_pct"] + pnl_pct
                ) / 2

            if pnl_pct > 0:
                win_count += 1
            elif pnl_pct < 0:
                loss_count += 1

    win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0.0
    profit_factor = _calc_profit_factor(trades)

    # 3. 포트폴리오 전체 손익률 (포지션 스냅샷 기준)
    total_pnl_pct = _calc_portfolio_pnl_pct(date_str, total_pnl)

    # 4. 리스크 레벨 이력 (당일 최고 레벨)
    risk_rows = fetch_all(
        """
        SELECT risk_level, active_alerts FROM risk_status
        WHERE date(created_at) = ?
        ORDER BY risk_level DESC LIMIT 1
        """,
        (date_str,),
    )
    max_risk_level = int(risk_rows[0]["risk_level"]) if risk_rows else 1
    risk_alerts = json.loads(risk_rows[0]["active_alerts"] or "[]") if risk_rows else []

    # 5. Hot List 성과 비교
    hot_list_stats = _calc_hot_list_accuracy(date_str)

    # 6. 경보 목록
    alerts = []
    if total_pnl_pct <= -3.0:
        alerts.append(f"당일 손실 {total_pnl_pct:.2f}% — 익일 보수적 운용 권고")
    if max_risk_level >= 4:
        alerts.append(f"최고 리스크 레벨 {max_risk_level} 도달")
    if win_rate < 40 and trade_count >= 3:
        alerts.append(f"승률 {win_rate:.1f}% — 전략 검토 필요")
    alerts.extend(risk_alerts[:2])

    report = {
        "date": date_str,
        "total_pnl_pct": round(total_pnl_pct, 3),
        "total_pnl_amt": round(total_pnl, 0),
        "trade_count": trade_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": round(win_rate, 1),
        "profit_factor": round(profit_factor, 2),
        "max_risk_level": max_risk_level,
        "hot_list_accuracy": hot_list_stats,
        "positions": sorted(
            position_pnl.values(),
            key=lambda x: x["pnl_pct"],
            reverse=True,
        ),
        "alerts": alerts,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    return report


def _calc_portfolio_pnl_pct(date_str: str, realized_pnl: float) -> float:
    """
    당일 총 손익률 계산.
    장 마감 시점 position_snapshot 기준 평가손익 + 실현손익.
    """
    try:
        # 당일 마지막 스냅샷
        row = fetch_one(
            """
            SELECT SUM(current_price * quantity) as eval_amt,
                   SUM(avg_price * quantity) as cost_amt
            FROM position_snapshot
            WHERE date(snapshot_at) = ?
              AND snapshot_at = (
                SELECT MAX(snapshot_at) FROM position_snapshot WHERE date(snapshot_at) = ?
              )
            """,
            (date_str, date_str),
        )
        if not row or not row["cost_amt"]:
            # 스냅샷 없으면 실현손익만으로 추정
            return 0.0
        eval_amt = float(row["eval_amt"] or 0)
        cost_amt = float(row["cost_amt"] or 0)
        unrealized = eval_amt - cost_amt
        total = realized_pnl + unrealized
        return (total / cost_amt * 100) if cost_amt > 0 else 0.0
    except Exception:
        return 0.0


def _calc_profit_factor(trades: list) -> float:
    """손익비 (총 이익 / 총 손실 절댓값) 계산."""
    total_profit = sum(
        float(t["pnl"] or 0)
        for t in trades
        if float(t["pnl"] or 0) > 0
    )
    total_loss = abs(sum(
        float(t["pnl"] or 0)
        for t in trades
        if float(t["pnl"] or 0) < 0
    ))
    return (total_profit / total_loss) if total_loss > 0 else 0.0


def _calc_hot_list_accuracy(date_str: str) -> dict:
    """Hot List에 올랐던 종목이 실제로 수익을 냈는지 비교."""
    try:
        hot_tickers = fetch_all(
            "SELECT DISTINCT ticker FROM hot_list WHERE date(created_at) = ?",
            (date_str,),
        )
        hot_set = {r["ticker"] for r in hot_tickers}
        if not hot_set:
            return {"total": 0, "traded": 0, "win": 0}

        traded = fetch_all(
            f"""
            SELECT ticker, pnl_pct FROM trades
            WHERE date = ? AND action IN ('sell','stop_loss','take_profit','time_cut')
              AND ticker IN ({','.join('?' * len(hot_set))})
            """,
            (date_str, *hot_set),
        )
        win = sum(1 for t in traded if float(t["pnl_pct"] or 0) > 0)
        return {
            "total": len(hot_set),
            "traded": len(traded),
            "win": win,
        }
    except Exception:
        return {"total": 0, "traded": 0, "win": 0}
