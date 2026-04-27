"""
engine.py — 일일 매매 복기 팀

역할:
  매일 장 마감 후 16:30에 자동 실행.
  오늘 발생한 모든 매매를 분석하고 무엇이 잘 됐는지, 무엇을 고쳐야 하는지
  Claude가 판단해 trade_review 테이블에 저장하고 Telegram으로 리포트한다.

시황 맥락 기능:
  - 당일 KOSPI/KOSDAQ 등락, 외인/기관 매매, 글로벌 리스크를 복기에 포함
  - market_regime 태그 (강세_외인주도 등) 저장 — 유사 시황에서 과거 대응 참조 가능
  - "이런 시장 상황에서 이런 전략이 효과적이었다"는 패턴 누적

출력:
  - trade_review 테이블에 당일 복기 레코드 저장 (market_context JSON 포함)
  - Telegram 복기 요약 발송
  - 연구소가 내일 전략 파라미터 조정에 참고

자기 개선 흐름:
  오늘 복기 → improvements JSON → 내일 _ask_claude / opening_gate 프롬프트 참고
  → 장기적으로 전략 파라미터 자동 튜닝의 기초 데이터로 활용
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_KIS_BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
_KIS_CASH_PATH    = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"


def _extract_json(raw: str) -> str:
    """Claude 응답에서 JSON 블록 추출. 코드 펜스·앞뒤 텍스트 제거."""
    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", raw)
    if m:
        return m.group(1)
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1:
        return raw[start : end + 1]
    return raw


# ──────────────────────────────────────────────
# KIS 잔고 / 평가금액 조회
# ──────────────────────────────────────────────

def _fetch_portfolio_summary() -> dict:
    """
    장 마감 후 KIS API로 평가금액·예수금·보유종목 조회.

    Returns:
        {
          "total_eval_amt": float,   # 총 평가금액 (보유주식 시가 + 예수금)
          "stock_eval_amt": float,   # 주식 평가금액
          "available_cash": float,   # 주문 가능 예수금
          "total_pnl_amt":  float,   # 평가 손익 합계
          "total_pnl_pct":  float,   # 평가 손익률 (%)
          "positions": [{"ticker", "name", "quantity", "avg_price",
                         "current_price", "pnl_pct"}, ...]
        }
    """
    _empty = {
        "total_eval_amt": 0.0,
        "stock_eval_amt": 0.0,
        "available_cash": 0.0,
        "total_pnl_amt":  0.0,
        "total_pnl_pct":  0.0,
        "positions": [],
    }
    try:
        from src.infra.kis_gateway import KISGateway
        from src.infra.rate_limiter import RequestPriority
        gw = KISGateway()
        acnt_no, acnt_prdt_cd = (settings.KIS_ACCOUNT_NO.split("-") + ["01"])[:2]
        tr_id = "VTTC8434R" if settings.KIS_MODE == "paper" else "TTTC8434R"

        resp = gw.request(
            method="GET",
            path=_KIS_BALANCE_PATH,
            params={
                "CANO": acnt_no,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "01",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
            tr_id=tr_id,
            priority=RequestPriority.DATA_COLLECTION,
        )

        output1 = resp.get("output1", [])
        output2 = resp.get("output2", [{}])
        summary = output2[0] if output2 else {}

        stock_eval  = float(summary.get("evlu_amt_smtl_amt", 0) or 0)
        total_eval  = float(summary.get("tot_evlu_amt", 0) or 0)
        total_pnl   = float(summary.get("evlu_pfls_smtl_amt", 0) or 0)
        purchase_amt = float(summary.get("pchs_amt_smtl_amt", 0) or stock_eval)
        total_pnl_pct = (total_pnl / purchase_amt * 100) if purchase_amt > 0 else 0.0

        # 예수금 (d+2 기준 실제 사용 가능 금액)
        available_cash = total_eval - stock_eval

        positions = []
        for item in output1:
            qty = int(item.get("hldg_qty", 0) or 0)
            if qty == 0:
                continue
            positions.append({
                "ticker":        item.get("pdno", ""),
                "name":          item.get("prdt_name", ""),
                "quantity":      qty,
                "avg_price":     float(item.get("pchs_avg_pric", 0) or 0),
                "current_price": float(item.get("prpr", 0) or 0),
                "pnl_pct":       float(item.get("evlu_pfls_rt", 0) or 0),
                "eval_amt":      float(item.get("evlu_amt", 0) or 0),
            })

        return {
            "total_eval_amt": total_eval,
            "stock_eval_amt": stock_eval,
            "available_cash": available_cash,
            "total_pnl_amt":  total_pnl,
            "total_pnl_pct":  round(total_pnl_pct, 3),
            "positions":      positions,
        }
    except Exception as e:
        logger.warning(f"포트폴리오 잔고 조회 실패: {e}")
        return _empty


# ──────────────────────────────────────────────
# 보조 분석 함수
# ──────────────────────────────────────────────

def _calc_profit_factor(sell_trades: list[dict]) -> float:
    """Profit Factor = 총 이익합 / 총 손실합 절대값. 1.5↑ 양호, 1.0↓ 위험."""
    total_profit = sum((t.get("pnl") or 0) for t in sell_trades if (t.get("pnl") or 0) > 0)
    total_loss   = abs(sum((t.get("pnl") or 0) for t in sell_trades if (t.get("pnl") or 0) < 0))
    return round(total_profit / total_loss, 2) if total_loss > 0 else float("inf") if total_profit > 0 else 0.0


def _calc_avg_hold_minutes(trades: list[dict]) -> float:
    """매수 → 매도 평균 보유 시간(분). 종목별로 페어링 후 평균."""
    _SELL_ACTIONS = ("sell", "stop_loss", "take_profit", "time_cut", "partial_exit", "force_close")
    buy_time: dict[str, datetime] = {}
    hold_minutes: list[float] = []

    for t in sorted(trades, key=lambda x: x.get("created_at") or ""):
        tk = t["ticker"]
        try:
            ts = datetime.fromisoformat(str(t.get("created_at") or ""))
        except Exception:
            continue
        if t["action"] == "buy" and tk not in buy_time:
            buy_time[tk] = ts
        elif t["action"] in _SELL_ACTIONS and tk in buy_time:
            diff = (ts - buy_time[tk]).total_seconds() / 60
            if 0 < diff < 480:
                hold_minutes.append(diff)
            del buy_time[tk]

    return round(sum(hold_minutes) / len(hold_minutes), 1) if hold_minutes else 0.0


def _calc_hot_list_efficiency(today: str, bought_tickers: set) -> dict:
    """
    오늘 hot_list에 올라온 종목 수 vs 실제 매수 수.
    '놓친 기회' — 매수하지 않은 hot_list 종목 중 등락률 상위.

    Returns:
        {"scanned": int, "bought": int, "conversion_pct": float,
         "missed_top": [{"ticker", "name", "change_pct", "signal_type"}, ...]}
    """
    rows = fetch_all(
        """
        SELECT ticker, name, price_change_pct, signal_type, momentum_score
        FROM hot_list
        WHERE DATE(created_at) = ?
        ORDER BY momentum_score DESC
        """,
        (today,),
    )
    if not rows:
        return {"scanned": 0, "bought": 0, "conversion_pct": 0.0, "missed_top": []}

    # 중복 제거 (같은 종목이 여러 스캔 사이클에서 올라올 수 있음)
    seen: dict[str, dict] = {}
    for r in rows:
        tk = r["ticker"]
        if tk not in seen:
            seen[tk] = dict(r)

    scanned = len(seen)
    missed = [
        {"ticker": tk, "name": d["name"],
         "change_pct": float(d["price_change_pct"] or 0),
         "signal_type": d["signal_type"] or ""}
        for tk, d in seen.items()
        if tk not in bought_tickers
    ]
    # 등락률 높은 순으로 상위 3개 (놓쳤는데 많이 올라간 것들)
    missed_top = sorted(missed, key=lambda x: x["change_pct"], reverse=True)[:3]

    bought_count = len(bought_tickers & set(seen.keys()))
    conv = round(bought_count / scanned * 100, 1) if scanned else 0.0

    return {
        "scanned":        scanned,
        "bought":         bought_count,
        "conversion_pct": conv,
        "missed_top":     missed_top,
    }


def _calc_consecutive_losses(sell_trades: list[dict]) -> int:
    """시간 순으로 정렬 후 현재 기준 연속 손실 최대 횟수."""
    sorted_sells = sorted(sell_trades, key=lambda x: x.get("created_at") or "")
    max_streak = cur_streak = 0
    for t in sorted_sells:
        if (t.get("pnl_pct") or 0) < 0:
            cur_streak += 1
            max_streak = max(max_streak, cur_streak)
        else:
            cur_streak = 0
    return max_streak


def _calc_tranche_effect(trades: list[dict]) -> dict:
    """
    분할매수(2·3차 매수) 효과 분석.
    2·3차 매수가 있는 종목에서 평균단가 개선폭과 최종 손익 비교.

    Returns:
        {"tickers_with_tranche": int, "avg_improvement_pct": float,
         "tranche2_count": int, "tranche3_count": int}
    """
    buy_trades = [t for t in trades if t["action"] == "buy"]
    t2 = [t for t in buy_trades if (t.get("tranche") or 1) == 2]
    t3 = [t for t in buy_trades if (t.get("tranche") or 1) == 3]
    tickers_with_extra = len({t["ticker"] for t in t2 + t3})
    return {
        "tickers_with_tranche": tickers_with_extra,
        "tranche2_count": len(t2),
        "tranche3_count": len(t3),
    }


def _calc_max_drawdown(today: str) -> float:
    """
    position_snapshot 기반 당일 포트폴리오 최대 미실현 손실률(%).
    각 종목의 최악 스냅샷 손익률을 투자금액 가중 합산.
    (동시 최악 가정 — 보수적 추정)
    """
    rows = fetch_all(
        """
        SELECT ticker, MIN(pnl_pct) AS worst_pnl, avg_price, quantity
        FROM position_snapshot
        WHERE DATE(snapshot_at) = ?
        GROUP BY ticker
        """,
        (today,),
    )
    if not rows:
        return 0.0

    total_invested = sum(float(r["avg_price"] or 0) * int(r["quantity"] or 0) for r in rows)
    if total_invested <= 0:
        return 0.0

    weighted_loss = sum(
        float(r["worst_pnl"] or 0) / 100
        * float(r["avg_price"] or 0) * int(r["quantity"] or 0)
        for r in rows
    )
    return round(weighted_loss / total_invested * 100, 2)


# ──────────────────────────────────────────────
# 메인 진입점
# ──────────────────────────────────────────────

def run_daily_review() -> dict | None:
    """
    오늘의 매매를 복기하고 trade_review에 저장.

    Returns:
        저장된 리뷰 딕셔너리 또는 None (매매 없는 날)
    """
    today = str(date.today())
    logger.info(f"=== 일일 복기 시작: {today} ===")

    # 이미 오늘 복기가 있으면 스킵 (재실행 방지)
    existing = fetch_one("SELECT id FROM trade_review WHERE review_date = ?", (today,))
    if existing:
        logger.info("오늘 복기 이미 완료됨 — 스킵")
        return None

    trades = _load_todays_trades(today)
    if not trades:
        logger.info("오늘 매매 내역 없음 — 복기 스킵")
        notify("📋 <b>[일일 복기]</b> 오늘 매매 없음")
        return None

    # 기초 통계
    stats = _calc_stats(trades)

    # 정량 신호 분석 (자기학습 피드백 루프 핵심)
    signal_analytics = _compute_signal_analytics(today)

    # 포지션 스냅샷 (오늘 보유 종목의 가격 흐름 맥락)
    snapshots = _load_snapshots_context(today, [t["ticker"] for t in trades])

    # 당일 시황 컨텍스트 (KOSPI/KOSDAQ/외인/글로벌 리스크)
    market_ctx = _load_market_context(today)

    # 유사 시황 패턴 과거 복기 (데이터 쌓이면 참조용)
    similar_days = _load_similar_market_days(market_ctx, days_back=60)

    # 추가 분석 지표
    bought_tickers = {t["ticker"] for t in trades if t["action"] == "buy"}
    sell_trades    = [t for t in trades if t["action"] not in ("buy",)]
    extra = {
        "profit_factor":     _calc_profit_factor(sell_trades),
        "avg_hold_minutes":  _calc_avg_hold_minutes(trades),
        "hot_list":          _calc_hot_list_efficiency(today, bought_tickers),
        "consecutive_losses":_calc_consecutive_losses(sell_trades),
        "tranche":           _calc_tranche_effect(trades),
        "max_drawdown_pct":  _calc_max_drawdown(today),
    }

    # 장 마감 후 포트폴리오 잔고 (평가금액 + 예수금)
    portfolio = _fetch_portfolio_summary()

    # Claude 분석
    review = _ask_claude_review(today, trades, stats, snapshots, market_ctx, similar_days, extra)

    # DB 저장
    _save_review(today, stats, review, market_ctx, signal_analytics)

    # 종목별 패턴 메모를 ticker_stats에 반영
    _update_ticker_stats_from_review(stats, review)

    # 오늘 슬롯 배정 현황 (ticker → slot)
    slot_map = _load_slot_map(today)

    # Telegram 발송
    _notify_review(today, stats, review, market_ctx, signal_analytics, extra, portfolio, slot_map)

    logger.info(f"일일 복기 완료 — 매매 {stats['total']}건, 수익 {stats['win']}건, 손실 {stats['loss']}건")
    return review


# ──────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────

def _load_todays_trades(today: str) -> list[dict]:
    """오늘 체결된 매매 내역 전체 조회. trade_context JOIN으로 진입 메타 포함."""
    rows = fetch_all(
        """
        SELECT t.ticker, t.name, t.action, t.order_type, t.exec_price,
               t.quantity, t.pnl, t.pnl_pct, t.signal_source, t.strategy_id,
               t.created_at, t.filled_at,
               COALESCE(tc.signal_type, '') AS signal_type,
               COALESCE(tc.entry_score, 0.0) AS entry_score,
               COALESCE(tc.rsi, 0.0) AS rsi,
               COALESCE(tc.sector, '') AS sector,
               COALESCE(tc.entry_hhmm, '') AS entry_hhmm,
               COALESCE(tc.rs_daily, 0.0) AS rs_daily,
               COALESCE(tc.momentum_score, 0.0) AS momentum_score
        FROM trades t
        LEFT JOIN trade_context tc
            ON tc.ticker = t.ticker AND tc.trade_date = t.date
        WHERE t.date = ?
          AND t.status IN ('filled', 'pending')
        ORDER BY t.created_at ASC
        """,
        (today,),
    )
    return [dict(r) for r in rows] if rows else []


def _compute_signal_analytics(today: str) -> dict:
    """
    오늘 매수 진입 컨텍스트(trade_context)와 매도 결과(trades)를 조인해
    신호 차원별 승률·평균 손익을 정량 계산.

    자기학습 피드백 루프의 핵심 — 이 데이터로 param_tuner가 규칙 기반 조정.

    Returns:
        {
          "by_signal_type": {"gap_up_breakout": {"win": 2, "loss": 1, "avg_pnl": 2.3}, ...},
          "by_rsi_bucket":  {"45-55": {"win": 3, "loss": 0, "avg_pnl": 3.1}, ...},
          "by_score_bucket": {"80+": {"win": 4, "loss": 0}, "50-70": {"win": 1, "loss": 2}, ...},
          "by_sector_hot":  {"hot": {"win": 3, "loss": 0}, "cold": {"win": 0, "loss": 2}, ...},
          "by_entry_hour":  {"09": {"win": 2, "loss": 1}, "10": {"win": 1, "loss": 0}, ...},
          "overall": {"win": 6, "loss": 3, "win_rate": 0.67, "avg_pnl": 2.1},
        }
    """
    # 오늘 trade_context와 매도 결과 조인
    rows = fetch_all(
        """
        SELECT tc.signal_type, tc.rsi, tc.entry_score, tc.sector,
               tc.exec_strength, tc.entry_hhmm,
               s.pnl_pct, s.action AS sell_action
        FROM trade_context tc
        JOIN trades s ON s.ticker = tc.ticker
            AND s.date = tc.trade_date
            AND s.action IN ('sell','stop_loss','take_profit','time_cut','partial_exit')
        WHERE tc.trade_date = ?
        """,
        (today,),
    )

    if not rows:
        return {}

    def _bucket(analytics: dict, key: str, subkey: str, pnl: float) -> None:
        grp = analytics.setdefault(subkey, {}).setdefault(key, {
            "win": 0, "loss": 0, "pnl_sum": 0.0, "count": 0,
            "win_pnl_sum": 0.0, "loss_pnl_sum": 0.0,
        })
        grp["count"] += 1
        grp["pnl_sum"] += pnl
        if pnl > 0:
            grp["win"] += 1
            grp["win_pnl_sum"] += pnl
        else:
            grp["loss"] += 1
            grp["loss_pnl_sum"] += pnl

    analytics: dict = {
        "by_signal_type": {},
        "by_rsi_bucket": {},
        "by_score_bucket": {},
        "by_sector_hot": {},
        "by_entry_hour": {},
    }
    total_win = total_loss = 0
    total_pnl = 0.0

    # 섹터 강/약세 기준 (오늘 sector_strength 기반)
    try:
        from src.infra.sector_rotation import get_hot_sectors, get_cold_sectors
        hot_sectors  = set(get_hot_sectors(3))
        cold_sectors = set(get_cold_sectors(3))
    except Exception:
        hot_sectors = cold_sectors = set()

    for r in rows:
        pnl = float(r["pnl_pct"] or 0.0)
        total_pnl += pnl
        if pnl > 0:
            total_win += 1
        else:
            total_loss += 1

        # 신호 유형별
        sig = r["signal_type"] or "unknown"
        _bucket(analytics, sig, "by_signal_type", pnl)

        # RSI 구간별
        rsi = float(r["rsi"] or 50.0)
        rsi_key = "35-" if rsi < 35 else "35-45" if rsi < 45 else "45-55" if rsi < 55 else "55-65" if rsi < 65 else "65-72" if rsi < 72 else "72+"
        _bucket(analytics, rsi_key, "by_rsi_bucket", pnl)

        # 진입 점수 구간별
        score = float(r["entry_score"] or 0.0)
        score_key = "80+" if score >= 80 else "70-79" if score >= 70 else "60-69" if score >= 60 else "50-59"
        _bucket(analytics, score_key, "by_score_bucket", pnl)

        # 섹터 강/약세별
        sector = r["sector"] or ""
        sector_tag = "hot" if sector in hot_sectors else "cold" if sector in cold_sectors else "neutral"
        _bucket(analytics, sector_tag, "by_sector_hot", pnl)

        # 진입 시간대별
        hhmm = str(r["entry_hhmm"] or "")
        hour_key = hhmm[:2] if len(hhmm) >= 2 else "?"
        _bucket(analytics, hour_key, "by_entry_hour", pnl)

    # avg_pnl / profit_factor / expectancy 계산
    total_gross_profit = total_gross_loss = 0.0
    for dim in analytics.values():
        for grp in dim.values():
            cnt   = grp["count"]
            grp["avg_pnl"] = round(grp["pnl_sum"] / cnt, 3) if cnt else 0.0

            gp = grp.get("win_pnl_sum", 0.0)
            gl = abs(grp.get("loss_pnl_sum", 0.0))
            grp["profit_factor"] = round(gp / gl, 2) if gl > 0 else (float("inf") if gp > 0 else 0.0)

            wr = grp["win"] / cnt if cnt else 0.0
            avg_win  = grp["win_pnl_sum"]  / grp["win"]  if grp["win"]  else 0.0
            avg_loss = grp["loss_pnl_sum"] / grp["loss"] if grp["loss"] else 0.0
            grp["expectancy"] = round(wr * avg_win + (1 - wr) * avg_loss, 3)

            # 전체 통계용 누적
            total_gross_profit += grp.get("win_pnl_sum", 0.0)
            total_gross_loss   += abs(grp.get("loss_pnl_sum", 0.0))

            del grp["pnl_sum"], grp["win_pnl_sum"], grp["loss_pnl_sum"]

    total = total_win + total_loss
    overall_pf = round(total_gross_profit / total_gross_loss, 2) if total_gross_loss > 0 else (float("inf") if total_gross_profit > 0 else 0.0)
    wr_overall = total_win / total if total else 0.0
    avg_win_all  = total_gross_profit / total_win  if total_win  else 0.0
    avg_loss_all = -total_gross_loss  / total_loss if total_loss else 0.0
    analytics["overall"] = {
        "win": total_win,
        "loss": total_loss,
        "win_rate": round(wr_overall, 3),
        "avg_pnl": round(total_pnl / total, 3) if total else 0.0,
        "profit_factor": overall_pf,
        "expectancy": round(wr_overall * avg_win_all + (1 - wr_overall) * avg_loss_all, 3) if total else 0.0,
    }

    logger.info(
        f"신호 분석 완료 — 총 {total}건 | "
        f"신호유형 {len(analytics['by_signal_type'])}종 | "
        f"RSI구간 {len(analytics['by_rsi_bucket'])}종 | "
        f"Profit Factor {overall_pf} | Expectancy {analytics['overall']['expectancy']:+.3f}%"
    )
    return analytics


def _load_signal_feedback(days: int = 20) -> dict[str, dict]:
    """
    최근 N일 signal_analytics를 집계해 신호 유형별 통합 성과를 반환.

    매매팀 진입 점수 조정의 피드백 루프 입력.

    Returns:
        {
            "gap_up_breakout":      {"win_rate": 0.72, "expectancy": 1.2, "profit_factor": 1.8, "n": 18},
            "pullback_rebound":     {"win_rate": 0.55, "expectancy": 0.3, "profit_factor": 1.1, "n": 12},
            ...
        }
        신호 유형별 집계가 3건 미만이면 해당 신호 제외 (통계 신뢰도 부족).
    """
    try:
        rows = fetch_all(
            """
            SELECT signal_analytics FROM review_reports
            WHERE signal_analytics IS NOT NULL AND signal_analytics != '{}'
            ORDER BY review_date DESC LIMIT ?
            """,
            (days,),
        )
    except Exception:
        return {}

    # 신호 유형별 원시 누적
    agg: dict[str, dict] = {}
    for row in rows:
        try:
            sa = json.loads(row["signal_analytics"] or "{}")
            by_sig = sa.get("by_signal_type") or {}
        except Exception:
            continue
        for sig, grp in by_sig.items():
            if not isinstance(grp, dict):
                continue
            acc = agg.setdefault(sig, {"win": 0, "loss": 0, "gp": 0.0, "gl": 0.0})
            acc["win"]  += grp.get("win", 0)
            acc["loss"] += grp.get("loss", 0)
            # profit_factor이 있으면 역산, 없으면 avg_pnl로 추정
            n = grp.get("win", 0) + grp.get("loss", 0)
            avg_pnl = grp.get("avg_pnl", 0.0)
            if "expectancy" in grp:
                acc["gp"] += max(0.0, grp["expectancy"]) * n
                acc["gl"] += abs(min(0.0, grp["expectancy"])) * n
            else:
                acc["gp"] += max(0.0, avg_pnl) * n
                acc["gl"] += abs(min(0.0, avg_pnl)) * n

    result: dict[str, dict] = {}
    for sig, acc in agg.items():
        total = acc["win"] + acc["loss"]
        if total < 3:  # 통계 신뢰도 부족
            continue
        wr = acc["win"] / total
        pf = round(acc["gp"] / acc["gl"], 2) if acc["gl"] > 0 else (float("inf") if acc["gp"] > 0 else 0.0)
        # 단순 expectancy 근사: win_rate * avg_gp_per_win - loss_rate * avg_gl_per_loss
        avg_win  = acc["gp"] / acc["win"]  if acc["win"]  else 0.0
        avg_loss = acc["gl"] / acc["loss"] if acc["loss"] else 0.0
        exp = round(wr * avg_win - (1 - wr) * avg_loss, 3)
        result[sig] = {"win_rate": round(wr, 3), "expectancy": exp, "profit_factor": pf, "n": total}

    return result


def _load_market_context(today: str) -> dict:
    """
    당일 시황 데이터 수집.
    market_condition (국내) + global_condition (글로벌) 최신 레코드 활용.
    Returns:
        {
            "kospi_chg": float,          # 당일 KOSPI 등락률
            "kosdaq_chg": float,         # 당일 KOSDAQ 등락률
            "foreign_dir": str,          # "매수" | "매도" | "중립"
            "institutional_dir": str,    # "매수" | "매도" | "중립"
            "market_score": float,       # -1.0 ~ 1.0
            "market_direction": str,     # bullish|neutral|bearish
            "global_risk": int,          # 0~10
            "korea_outlook": str,        # positive|neutral|negative
            "leading_force": str,        # foreign|institutional|individual|mixed
            "summary": str,              # 국내 시황 한줄 요약
        }
    """
    ctx = {
        "kospi_chg": 0.0, "kosdaq_chg": 0.0,
        "foreign_dir": "중립", "institutional_dir": "중립",
        "market_score": 0.0, "market_direction": "neutral",
        "global_risk": 5, "korea_outlook": "neutral",
        "leading_force": "mixed", "summary": "",
    }
    try:
        mc = fetch_one(
            "SELECT * FROM market_condition WHERE date(created_at) = ? ORDER BY created_at DESC LIMIT 1",
            (today,),
        )
        if mc:
            mc = dict(mc)
            ctx["market_score"]      = mc.get("market_score", 0.0) or 0.0
            ctx["market_direction"]  = mc.get("market_direction", "neutral") or "neutral"
            fnet = mc.get("foreign_net_buy_bn") or 0
            inet = mc.get("institutional_net_buy_bn") or 0
            ctx["foreign_dir"]       = "매수" if fnet > 100 else "매도" if fnet < -100 else "중립"
            ctx["institutional_dir"] = "매수" if inet > 50 else "매도" if inet < -50 else "중립"
            summary_json = mc.get("summary") or "{}"
            try:
                s = json.loads(summary_json)
                ctx["kospi_chg"]    = s.get("kospi", 0.0) or 0.0
                ctx["kosdaq_chg"]   = s.get("kosdaq", 0.0) or 0.0
                ctx["leading_force"] = s.get("leading_force", "mixed") or "mixed"
                ctx["summary"]      = s.get("analysis", "") or ""
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"국내 시황 조회 실패: {e}")

    try:
        gc = fetch_one(
            "SELECT * FROM global_condition WHERE date(created_at) = ? ORDER BY created_at DESC LIMIT 1",
            (today,),
        )
        if gc:
            gc = dict(gc)
            ctx["global_risk"]   = int(gc.get("global_risk_score") or 5)
            ctx["korea_outlook"] = gc.get("korea_market_outlook", "neutral") or "neutral"
    except Exception as e:
        logger.debug(f"글로벌 시황 조회 실패: {e}")

    return ctx


def _load_similar_market_days(market_ctx: dict, days_back: int = 60) -> list[dict]:
    """
    오늘과 유사한 시황 패턴의 과거 복기 조회.
    market_direction + foreign_dir 기준으로 유사 날을 찾아 참조용으로 반환.
    Returns:
        최대 3개의 과거 복기 요약 리스트
    """
    direction  = market_ctx.get("market_direction", "neutral")
    foreign    = market_ctx.get("foreign_dir", "중립")

    try:
        rows = fetch_all(
            """
            SELECT review_date, total_pnl, win_trades, loss_trades,
                   summary, market_context
            FROM trade_review
            WHERE market_context IS NOT NULL
              AND created_at >= datetime('now', ? || ' days')
            ORDER BY created_at DESC
            LIMIT 10
            """,
            (f"-{days_back}",),
        )
        if not rows:
            return []

        similar = []
        for r in rows:
            r = dict(r)
            try:
                mc = json.loads(r.get("market_context") or "{}")
                # 시황 방향 + 외인 방향이 같은 날만 유사로 간주
                if mc.get("market_direction") == direction and mc.get("foreign_dir") == foreign:
                    similar.append({
                        "date": r["review_date"],
                        "total_pnl": r.get("total_pnl") or 0,
                        "win": r.get("win_trades") or 0,
                        "loss": r.get("loss_trades") or 0,
                        "summary": r.get("summary") or "",
                        "global_risk": mc.get("global_risk", 5),
                    })
                    if len(similar) >= 3:
                        break
            except Exception:
                continue
        return similar
    except Exception as e:
        logger.debug(f"유사 시황 조회 실패: {e}")
        return []


def _load_slot_map(today: str) -> dict[str, str]:
    """오늘 slot_assignments에서 ticker → slot 매핑 반환."""
    try:
        rows = fetch_all(
            "SELECT ticker, slot FROM slot_assignments WHERE trade_date = ?",
            (today,),
        )
        return {r["ticker"]: r["slot"] for r in rows if r["ticker"]}
    except Exception:
        return {}


def _load_snapshots_context(today: str, tickers: list[str]) -> dict[str, list[dict]]:
    """
    오늘 포지션 스냅샷 — 각 종목의 가격 흐름 (최대 10개 포인트).
    복기 맥락 제공용.
    """
    ctx: dict[str, list[dict]] = {}
    for ticker in set(tickers):
        rows = fetch_all(
            """
            SELECT pnl_pct, current_price, snapshot_at
            FROM position_snapshot
            WHERE ticker = ?
              AND date(snapshot_at) = ?
            ORDER BY snapshot_at ASC
            LIMIT 10
            """,
            (ticker, today),
        )
        if rows:
            ctx[ticker] = [dict(r) for r in rows]
    return ctx


# ──────────────────────────────────────────────
# 통계 계산
# ──────────────────────────────────────────────

def _calc_stats(trades: list[dict]) -> dict:
    """매매 기초 통계 + 종목별 매수/매도 페어링 + 매도 사유 집계."""
    _SELL_ACTIONS = ("sell", "stop_loss", "take_profit", "time_cut", "partial_exit", "force_close")
    sell_trades = [t for t in trades if t["action"] in _SELL_ACTIONS]
    buy_trades  = [t for t in trades if t["action"] == "buy"]

    win_trades  = [t for t in sell_trades if (t.get("pnl_pct") or 0) > 0]
    loss_trades = [t for t in sell_trades if (t.get("pnl_pct") or 0) <= 0]
    total_pnl   = sum((t.get("pnl") or 0) for t in sell_trades)

    best  = max(sell_trades, key=lambda t: t.get("pnl_pct") or 0) if sell_trades else None
    worst = min(sell_trades, key=lambda t: t.get("pnl_pct") or 0) if sell_trades else None

    # 매도 사유 집계
    exit_reason_counts: dict[str, int] = {}
    for t in sell_trades:
        reason = t["action"]
        exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + 1

    # 종목별 페어링 (매수가 → 매도가 연결, 진입 컨텍스트 포함)
    ticker_summary: dict[str, dict] = {}
    for t in buy_trades:
        tk = t["ticker"]
        if tk not in ticker_summary:
            ticker_summary[tk] = {
                "ticker": tk,
                "name": t.get("name") or tk,
                "buy_price": t.get("exec_price") or 0,
                "buy_qty": t.get("quantity") or 0,
                "sell_price": None,
                "sell_qty": 0,
                "pnl_pct": None,
                "pnl_amt": 0.0,
                "action": "보유중",
                "status": "holding",
                # 진입 컨텍스트
                "signal_type":    t.get("signal_type", ""),
                "entry_score":    float(t.get("entry_score") or 0.0),
                "rsi":            float(t.get("rsi") or 0.0),
                "sector":         t.get("sector", ""),
                "entry_hhmm":     t.get("entry_hhmm", ""),
                "rs_daily":       float(t.get("rs_daily") or 0.0),
                "momentum_score": float(t.get("momentum_score") or 0.0),
            }
        else:
            # 추가 매수: 평균단가 갱신
            prev = ticker_summary[tk]
            total_qty = prev["buy_qty"] + (t.get("quantity") or 0)
            if total_qty > 0:
                prev["buy_price"] = (
                    prev["buy_price"] * prev["buy_qty"]
                    + (t.get("exec_price") or 0) * (t.get("quantity") or 0)
                ) / total_qty
            prev["buy_qty"] = total_qty

    for t in sell_trades:
        tk = t["ticker"]
        if tk in ticker_summary:
            ticker_summary[tk]["sell_price"] = t.get("exec_price") or 0
            ticker_summary[tk]["sell_qty"] = t.get("quantity") or 0
            ticker_summary[tk]["pnl_pct"] = t.get("pnl_pct") or 0
            ticker_summary[tk]["pnl_amt"] = t.get("pnl") or 0.0
            ticker_summary[tk]["action"] = t["action"]
            ticker_summary[tk]["status"] = "closed"
        else:
            # 매수 없이 매도만 있는 경우 (오버나잇 포지션 청산 등)
            ticker_summary[tk] = {
                "ticker": tk,
                "name": t.get("name") or tk,
                "buy_price": None,
                "buy_qty": 0,
                "sell_price": t.get("exec_price") or 0,
                "sell_qty": t.get("quantity") or 0,
                "pnl_pct": t.get("pnl_pct") or 0,
                "pnl_amt": t.get("pnl") or 0.0,
                "action": t["action"],
                "status": "closed",
                "signal_type": "",
                "entry_score": 0.0,
                "rsi": 0.0,
                "sector": "",
                "entry_hhmm": "",
                "rs_daily": 0.0,
                "momentum_score": 0.0,
            }

    return {
        "total":              len(trades),
        "buys":               len(buy_trades),
        "sells":              len(sell_trades),
        "win":                len(win_trades),
        "loss":               len(loss_trades),
        "total_pnl":          total_pnl,
        "win_rate":           len(win_trades) / len(sell_trades) if sell_trades else 0.0,
        "best":               best,
        "worst":              worst,
        "ticker_summary":     ticker_summary,
        "exit_reason_counts": exit_reason_counts,
    }


# ──────────────────────────────────────────────
# Claude 분석
# ──────────────────────────────────────────────

def _ask_claude_review(
    today: str,
    trades: list[dict],
    stats: dict,
    snapshots: dict[str, list[dict]],
    market_ctx: dict,
    similar_days: list[dict],
    extra: dict | None = None,
) -> dict:
    """
    Claude에게 오늘 매매 복기 및 개선점 분석 요청.

    Returns:
        {
          "pattern_hits":    [...],   # 잘 작동한 패턴
          "pattern_fails":   [...],   # 실패한 패턴
          "improvements":    [...],   # 내일 당장 적용 가능한 개선점
          "market_regime":   "...",   # 오늘 시장 성격 태그
          "strategy_fit":    "...",   # 전략 적합성 평가
          "tomorrow_watch":  "...",   # 내일 주목 전략·주의사항 (2문장 이내)
          "summary":         "..."    # 한국어 총평
        }
    """
    extra = extra or {}
    # 종목별 페어링 요약 (진입 컨텍스트 포함)
    ticker_summary = stats.get("ticker_summary", {})
    pair_lines = []
    for tk, ts in ticker_summary.items():
        buy_str  = f"매수 {ts['buy_price']:,.0f}원×{ts['buy_qty']}주" if ts.get("buy_price") else "매수미상"
        sell_str = f"매도 {ts['sell_price']:,.0f}원×{ts['sell_qty']}주" if ts.get("sell_price") else "미청산(보유중)"
        pnl_str  = f"손익 {ts['pnl_pct']:+.2f}%" if ts.get("pnl_pct") is not None else ""
        ctx_str  = ""
        if ts.get("signal_type"):
            ctx_str = (
                f" | 신호:{ts['signal_type']} 점수:{ts['entry_score']:.0f} "
                f"RSI:{ts['rsi']:.0f} 진입:{ts.get('entry_hhmm','?')}"
                + (f" 섹터:{ts['sector']}" if ts.get("sector") else "")
            )
        pair_lines.append(
            f"  - {ts['name']}({tk}): {buy_str} → {sell_str} {pnl_str} [{ts.get('action','?')}]{ctx_str}"
        )

    # 매도 사유 집계
    exit_counts = stats.get("exit_reason_counts", {})
    exit_str = " | ".join(f"{k} {v}건" for k, v in exit_counts.items()) if exit_counts else "없음"

    # 가격 흐름 맥락
    snap_lines = []
    for ticker, snaps in snapshots.items():
        if snaps:
            first_pnl = snaps[0].get("pnl_pct", 0)
            last_pnl  = snaps[-1].get("pnl_pct", 0)
            snap_lines.append(f"  - {ticker}: 장중 손익 {first_pnl:+.1f}% → {last_pnl:+.1f}%")

    # 유사 시황 과거 복기 요약
    similar_lines = []
    for s in similar_days:
        pnl_sign = "+" if s["total_pnl"] >= 0 else ""
        similar_lines.append(
            f"  - {s['date']}: 승률 {s['win']}/{s['win']+s['loss']}건 | "
            f"손익 {pnl_sign}{s['total_pnl']:,.0f}원 | {s['summary'][:40]}"
        )

    similar_section = ""
    if similar_lines:
        similar_section = f"""
## 유사 시황 과거 성과 (참고용 — {market_ctx['market_direction']} 장, 외인 {market_ctx['foreign_dir']})
{chr(10).join(similar_lines)}
→ 위 과거 데이터 기반으로 오늘 전략의 적합성을 평가하세요."""

    prompt = f"""당신은 국내 주식 단타 퀀트 트레이딩 시스템의 성과 분석 AI입니다.
오늘({today}) 매매 전체를 분석하고, 시장 상황과 연결하여 무엇이 잘 됐는지·무엇을 고쳐야 하는지 판단하세요.
시스템은 당일 매수·매도 단타 전략(스캘핑 포함)입니다. 오버나잇은 하지 않습니다.

## 오늘 시장 상황
- KOSPI: {market_ctx['kospi_chg']:+.2f}% | KOSDAQ: {market_ctx['kosdaq_chg']:+.2f}%
- 시장 방향: {market_ctx['market_direction']} (점수 {market_ctx['market_score']:+.2f})
- 외인: {market_ctx['foreign_dir']} | 기관: {market_ctx['institutional_dir']} | 주도세력: {market_ctx['leading_force']}
- 글로벌 리스크: {market_ctx['global_risk']}/10 | 한국 전망: {market_ctx['korea_outlook']}
- 시황 요약: {market_ctx['summary'] or '데이터 없음'}{similar_section}

## 오늘 매매 요약
- 총 거래: {stats['total']}건 (매수 {stats['buys']}, 청산 {stats['sells']})
- 승률: {stats['win_rate']*100:.0f}% (수익 {stats['win']}건 / 손실 {stats['loss']}건)
- 당일 실현 손익: {stats['total_pnl']:+,.0f}원
- 청산 사유: {exit_str}

## 종목별 매수→청산 상세 (신호유형·진입점수·RSI·진입시간 포함)
{chr(10).join(pair_lines) if pair_lines else "  (없음)"}

## 장중 가격 흐름
{chr(10).join(snap_lines) if snap_lines else "  (없음)"}

## 시스템 전략 참고
- 매수 조건: Hot List + MACD 강세 + 거래량급증, 분할 매수 (60/25/15%)
- 신호유형: gap_up_breakout(갭업돌파) / momentum(모멘텀) / pullback_rebound(눌림반등) / opening_plunge_rebound(급락반등)
- 진입점수 50~100: 50미만 차단, 72이상 풀사이즈, 50~72 75%사이즈
- 손절: 트레일링 스톱(-3%), MACD 역행 시 조기 손절 (stop_loss)
- 익절: take_profit(목표가), time_cut(14:50 수익청산), force_close(15:20 강제)

## 오늘 성과 지표 요약
- Profit Factor: {extra.get('profit_factor', 'N/A')} (1.5↑ 양호, 1.0↓ 위험)
- 평균 보유시간: {extra.get('avg_hold_minutes', 0):.0f}분
- 연속 최대 손절: {extra.get('consecutive_losses', 0)}회
- Hot List 전환율: {extra.get('hot_list', {}).get('scanned', 0)}종목 스캔 → {extra.get('hot_list', {}).get('bought', 0)}개 매수 ({extra.get('hot_list', {}).get('conversion_pct', 0)}%)
- 분할매수 실행: {extra.get('tranche', {}).get('tickers_with_tranche', 0)}종목 (2차 {extra.get('tranche', {}).get('tranche2_count', 0)}건, 3차 {extra.get('tranche', {}).get('tranche3_count', 0)}건)
- 장중 최대 드로우다운: {extra.get('max_drawdown_pct', 0):.2f}%

## 분석 요청 (반드시 구체적으로, 종목명 + 진입 시그널 + 수치 포함)
1. **pattern_hits**: 오늘 잘 작동한 진입/청산 패턴 (종목명·신호유형·수익률 포함, 최대 4개)
2. **pattern_fails**: 손실이 났거나 아쉬웠던 부분 (종목명·이유 포함, 최대 4개)
3. **improvements**: 내일 당장 바꿀 수 있는 개선점 (파라미터 수치 제안 포함, 최대 4개)
4. **market_regime**: 오늘 시장 성격을 태그로 요약 (예: "강세_외인주도", "혼조_개인장", "약세_매도압력" 등 20자 이내)
5. **strategy_fit**: 오늘 시장 상황에 현재 전략이 얼마나 잘 맞았는지 평가 (30자 이내)
6. **tomorrow_watch**: 내일 오전 전략 방향 + 주의사항 (글로벌 선물·미청산 포지션·시장 흐름 고려, 2문장 이내)
7. **summary**: 오늘 총평 (시황 + 승률 + 실현손익 + 특이사항 포함, 2~3문장)

JSON만 응답:
{{
  "pattern_hits":   ["...", ...],
  "pattern_fails":  ["...", ...],
  "improvements":   ["...", ...],
  "market_regime":  "...",
  "strategy_fit":   "...",
  "tomorrow_watch": "...",
  "summary":        "..."
}}"""

    response = None
    for attempt in range(1, 4):
        try:
            response = _client.messages.create(
                model=settings.CLAUDE_MODEL_MAIN,
                max_tokens=1500,
                temperature=0,
                timeout=60.0,
                messages=[{"role": "user", "content": prompt}],
            )
            break
        except Exception as e:
            if attempt == 3:
                logger.error(f"Claude 복기 분석 최종 실패 ({attempt}회): {type(e).__name__}: {e}")
                return _fallback_review(stats)
            logger.warning(f"Claude 복기 분석 재시도 {attempt}/3: {type(e).__name__}: {e}")
            time.sleep(5 * attempt)
    try:
        raw = response.content[0].text.strip()
        return json.loads(_extract_json(raw))
    except Exception as e:
        logger.error(f"Claude 복기 응답 파싱 실패: {type(e).__name__}: {e}")
        return _fallback_review(stats)


def _fallback_review(stats: dict) -> dict:
    """Claude API 실패 시 거래 데이터로 자동 생성하는 복기."""
    hits, fails, improvements = [], [], []
    ticker_summary = stats.get("ticker_summary", {})

    win_rate = stats.get("win_rate", 0)
    total_pnl = stats.get("total_pnl", 0)

    # 잘 된 것: 수익 난 종목
    for tk, ts in ticker_summary.items():
        pnl = ts.get("pnl_pct") or 0
        if pnl > 0:
            hits.append(
                f"{ts['name']}({tk}) +{pnl:.2f}% 익절 성공"
                + (f" (매수 {ts['buy_price']:,.0f}→매도 {ts['sell_price']:,.0f}원)" if ts.get("sell_price") else "")
            )

    # 아쉬운 것: 손실 난 종목
    for tk, ts in ticker_summary.items():
        pnl = ts.get("pnl_pct") or 0
        if pnl < 0:
            fails.append(
                f"{ts['name']}({tk}) {pnl:.2f}% 손절"
                + (f" ({ts.get('action','?')} 사유)" if ts.get("action") else "")
            )

    # 개선 포인트: 승률·손익 기반 자동 생성
    if win_rate < 0.4 and stats.get("sells", 0) >= 3:
        improvements.append(f"승률 {win_rate*100:.0f}% — 진입 기준 강화 필요 (Hot List 신호 복합 조건 추가 검토)")
    if total_pnl < 0:
        improvements.append("당일 실현 손익 마이너스 — 손절선 타이트하게 조정 (trailing_initial_stop 축소 검토)")
    if stats.get("buys", 0) > stats.get("sells", 0) + 2:
        improvements.append("미청산 포지션 다수 — 장마감 청산 로직 확인 필요")
    if not improvements:
        improvements.append("Claude API 오류로 자동 분석 — 로그에서 상세 확인 필요")

    pnl_sign = "+" if total_pnl >= 0 else ""
    summary = (
        f"총 {stats['total']}건 매매 | 승률 {win_rate*100:.0f}% | "
        f"실현손익 {pnl_sign}{total_pnl:,.0f}원. "
        f"(Claude API 미응답 — 데이터 기반 자동 생성)"
    )

    return {
        "pattern_hits":  hits[:4],
        "pattern_fails": fails[:4],
        "improvements":  improvements[:4],
        "market_regime": "",
        "strategy_fit":  "",
        "summary":       summary,
    }


# ──────────────────────────────────────────────
# DB 저장
# ──────────────────────────────────────────────

def _update_ticker_stats_from_review(stats: dict, review: dict) -> None:
    """
    오늘 거래한 종목들의 avg_hold_minutes·best_signal_type·notes를 ticker_stats에 갱신.
    position_monitor가 pnl 기반 win/loss를 이미 갱신했으므로, 여기서는 추가 메타만 보강.
    """
    try:
        ticker_summary = stats.get("ticker_summary", {})
        pattern_hits = review.get("pattern_hits", [])
        # pattern_hits에서 종목명 → 패턴 매핑 (best_signal_type 힌트)
        _hit_map: dict[str, str] = {}
        for hit in pattern_hits:
            if isinstance(hit, str):
                # 예: "005930(삼성) gap_up_breakout +3.2%"
                parts = hit.split()
                if parts:
                    # 첫 번째 괄호 앞 숫자열이 ticker일 수 있음
                    for tk in ticker_summary:
                        if tk in hit:
                            _hit_map[tk] = hit[:80]
                            break

        for tk, ts in ticker_summary.items():
            if ts.get("status") != "closed":
                continue
            # avg_hold_minutes 갱신 (buy_time → sell_time)
            _hold = ts.get("hold_minutes")
            _sig  = ts.get("signal_type", "")
            _note = _hit_map.get(tk)
            try:
                execute(
                    """
                    UPDATE ticker_stats
                    SET avg_hold_minutes = CASE
                            WHEN total_trades <= 1 THEN ?
                            ELSE (avg_hold_minutes * (total_trades - 1) + ?) / total_trades
                        END,
                        best_signal_type = COALESCE(NULLIF(?, ''), best_signal_type),
                        notes = CASE WHEN ? IS NOT NULL THEN ? ELSE notes END,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = ?
                    """,
                    (_hold or 0, _hold or 0, _sig, _note, _note, tk),
                )
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"ticker_stats 메타 갱신 실패: {e}")


def _save_review(today: str, stats: dict, review: dict, market_ctx: dict, signal_analytics: dict | None = None) -> None:
    best_t  = stats.get("best")
    worst_t = stats.get("worst")

    best_json  = json.dumps(
        {"ticker": best_t["ticker"],  "pnl_pct": best_t.get("pnl_pct"),  "reason": best_t.get("strategy_id")},
        ensure_ascii=False,
    ) if best_t else None
    worst_json = json.dumps(
        {"ticker": worst_t["ticker"], "pnl_pct": worst_t.get("pnl_pct"), "reason": worst_t.get("strategy_id")},
        ensure_ascii=False,
    ) if worst_t else None

    market_context_json = json.dumps(
        {
            **market_ctx,
            "market_regime": review.get("market_regime", ""),
            "strategy_fit":  review.get("strategy_fit", ""),
        },
        ensure_ascii=False,
    )

    execute(
        """
        INSERT OR REPLACE INTO trade_review
            (review_date, total_trades, win_trades, loss_trades, total_pnl,
             best_trade, worst_trade, pattern_hits, pattern_fails, improvements,
             summary, market_context, signal_analytics)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            today,
            stats["total"],
            stats["win"],
            stats["loss"],
            stats["total_pnl"],
            best_json,
            worst_json,
            json.dumps(review.get("pattern_hits",  []), ensure_ascii=False),
            json.dumps(review.get("pattern_fails", []), ensure_ascii=False),
            json.dumps(review.get("improvements",  []), ensure_ascii=False),
            review.get("summary", ""),
            market_context_json,
            json.dumps(signal_analytics or {}, ensure_ascii=False),
        ),
    )
    logger.info(f"trade_review 저장 완료: {today}")


# ──────────────────────────────────────────────
# Telegram 알림
# ──────────────────────────────────────────────

_EXIT_LABEL = {
    "take_profit":   "익절",
    "stop_loss":     "손절",
    "time_cut":      "시간청산",
    "force_close":   "강제청산",
    "sell":          "매도",
    "partial_exit":  "부분익절",
}

_SIG_LABEL = {
    "gap_up_breakout":        "갭업돌파",
    "momentum":               "모멘텀",
    "pullback_rebound":       "눌림반등",
    "opening_plunge_rebound": "급락반등",
    "volume_surge":           "거래량급등",
    "sector_momentum":        "섹터모멘텀",
    "breakout":               "돌파",
}


_SLOT_LABEL = {
    "leader":   "👑주도주",
    "breakout": "🚀돌파",
    "pullback": "🔄눌림목",
}


def _notify_review(
    today: str,
    stats: dict,
    review: dict,
    market_ctx: dict,
    signal_analytics: dict | None = None,
    extra: dict | None = None,
    portfolio: dict | None = None,
    slot_map: dict[str, str] | None = None,
) -> None:
    extra    = extra or {}
    sa       = signal_analytics or {}
    pf       = portfolio or {}
    slot_map = slot_map or {}
    total_pnl  = stats.get("total_pnl") or 0
    pnl_emoji  = "🟢" if total_pnl >= 0 else "🔴"
    win_rate   = stats.get("win_rate", 0)

    # ── 1. 헤더 ───────────────────────────────────────────
    lines = [
        f"📊 <b>[일일 복기] {today}</b>",
        f"거래 {stats['total']}건 | 승률 {win_rate*100:.0f}% ({stats['win']}승/{stats['loss']}패)"
        f"  |  {pnl_emoji} <b>{total_pnl:+,.0f}원</b>",
        "",
    ]

    # ── 2. 포트폴리오 잔고 (평가금액 + 예수금) ──────────────
    if pf.get("total_eval_amt"):
        total_eval  = pf["total_eval_amt"]
        stock_eval  = pf.get("stock_eval_amt", 0)
        avail_cash  = pf.get("available_cash", 0)
        pf_pnl      = pf.get("total_pnl_amt", 0)
        pf_pnl_pct  = pf.get("total_pnl_pct", 0)
        pf_emoji    = "🟢" if pf_pnl >= 0 else "🔴"
        lines.append("💼 <b>포트폴리오 현황 (장 마감 기준)</b>")
        lines.append(
            f"  총 평가금액: <b>{total_eval:,.0f}원</b>"
            f"  (주식 {stock_eval:,.0f}원 + 예수금 {avail_cash:,.0f}원)"
        )
        lines.append(
            f"  평가 손익: {pf_emoji} <b>{pf_pnl:+,.0f}원 ({pf_pnl_pct:+.2f}%)</b>"
        )
        # 보유 중인 종목 (미청산)
        if pf.get("positions"):
            pos_parts = []
            for p in pf["positions"]:
                sl = _SLOT_LABEL.get(slot_map.get(p["ticker"], ""), "")
                tag = f" {sl}" if sl else ""
                pos_parts.append(f"{p['name']}({p['ticker']}){tag} {p['pnl_pct']:+.1f}%")
            lines.append(f"  보유종목: {' | '.join(pos_parts)}")
        lines.append("")

    # ── 3. 시장 상황 + 성격 태그 ─────────────────────────
    regime       = review.get("market_regime", "")
    strategy_fit = review.get("strategy_fit", "")
    lines.append(
        f"📈 KOSPI {market_ctx.get('kospi_chg', 0):+.2f}%"
        f" | KOSDAQ {market_ctx.get('kosdaq_chg', 0):+.2f}%"
        f" | 외인 {market_ctx.get('foreign_dir', '-')}"
        f" | 글로벌리스크 {market_ctx.get('global_risk', '-')}/10"
    )
    if regime:
        lines.append(f"🏷 <b>{regime}</b>" + (f"  ·  {strategy_fit}" if strategy_fit else ""))

    # 청산 사유 집계
    exit_counts = stats.get("exit_reason_counts", {})
    if exit_counts:
        exit_parts = [f"{_EXIT_LABEL.get(k, k)} {v}건" for k, v in exit_counts.items()]
        lines.append(f"🔚 청산: {' | '.join(exit_parts)}")
    lines.append("")

    # ── 4. 핵심 지표 한눈에 보기 ─────────────────────────
    pf_val      = extra.get("profit_factor", 0)
    pf_str      = f"{pf_val:.2f}" if pf_val != float("inf") else "∞ (손실 없음)"
    pf_judge    = "✅ 양호" if pf_val >= 1.5 else ("⚠️ 주의" if pf_val >= 1.0 else "🚨 위험")
    hold_min    = extra.get("avg_hold_minutes", 0)
    cons_loss   = extra.get("consecutive_losses", 0)
    max_dd      = extra.get("max_drawdown_pct", 0)
    hl          = extra.get("hot_list", {})
    tr          = extra.get("tranche", {})

    lines.append("📐 <b>오늘 성과 지표</b>")
    lines.append(f"  Profit Factor: <b>{pf_str}</b>  {pf_judge}")
    lines.append(f"  평균 보유시간: <b>{hold_min:.0f}분</b>" + ("  ⚠️ 장기보유 의심" if hold_min > 45 else ""))
    if cons_loss >= 3:
        lines.append(f"  🚨 연속 손절: <b>{cons_loss}회</b> — 진입 기준 점검 필요")
    elif cons_loss > 0:
        lines.append(f"  연속 최대 손절: {cons_loss}회")
    if max_dd < 0:
        lines.append(f"  장중 최대 드로우다운: <b>{max_dd:.2f}%</b>" + ("  🚨 위험" if max_dd < -3 else ""))
    if hl.get("scanned"):
        lines.append(
            f"  Hot List 전환율: {hl['scanned']}종목 스캔 → {hl['bought']}개 매수"
            f" ({hl['conversion_pct']}%)"
        )
    if tr.get("tickers_with_tranche"):
        lines.append(
            f"  분할매수: {tr['tickers_with_tranche']}종목"
            f" (2차 {tr.get('tranche2_count', 0)}건 / 3차 {tr.get('tranche3_count', 0)}건)"
        )
    lines.append("")

    # ── 5. 종목별 성과 (수익 → 손실 순) ───────────────────
    ticker_summary = stats.get("ticker_summary", {})
    if ticker_summary:
        lines.append("📋 <b>종목별 성과</b>")
        for ts in sorted(ticker_summary.values(), key=lambda x: x.get("pnl_pct") or 0, reverse=True):
            pnl      = ts.get("pnl_pct")
            name_str = ts.get("name") or ts["ticker"]
            sig_lbl  = _SIG_LABEL.get(ts.get("signal_type", ""), ts.get("signal_type", ""))
            hhmm     = ts.get("entry_hhmm", "")
            score    = ts.get("entry_score", 0.0)
            rsi_val  = ts.get("rsi", 0.0)

            slot_lbl = _SLOT_LABEL.get(slot_map.get(ts["ticker"], ""), "")
            if ts["status"] == "closed" and pnl is not None:
                p_emoji  = "▲" if pnl >= 0 else "▼"
                buy_str  = f"{ts['buy_price']:,.0f}" if ts.get("buy_price") else "?"
                sell_str = f"{ts['sell_price']:,.0f}" if ts.get("sell_price") else "?"
                exit_lbl = _EXIT_LABEL.get(ts.get("action", ""), ts.get("action", ""))
                slot_tag = f" {slot_lbl}" if slot_lbl else ""
                lines.append(
                    f"  {p_emoji} <b>{name_str}</b>({ts['ticker']}){slot_tag}"
                    f"  {buy_str}→{sell_str}원  <b>{pnl:+.2f}%</b>  [{exit_lbl}]"
                )
                ctx_parts = []
                if sig_lbl:   ctx_parts.append(f"신호:{sig_lbl}")
                if score:     ctx_parts.append(f"점수:{score:.0f}")
                if rsi_val:   ctx_parts.append(f"RSI:{rsi_val:.0f}")
                if hhmm:      ctx_parts.append(f"진입:{hhmm[:2]}:{hhmm[2:]}")
                if ts.get("sector"): ctx_parts.append(f"섹터:{ts['sector']}")
                if ctx_parts:
                    lines.append(f"      └ {' | '.join(ctx_parts)}")
            else:
                slot_tag = f" {slot_lbl}" if slot_lbl else ""
                lines.append(f"  ⏳ <b>{name_str}</b>({ts['ticker']}){slot_tag}: 보유중")
        lines.append("")

    # ── 6. 놓친 기회 (Hot List에 올랐으나 미매수 상위 3종) ──
    missed = hl.get("missed_top", [])
    if missed:
        lines.append("🔍 <b>놓친 기회 (미매수 Hot List 상위)</b>")
        for m in missed:
            sig = _SIG_LABEL.get(m.get("signal_type", ""), m.get("signal_type", ""))
            sl  = _SLOT_LABEL.get(slot_map.get(m.get("ticker", ""), ""), "")
            lines.append(
                f"  • {m['name']}({m['ticker']})"
                + (f" {sl}" if sl else "")
                + f"  스캔시 {m['change_pct']:+.2f}%"
                + (f"  [{sig}]" if sig else "")
            )
        lines.append("")

    # ── 7. Claude 총평 + 분석 ────────────────────────────
    if review.get("summary"):
        lines.append(f"💬 {review['summary']}")
        lines.append("")

    if review.get("pattern_hits"):
        lines.append("✅ <b>잘 된 것</b>")
        for h in review["pattern_hits"][:4]:
            lines.append(f"  • {h}")
        lines.append("")

    if review.get("pattern_fails"):
        lines.append("⚠️ <b>아쉬운 것</b>")
        for f_ in review["pattern_fails"][:4]:
            lines.append(f"  • {f_}")
        lines.append("")

    if review.get("improvements"):
        lines.append("🔧 <b>내일 개선 포인트</b>")
        for imp in review["improvements"][:4]:
            lines.append(f"  • {imp}")
        lines.append("")

    if review.get("tomorrow_watch"):
        lines.append(f"📅 <b>내일 주목</b>: {review['tomorrow_watch']}")
        lines.append("")

    # ── 8. 정량 신호 분석 4종 ────────────────────────────
    by_sig = sa.get("by_signal_type", {})
    if by_sig:
        lines.append("📐 <b>신호유형별 성과</b>")
        for sig, g in sorted(by_sig.items(), key=lambda x: x[1].get("avg_pnl", 0), reverse=True):
            n = g["win"] + g["loss"]
            wr = g["win"] / n * 100 if n else 0
            lines.append(
                f"  • {_SIG_LABEL.get(sig, sig)}: {wr:.0f}%"
                f" ({g['win']}승/{g['loss']}패) 평균 {g['avg_pnl']:+.2f}%"
            )
        lines.append("")

    by_rsi = sa.get("by_rsi_bucket", {})
    if by_rsi:
        lines.append("📊 <b>RSI 구간별 성과</b>")
        for bucket in ["35-", "35-45", "45-55", "55-65", "65-72", "72+"]:
            g = by_rsi.get(bucket)
            if not g:
                continue
            n = g["win"] + g["loss"]
            wr = g["win"] / n * 100 if n else 0
            lines.append(
                f"  • RSI {bucket}: {wr:.0f}%"
                f" ({g['win']}승/{g['loss']}패) 평균 {g['avg_pnl']:+.2f}%"
            )
        lines.append("")

    by_score = sa.get("by_score_bucket", {})
    if by_score and len(by_score) >= 2:
        lines.append("🎯 <b>진입점수별 성과</b>")
        for bucket in ["80+", "70-79", "60-69", "50-59"]:
            g = by_score.get(bucket)
            if not g:
                continue
            n = g["win"] + g["loss"]
            wr = g["win"] / n * 100 if n else 0
            lines.append(
                f"  • {bucket}점: {wr:.0f}%"
                f" ({g['win']}승/{g['loss']}패) 평균 {g['avg_pnl']:+.2f}%"
            )
        lines.append("")

    by_hour = sa.get("by_entry_hour", {})
    if by_hour:
        lines.append("🕐 <b>시간대별 성과</b>")
        for hour in sorted(by_hour.keys()):
            g = by_hour[hour]
            n = g["win"] + g["loss"]
            wr = g["win"] / n * 100 if n else 0
            lines.append(
                f"  • {hour}시: {wr:.0f}%"
                f" ({g['win']}승/{g['loss']}패) 평균 {g['avg_pnl']:+.2f}%"
            )
        lines.append("")

    by_sector = sa.get("by_sector_hot", {})
    if by_sector:
        lines.append("🏭 <b>섹터 강/약세별 성과</b>")
        for tag in ["hot", "neutral", "cold"]:
            g = by_sector.get(tag)
            if not g:
                continue
            n = g["win"] + g["loss"]
            wr = g["win"] / n * 100 if n else 0
            lbl = {"hot": "강세섹터🔥", "neutral": "중립섹터", "cold": "약세섹터🧊"}.get(tag, tag)
            lines.append(
                f"  • {lbl}: {wr:.0f}%"
                f" ({g['win']}승/{g['loss']}패) 평균 {g['avg_pnl']:+.2f}%"
            )

    notify("\n".join(lines))
