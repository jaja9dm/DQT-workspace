"""
engine.py — 포지션 감시 서브엔진

역할:
  보유 포지션을 1~2분 주기로 감시하여 손절·익절·타임컷을 자동 실행한다.
  매매팀과 독립적으로 동작하며, 위기 관리팀의 리스크 레벨을 실시간 참조한다.

손절 기준 (리스크 레벨 연동):
  Level 1~3: -5% (settings.STOP_LOSS_DEFAULT_PCT)
  Level 2:   -3% (settings.STOP_LOSS_LEVEL2_PCT)
  Level 4~5: -1% (settings.STOP_LOSS_LEVEL4_PCT)

분할 익절:
  +5%  도달 시 보유량의 1/3 매도 (1차)
  +10% 도달 시 보유량의 1/3 매도 (2차)
  나머지는 손절 또는 타임컷까지 유지

타임컷:
  5 영업일 초과 보유 → 수익 여부 무관 전량 청산

Level 5 (극위험):
  모든 포지션 즉시 전량 청산
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime, timedelta

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, RequestPriority
from src.teams.risk.engine import get_current_risk, get_stop_loss_pct
from src.utils.logger import get_logger

logger = get_logger(__name__)

_INTERVAL_SEC = 90          # 1분 30초 (1~2분 주기)

# KIS API 경로
_KIS_BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
_KIS_ORDER_PATH = "/uapi/domestic-stock/v1/trading/order-cash"

# 익절 목표 (concept.md 기준)
_TAKE_PROFIT_1_PCT = settings.TAKE_PROFIT_1_PCT   # +5% → 1/3 매도
_TAKE_PROFIT_2_PCT = settings.TAKE_PROFIT_2_PCT   # +10% → 1/3 매도
_MAX_HOLD_DAYS = settings.POSITION_MAX_HOLD_DAYS  # 5 영업일


class PositionMonitorEngine:
    """포지션 감시 서브엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="position-monitor-engine",
        )

    def start(self) -> None:
        logger.info("포지션 감시 엔진 시작")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=15)
        logger.info("포지션 감시 엔진 종료")

    # ──────────────────────────────────────────
    # 메인 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"포지션 감시 오류: {e}", exc_info=True)
            self._stop_event.wait(timeout=_INTERVAL_SEC)

    def run_once(self) -> list[dict]:
        """
        1회 실행: 잔고 조회 → 스냅샷 저장 → 손절·익절·타임컷 판단 → 주문.

        Returns:
            처리된 액션 목록
        """
        # 1. 리스크 레벨 참조
        risk = get_current_risk()
        level = risk.get("risk_level", 1)
        stop_loss_pct = get_stop_loss_pct()

        # 2. KIS 잔고 조회
        positions = _fetch_positions()
        if not positions:
            return []

        # 3. 스냅샷 저장
        _save_snapshots(positions)

        # 4. Level 5 → 전량 청산
        if level >= 5:
            actions = self._liquidate_all(positions, reason="level5_emergency")
            return actions

        # 5. 개별 종목 감시
        actions = []
        for pos in positions:
            action = self._evaluate_position(pos, stop_loss_pct, level)
            if action:
                actions.append(action)

        return actions

    # ──────────────────────────────────────────
    # 포지션 평가
    # ──────────────────────────────────────────

    def _evaluate_position(
        self, pos: dict, stop_loss_pct: float, risk_level: int
    ) -> dict | None:
        """
        단일 포지션에 대해 손절·익절·타임컷 여부 판단.

        Returns:
            실행된 액션 딕셔너리 또는 None
        """
        ticker = pos["ticker"]
        pnl_pct = pos["pnl_pct"]
        held_days = pos["held_days"]
        quantity = pos["quantity"]
        current_price = pos["current_price"]
        partial_sold = pos.get("partial_sold", 0)

        # ── 손절 ─────────────────────────────────
        if pnl_pct <= -stop_loss_pct:
            logger.warning(
                f"[손절] {ticker} | 손익 {pnl_pct:+.2f}% | "
                f"기준 -{stop_loss_pct:.1f}% | {quantity}주 전량"
            )
            return self._place_sell(
                ticker=ticker,
                quantity=quantity,
                current_price=current_price,
                action="stop_loss",
                reason=f"손익 {pnl_pct:+.2f}% ≤ -{stop_loss_pct:.1f}%",
            )

        # ── 타임컷 ───────────────────────────────
        if held_days > _MAX_HOLD_DAYS:
            logger.warning(
                f"[타임컷] {ticker} | {held_days}영업일 보유 | "
                f"손익 {pnl_pct:+.2f}% | {quantity}주 전량"
            )
            return self._place_sell(
                ticker=ticker,
                quantity=quantity,
                current_price=current_price,
                action="time_cut",
                reason=f"{held_days}영업일 초과 ({_MAX_HOLD_DAYS}일 기준)",
            )

        # ── 분할 익절 ────────────────────────────
        # 2차 익절 (+10%): partial_sold == 1 (1차 완료)
        if pnl_pct >= _TAKE_PROFIT_2_PCT and partial_sold >= 1:
            sell_qty = max(1, quantity // 3)
            logger.info(
                f"[익절 2차] {ticker} | 손익 {pnl_pct:+.2f}% | {sell_qty}주"
            )
            return self._place_sell(
                ticker=ticker,
                quantity=sell_qty,
                current_price=current_price,
                action="take_profit",
                reason=f"2차 익절 {pnl_pct:+.2f}% ≥ +{_TAKE_PROFIT_2_PCT:.0f}%",
            )

        # 1차 익절 (+5%): partial_sold == 0 (아직 없음)
        if pnl_pct >= _TAKE_PROFIT_1_PCT and partial_sold == 0:
            sell_qty = max(1, quantity // 3)
            logger.info(
                f"[익절 1차] {ticker} | 손익 {pnl_pct:+.2f}% | {sell_qty}주"
            )
            return self._place_sell(
                ticker=ticker,
                quantity=sell_qty,
                current_price=current_price,
                action="take_profit",
                reason=f"1차 익절 {pnl_pct:+.2f}% ≥ +{_TAKE_PROFIT_1_PCT:.0f}%",
            )

        return None

    def _liquidate_all(self, positions: list[dict], reason: str) -> list[dict]:
        """모든 포지션 전량 청산 (Level 5 긴급)."""
        logger.warning(f"[긴급 전량 청산] {len(positions)}종목 — {reason}")
        actions = []
        for pos in positions:
            action = self._place_sell(
                ticker=pos["ticker"],
                quantity=pos["quantity"],
                current_price=pos["current_price"],
                action="stop_loss",
                reason=reason,
            )
            if action:
                actions.append(action)
        return actions

    # ──────────────────────────────────────────
    # KIS 매도 주문
    # ──────────────────────────────────────────

    def _place_sell(
        self,
        ticker: str,
        quantity: int,
        current_price: float,
        action: str,
        reason: str,
    ) -> dict | None:
        """KIS API 시장가 매도 주문 + trades 테이블 저장."""
        if quantity <= 0:
            return None

        gw = KISGateway()
        tr_id = "VTTC0801U" if settings.KIS_MODE == "paper" else "TTTC0801U"

        try:
            resp = gw.request(
                method="POST",
                path=_KIS_ORDER_PATH,
                body={
                    "CANO": settings.KIS_ACCOUNT_NO.split("-")[0],
                    "ACNT_PRDT_CD": (settings.KIS_ACCOUNT_NO.split("-") + ["01"])[1],
                    "PDNO": ticker,
                    "ORD_DVSN": "01",        # 시장가
                    "ORD_QTY": str(quantity),
                    "ORD_UNPR": "0",         # 시장가이므로 0
                    "ALGO_NO": "",
                },
                tr_id=tr_id,
                priority=RequestPriority.TRADING,
            )
            order_no = resp.get("output", {}).get("ODNO", "")
            exec_price = current_price  # 체결가는 나중에 조회 가능

            # trades 테이블 저장
            _record_trade(
                ticker=ticker,
                action=action,
                quantity=quantity,
                exec_price=exec_price,
                signal_source="position_monitor",
                reason=reason,
            )

            logger.info(
                f"매도 주문 완료 [{action}] {ticker} {quantity}주 "
                f"@ {exec_price:,.0f}원 | 주문번호 {order_no}"
            )

            return {
                "ticker": ticker,
                "action": action,
                "quantity": quantity,
                "exec_price": exec_price,
                "order_no": order_no,
                "reason": reason,
            }

        except Exception as e:
            logger.error(f"매도 주문 실패 [{ticker}]: {e}")
            return None


# ──────────────────────────────────────────────
# KIS 잔고 조회
# ──────────────────────────────────────────────

def _fetch_positions() -> list[dict]:
    """
    KIS API에서 보유 포지션 목록 조회.
    보유 수량 0인 종목 제외.
    """
    gw = KISGateway()
    acnt_no, acnt_prdt_cd = (settings.KIS_ACCOUNT_NO.split("-") + ["01"])[:2]
    tr_id = "VTTC8434R" if settings.KIS_MODE == "paper" else "TTTC8434R"

    try:
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
            priority=RequestPriority.POSITION_MONITOR,
        )

        output1 = resp.get("output1", [])
        positions = []
        for item in output1:
            qty = int(item.get("hldg_qty", 0) or 0)
            if qty == 0:
                continue

            avg_price = float(item.get("pchs_avg_pric", 0) or 0)
            current_price = float(item.get("prpr", 0) or 0)
            pnl_pct = float(item.get("evlu_pfls_rt", 0) or 0)

            # 보유 영업일 계산 (position_snapshot에서 최초 진입 날짜 참조)
            ticker = item.get("pdno", "")
            held_days = _calc_held_days(ticker)

            # partial_sold: 이미 익절 매도한 횟수 (trades 테이블에서 카운트)
            partial_sold = _count_partial_sells(ticker)

            positions.append({
                "ticker": ticker,
                "name": item.get("prdt_name", ""),
                "quantity": qty,
                "avg_price": avg_price,
                "current_price": current_price,
                "pnl_pct": pnl_pct,
                "held_days": held_days,
                "partial_sold": partial_sold,
            })

        return positions

    except Exception as e:
        logger.warning(f"KIS 잔고 조회 실패: {e}")
        return []


# ──────────────────────────────────────────────
# 보유 기간 계산
# ──────────────────────────────────────────────

def _calc_held_days(ticker: str) -> int:
    """
    trades 테이블에서 최초 매수일 조회 → 오늘까지 영업일 수 계산.
    데이터 없으면 0 반환.
    """
    try:
        row = fetch_one(
            """
            SELECT date FROM trades
            WHERE ticker = ? AND action = 'buy' AND status = 'filled'
            ORDER BY date ASC LIMIT 1
            """,
            (ticker,),
        )
        if not row:
            return 0
        buy_date = date.fromisoformat(row["date"])
        today = date.today()
        # 영업일 근사: 전체 일수에서 주말 제외 (공휴일 미반영)
        delta = (today - buy_date).days
        weekdays = sum(
            1 for i in range(delta)
            if (buy_date + timedelta(days=i)).weekday() < 5
        )
        return weekdays
    except Exception:
        return 0


def _count_partial_sells(ticker: str) -> int:
    """오늘 해당 종목의 take_profit 매도 횟수 (분할 익절 추적)."""
    try:
        rows = fetch_all(
            """
            SELECT COUNT(*) as cnt FROM trades
            WHERE ticker = ? AND action = 'take_profit'
              AND date = ? AND status IN ('filled', 'pending')
            """,
            (ticker, str(date.today())),
        )
        return int(rows[0]["cnt"]) if rows else 0
    except Exception:
        return 0


# ──────────────────────────────────────────────
# DB 저장
# ──────────────────────────────────────────────

def _save_snapshots(positions: list[dict]) -> None:
    """현재 포지션을 position_snapshot 테이블에 저장."""
    now = datetime.now().isoformat(timespec="seconds")
    for pos in positions:
        execute(
            """
            INSERT INTO position_snapshot
                (ticker, name, quantity, avg_price, current_price,
                 pnl_pct, held_days, partial_sold, snapshot_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pos["ticker"],
                pos["name"],
                pos["quantity"],
                pos["avg_price"],
                pos["current_price"],
                pos["pnl_pct"],
                pos["held_days"],
                pos.get("partial_sold", 0),
                now,
            ),
        )


def _record_trade(
    ticker: str,
    action: str,
    quantity: int,
    exec_price: float,
    signal_source: str,
    reason: str,
) -> None:
    """trades 테이블에 매도 이력 기록."""
    execute(
        """
        INSERT INTO trades
            (date, ticker, action, order_type, exec_price,
             quantity, status, signal_source, strategy_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(date.today()),
            ticker,
            action,
            "market",
            exec_price,
            quantity,
            "filled",       # 시장가이므로 즉시 체결 처리
            signal_source,
            reason[:50],    # strategy_id 컬럼 임시 활용
        ),
    )
