"""
engine.py — 포지션 감시 서브엔진

역할:
  보유 포지션을 1~2분 주기로 감시하여 손절·익절·타임컷을 자동 실행한다.
  매매팀과 독립적으로 동작하며, 위기 관리팀의 리스크 레벨을 실시간 참조한다.

손절 기준:
  트레일링 스톱 (기본): 매수가 대비 -10% 초기 손절선, 수익 시 손절선 상향
  고정 손절 (트레일링 미등록 포지션): 리스크 레벨 연동

트레일링 스톱:
  초기 손절선 = 매수가 × (1 - TRAILING_INITIAL_STOP_PCT)
  매수가 대비 +TRAILING_TRIGGER_PCT% 수익 시 손절선 상향 시작
  손절선 = max(현재 손절선, 현재가 × (1 - TRAILING_FLOOR_PCT))
  손절선은 절대 내려가지 않음

사다리 매수:
  현재가 ≤ 매수가 × (1 - LADDER_TRIGGER_PCT) 이고 아직 미실행 시
  보유 수량 × LADDER_QTY_RATIO 만큼 추가 매수

분할 익절:
  +5%  도달 시 보유량의 1/3 매도 (1차)
  +10% 도달 시 보유량의 1/3 매도 (2차)
  나머지는 트레일링 스톱 또는 타임컷까지 유지

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

# 트레일링 스톱 파라미터
_TRAILING_INITIAL_STOP = settings.TRAILING_INITIAL_STOP_PCT  # 초기 손절선 %
_TRAILING_TRIGGER     = settings.TRAILING_TRIGGER_PCT         # 손절선 상향 시작 수익률 %
_TRAILING_FLOOR       = settings.TRAILING_FLOOR_PCT           # 트레일링 간격 %
_LADDER_TRIGGER       = settings.LADDER_TRIGGER_PCT           # 사다리 매수 발동 하락률 %
_LADDER_QTY_RATIO     = settings.LADDER_QTY_RATIO             # 사다리 매수 수량 배율


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
        단일 포지션에 대해 트레일링 스톱·사다리 매수·익절·타임컷 판단.

        Returns:
            실행된 액션 딕셔너리 또는 None
        """
        ticker = pos["ticker"]
        pnl_pct = pos["pnl_pct"]
        held_days = pos["held_days"]
        quantity = pos["quantity"]
        current_price = pos["current_price"]
        avg_price = pos["avg_price"]
        partial_sold = pos.get("partial_sold", 0)

        # ── MACD 조기 손절 (최우선) ─────────────
        if settings.MACD_EARLY_EXIT_ENABLED:
            from src.teams.intraday_macd.engine import get_latest_macd_signal
            macd_sig = get_latest_macd_signal(ticker, max_age_minutes=6)
            min_loss = settings.MACD_EARLY_EXIT_MIN_LOSS_PCT
            if macd_sig == "sell_pre" and pnl_pct <= -min_loss:
                logger.warning(
                    f"[MACD 조기손절] {ticker} | MACD 역행 + 손익 {pnl_pct:+.2f}% | "
                    f"기준 -{min_loss:.1f}% | {quantity}주 전량 청산"
                )
                _delete_trailing_stop(ticker)
                from src.utils.notifier import notify
                notify(
                    f"⚡ <b>[MACD 조기손절]</b> {ticker}\n"
                    f"분봉 MACD 역행 감지 + 손익 {pnl_pct:+.2f}%\n"
                    f"{quantity}주 즉시 청산"
                )
                return self._place_sell(
                    ticker=ticker,
                    quantity=quantity,
                    current_price=current_price,
                    action="stop_loss",
                    reason=f"MACD 조기손절 (sell_pre, 손익 {pnl_pct:+.2f}%)",
                )

        # ── 트레일링 스톱 ────────────────────────
        ts = _load_trailing_stop(ticker)
        if ts:
            # 손절선 업데이트 (단방향 상승)
            updated_floor = _update_trailing_floor(ticker, ts, current_price, avg_price, quantity)
            trailing_floor = updated_floor

            # 트레일링 스톱 발동 (현재가 ≤ 손절선)
            if current_price <= trailing_floor:
                logger.warning(
                    f"[트레일링 스톱] {ticker} | 현재가 {current_price:,.0f} ≤ "
                    f"손절선 {trailing_floor:,.0f} | 손익 {pnl_pct:+.2f}%"
                )
                _delete_trailing_stop(ticker)
                from src.utils.notifier import notify
                notify(
                    f"🔻 <b>[트레일링 스톱]</b> {ticker}\n"
                    f"현재가 {current_price:,.0f}원 ≤ 손절선 {trailing_floor:,.0f}원\n"
                    f"손익 {pnl_pct:+.2f}% | {quantity}주 전량 매도"
                )
                return self._place_sell(
                    ticker=ticker,
                    quantity=quantity,
                    current_price=current_price,
                    action="stop_loss",
                    reason=f"트레일링 스톱 발동 (손절선 {trailing_floor:,.0f}원)",
                )

            # 사다리 매수 (현재가 ≤ 매수가 × (1 - LADDER_TRIGGER%) 이고 미실행)
            if ts["ladder_bought"] == 0:
                ladder_trigger_price = avg_price * (1 - _LADDER_TRIGGER / 100)
                if current_price <= ladder_trigger_price:
                    ladder_qty = max(1, int(quantity * _LADDER_QTY_RATIO))
                    logger.info(
                        f"[사다리 매수] {ticker} | 현재가 {current_price:,.0f} ≤ "
                        f"발동가 {ladder_trigger_price:,.0f} | {ladder_qty}주 추가 매수"
                    )
                    result = self._place_buy(
                        ticker=ticker,
                        quantity=ladder_qty,
                        current_price=current_price,
                        reason=f"사다리 매수 (하락 {pnl_pct:+.2f}% ≤ -{_LADDER_TRIGGER:.0f}%)",
                    )
                    if result:
                        _mark_ladder_bought(ticker)
                    return result

        else:
            # 트레일링 스톱 미등록 포지션 → 기존 고정 손절 유지
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
            _delete_trailing_stop(ticker)
            return self._place_sell(
                ticker=ticker,
                quantity=quantity,
                current_price=current_price,
                action="time_cut",
                reason=f"{held_days}영업일 초과 ({_MAX_HOLD_DAYS}일 기준)",
            )

        # ── 분할 익절 ────────────────────────────
        if pnl_pct >= _TAKE_PROFIT_2_PCT and partial_sold >= 1:
            sell_qty = max(1, quantity // 3)
            logger.info(f"[익절 2차] {ticker} | 손익 {pnl_pct:+.2f}% | {sell_qty}주")
            return self._place_sell(
                ticker=ticker,
                quantity=sell_qty,
                current_price=current_price,
                action="take_profit",
                reason=f"2차 익절 {pnl_pct:+.2f}% ≥ +{_TAKE_PROFIT_2_PCT:.0f}%",
            )

        if pnl_pct >= _TAKE_PROFIT_1_PCT and partial_sold == 0:
            sell_qty = max(1, quantity // 3)
            logger.info(f"[익절 1차] {ticker} | 손익 {pnl_pct:+.2f}% | {sell_qty}주")
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
            _delete_trailing_stop(pos["ticker"])
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

        # 이중 매도 방지: 거래소에 걸어둔 지정가 손절 주문 먼저 취소
        from src.infra.stop_order_manager import cancel_stop_order
        cancel_stop_order(ticker)

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


    def _place_buy(
        self,
        ticker: str,
        quantity: int,
        current_price: float,
        reason: str,
    ) -> dict | None:
        """사다리 매수 — KIS API 시장가 매수 주문."""
        if quantity <= 0:
            return None

        gw = KISGateway()
        tr_id = "VTTC0802U" if settings.KIS_MODE == "paper" else "TTTC0802U"
        acnt_no, acnt_prdt_cd = (settings.KIS_ACCOUNT_NO.split("-") + ["01"])[:2]

        try:
            resp = gw.request(
                method="POST",
                path=_KIS_ORDER_PATH,
                body={
                    "CANO": acnt_no,
                    "ACNT_PRDT_CD": acnt_prdt_cd,
                    "PDNO": ticker,
                    "ORD_DVSN": "01",
                    "ORD_QTY": str(quantity),
                    "ORD_UNPR": "0",
                    "ALGO_NO": "",
                },
                tr_id=tr_id,
                priority=RequestPriority.TRADING,
            )
            order_no = resp.get("output", {}).get("ODNO", "")

            _record_trade(
                ticker=ticker,
                action="buy",
                quantity=quantity,
                exec_price=current_price,
                signal_source="position_monitor",
                reason=reason,
            )

            from src.utils.notifier import notify_trade
            notify_trade(
                ticker=ticker, name=ticker,
                action="buy", quantity=quantity,
                price=current_price, reason=reason,
            )
            logger.info(
                f"사다리 매수 완료 {ticker} {quantity}주 "
                f"@ {current_price:,.0f}원 | {reason}"
            )
            return {"ticker": ticker, "action": "ladder_buy", "quantity": quantity,
                    "exec_price": current_price, "order_no": order_no, "reason": reason}

        except Exception as e:
            logger.error(f"사다리 매수 실패 [{ticker}]: {e}")
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


# ──────────────────────────────────────────────
# 트레일링 스톱 헬퍼
# ──────────────────────────────────────────────

def _load_trailing_stop(ticker: str) -> dict | None:
    """trailing_stop 테이블에서 해당 종목 레코드 조회."""
    try:
        row = fetch_one("SELECT * FROM trailing_stop WHERE ticker = ?", (ticker,))
        if row:
            return dict(row)
        return None
    except Exception:
        return None


def _update_trailing_floor(
    ticker: str, ts: dict, current_price: float, avg_price: float, quantity: int
) -> float:
    """
    트레일링 손절선 업데이트.
    수익이 TRAILING_TRIGGER% 이상이면 손절선을 현재가 기준 TRAILING_FLOOR% 아래로 상향.
    손절선은 절대 내려가지 않음.
    손절선이 실제로 올라간 경우 거래소 사전 손절 주문도 취소 후 재제출.
    """
    current_floor = float(ts["trailing_floor"])
    highest = float(ts["highest_price"])

    # 최고가 갱신
    new_highest = max(highest, current_price)

    # 손절선 상향 조건: 현재 수익 ≥ TRAILING_TRIGGER%
    gain_pct = (current_price / avg_price - 1) * 100
    if gain_pct >= _TRAILING_TRIGGER:
        candidate_floor = current_price * (1 - _TRAILING_FLOOR / 100)
        new_floor = max(current_floor, candidate_floor)
    else:
        new_floor = current_floor

    # DB 업데이트
    execute(
        """
        UPDATE trailing_stop
        SET trailing_floor = ?, highest_price = ?, updated_at = CURRENT_TIMESTAMP
        WHERE ticker = ?
        """,
        (new_floor, new_highest, ticker),
    )

    if new_floor > current_floor:
        logger.info(
            f"[트레일링] {ticker} 손절선 상향: {current_floor:,.0f} → {new_floor:,.0f}원 "
            f"(현재가 {current_price:,.0f}, 수익 {gain_pct:+.1f}%)"
        )
        # 거래소 사전 손절 주문도 새 손절선으로 업데이트
        from src.infra.stop_order_manager import update_stop_order
        update_stop_order(ticker, quantity, new_floor)

    return new_floor


def _delete_trailing_stop(ticker: str) -> None:
    """포지션 청산 시 트레일링 스톱 레코드 삭제."""
    try:
        execute("DELETE FROM trailing_stop WHERE ticker = ?", (ticker,))
    except Exception:
        pass


def _mark_ladder_bought(ticker: str) -> None:
    """사다리 매수 실행 완료 표시."""
    try:
        execute(
            "UPDATE trailing_stop SET ladder_bought = ladder_bought + 1 WHERE ticker = ?",
            (ticker,),
        )
    except Exception:
        pass


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
