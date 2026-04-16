"""
engine.py — 포지션 감시 서브엔진

역할:
  보유 포지션을 1~2분 주기로 감시하여 손절·익절·타임컷을 자동 실행한다.
  매매팀과 독립적으로 동작하며, 위기 관리팀의 리스크 레벨을 실시간 참조한다.

손절 기준:
  트레일링 스톱 (기본): 매수가 대비 -5% 초기 손절선, 수익 시 손절선 상향
  고정 손절 (트레일링 미등록 포지션): 리스크 레벨 연동

트레일링 스톱 (동적):
  초기 손절선 = 매수가 × (1 - TRAILING_INITIAL_STOP_PCT)
  매수가 대비 +TRAILING_TRIGGER_PCT% 수익 시 손절선 상향 시작
  손절선 = max(현재 손절선, 현재가 × (1 - TRAILING_FLOOR_PCT))
  MACD sell_pre 신호 발생 시 손절선 간격 절반으로 타이트하게 조임
  손절선은 절대 내려가지 않음

사다리 매수 (하락 시 평단 낮추기):
  현재가 ≤ 매수가 × (1 - LADDER_TRIGGER_PCT) 이고 아직 미실행 시
  보유 수량 × LADDER_QTY_RATIO 만큼 추가 매수

피라미딩 (상승 시 비중 추가 — 스마트 scale-in):
  수익 +SCALE_IN_TRIGGER_PCT% 이상 + MACD buy_pre/buy 신호 시
  최대 SCALE_IN_MAX회까지 원래 수량의 SCALE_IN_QTY_RATIO만큼 추가 매수
  추가 매수 후 entry_price 갱신 → 트레일링 스톱 자동 재조정

동적 익절 (MACD 연동):
  +5% 도달 시 MACD 확인
    → MACD bullish: 익절 보류 + 손절선을 매수가+1% 이상으로 상향 (수익 확보)
    → MACD neutral/bearish: 보유량 1/3 매도
  +10% 도달 시 동일 로직
  MACD가 sell_pre로 돌아서는 순간 익절 즉시 실행

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

# 익절 목표
_TAKE_PROFIT_1_PCT = settings.TAKE_PROFIT_1_PCT   # +5% → MACD 판단 후 1/3 매도 or 보류
_TAKE_PROFIT_2_PCT = settings.TAKE_PROFIT_2_PCT   # +10% → 동일 로직
_MAX_HOLD_DAYS = settings.POSITION_MAX_HOLD_DAYS  # 5 영업일

# 트레일링 스톱 파라미터
_TRAILING_INITIAL_STOP = settings.TRAILING_INITIAL_STOP_PCT  # 초기 손절선 %
_TRAILING_TRIGGER     = settings.TRAILING_TRIGGER_PCT         # 손절선 상향 시작 수익률 %
_TRAILING_FLOOR       = settings.TRAILING_FLOOR_PCT           # 트레일링 간격 %
_TRAILING_TIGHT_FLOOR = _TRAILING_FLOOR / 2                   # MACD 약화 시 절반으로 타이트하게

# 사다리 / 피라미딩 파라미터 — 기본값 (DB strategy_params가 우선, 없으면 이 값 사용)
_LADDER_TRIGGER       = settings.LADDER_TRIGGER_PCT
_LADDER_QTY_RATIO     = settings.LADDER_QTY_RATIO
_SCALE_IN_TRIGGER_PCT = 3.0
_SCALE_IN_QTY_RATIO   = 0.3
_SCALE_IN_MAX         = 2

# 스마트 물타기 기본값
_DIP_BUY_MIN_LOSS     = -1.0
_DIP_BUY_MAX_LOSS     = -4.9
_DIP_BUY_QTY_RATIO    = 0.25
_DIP_BUY_MAX          = 2
_DIP_BUY_VOL_MIN      = 1.5
_DIP_BUY_HOTLIST_MIN  = 15

# 물타기 후 MACD 반등 탈출 기본값
_MACD_REVERSAL_EXIT_MIN_PNL = 0.3
_MACD_REVERSAL_EXIT_MAX_PNL = 4.9

# 불타기 기본값
_FIRE_BUY_TRIGGER_PCT = 1.5
_FIRE_BUY_QTY_RATIO   = 0.5
_FIRE_BUY_VOL_MIN     = 2.0
_FIRE_BUY_HOTLIST_MIN = 30

# MACD 신호 분류
_MACD_BULLISH  = {"buy_pre", "buy"}
_MACD_BEARISH  = {"sell_pre", "sell"}


def _p(name: str, fallback: float) -> float:
    """DB strategy_params 우선 조회. 자동 튜닝 값이 있으면 반환, 없으면 fallback."""
    from src.teams.research.param_tuner import get_param
    return get_param(name, fallback)


class PositionMonitorEngine:
    """포지션 감시 서브엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="position-monitor-engine",
        )
        # WebSocket 실시간 구독 관리
        self._ws_subscribed: set[str] = set()   # 현재 구독 중인 종목
        self._ws_triggered: set[str] = set()    # WebSocket이 매도 트리거한 종목 (폴링 중복 방지)
        # 콜백에서 _place_sell 호출 시 필요한 최신 수량 캐시
        self._qty_cache: dict[str, int] = {}    # {ticker: quantity}
        # 종목별 이전 MACD 상태 — 반등 전환(bearish/None → bullish) 감지용
        self._macd_prev: dict[str, str | None] = {}  # {ticker: last_signal}
        # 이전 사이클 MACD 히스토그램 — 모멘텀 피크 감지용 (동적 스캘핑)
        self._macd_hist_prev: dict[str, float] = {}  # {ticker: prev hist_3m}

    def start(self) -> None:
        logger.info("포지션 감시 엔진 시작")
        self._thread.start()
        # WebSocket 클라이언트 기동 (싱글턴 — 이미 실행 중이면 무시)
        try:
            from src.infra.kis_websocket import KISWebSocket
            KISWebSocket()
            logger.info("KIS WebSocket 클라이언트 연결 대기 중")
        except Exception as e:
            logger.warning(f"KIS WebSocket 초기화 실패 (폴링으로 대체): {e}")

    def stop(self) -> None:
        self._stop_event.set()
        # 모든 WebSocket 구독 해제
        try:
            from src.infra.kis_websocket import KISWebSocket
            ws = KISWebSocket()
            for ticker in list(self._ws_subscribed):
                ws.unsubscribe(ticker)
        except Exception:
            pass
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

        KIS 잔고 API 실패 시 폴백 순서:
          1순위: KIS 잔고 API (실시간 가격 포함)
          2순위: DB 스냅샷 + KIS 현재가 개별 조회
          3순위: DB 스냅샷 마지막 가격 그대로 사용 (stale 가능성 있지만 손절 판단은 유지)

        Returns:
            처리된 액션 목록
        """
        # 1. 리스크 레벨 참조
        risk = get_current_risk()
        level = risk.get("risk_level", 1)
        stop_loss_pct = get_stop_loss_pct()

        # 2. KIS 잔고 조회 (실패 시 DB 스냅샷 폴백)
        positions = _fetch_positions()
        if not positions:
            positions = _fetch_positions_from_snapshot()
            if positions:
                logger.warning(
                    f"KIS 잔고 API 실패 → DB 스냅샷 폴백 ({len(positions)}종목) — 가격 stale 가능"
                )
        if not positions:
            self._sync_ws_subscriptions([])
            return []

        # 3. 스냅샷 저장 + 수량 캐시 갱신
        _save_snapshots(positions)
        for pos in positions:
            self._qty_cache[pos["ticker"]] = pos["quantity"]

        # 4. WebSocket 구독 동기화 (신규 종목 구독 / 청산 종목 해제)
        self._sync_ws_subscriptions(positions)

        # 5. Level 5 → 전량 청산
        if level >= 5:
            actions = self._liquidate_all(positions, reason="level5_emergency")
            return actions

        # 6. 초과 포지션 정리 (보유 수 > max_positions)
        from src.teams.research.param_tuner import get_param
        max_pos = int(get_param("max_positions", 5.0))
        if len(positions) > max_pos:
            excess = len(positions) - max_pos
            # PnL 낮은 순(손실 큰 것부터) 정렬해서 초과분 청산
            sorted_pos = sorted(positions, key=lambda p: p["pnl_pct"])
            for pos in sorted_pos[:excess]:
                logger.info(
                    f"[초과 포지션 정리] {pos['ticker']} | 손익 {pos['pnl_pct']:+.2f}% | "
                    f"보유 {len(positions)}종목 > 최대 {max_pos}종목"
                )
                from src.utils.notifier import notify
                notify(
                    f"📉 <b>[초과 포지션 정리]</b> {pos.get('name', pos['ticker'])}({pos['ticker']})\n"
                    f"보유 {len(positions)}종목 → 최대 {max_pos}종목 초과\n"
                    f"손익 {pos['pnl_pct']:+.2f}% | {pos['quantity']}주 전량 청산"
                )
                result = self._place_sell(
                    ticker=pos["ticker"],
                    quantity=pos["quantity"],
                    current_price=pos["current_price"],
                    action="time_cut",
                    reason=f"초과 포지션 정리 ({len(positions)}종목>{max_pos}종목)",
                    avg_price=pos["avg_price"],
                    name=pos.get("name", pos["ticker"]),
                )
                if result:
                    positions = [p for p in positions if p["ticker"] != pos["ticker"]]

        # 7. 개별 종목 감시 (WebSocket이 이미 트리거한 종목은 스킵)
        actions = []
        for pos in positions:
            if pos["ticker"] in self._ws_triggered:
                logger.debug(f"[WS 트리거됨] {pos['ticker']} — 폴링 스킵")
                continue
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
        단일 포지션에 대해 스마트 포지션 관리 실행.

        판단 순서:
          0. MACD 신호 수집 (이후 모든 판단에 활용)
          1. MACD 조기 손절 (최우선)
          2. 트레일링 스톱 — MACD sell_pre 시 간격 절반으로 타이트하게 조임
          3. 스마트 물타기 (일시 눌림 + 반등 기대)
          3.5. 물타기 후 MACD 반등 탈출 — 목표가 미달이더라도 소폭 수익 시 조기 익절
          3.7. 불타기 — 수익 +1.5% + MACD 강세 + Hot List 모멘텀 확인 → 비중 50% 추가
          4. 피라미딩 — 수익 +3% + MACD 강세 (Hot List 없어도 됨) → 비중 30% 추가
          5. 타임컷
          6. 동적 익절 — MACD bullish면 목표 상향/보류, bearish면 즉시 실행

        Returns:
            실행된 액션 딕셔너리 또는 None
        """
        ticker = pos["ticker"]
        name = pos.get("name", ticker)
        pnl_pct = pos["pnl_pct"]
        held_days = pos["held_days"]
        quantity = pos["quantity"]
        current_price = pos["current_price"]
        avg_price = pos["avg_price"]
        partial_sold = pos.get("partial_sold", 0)

        # ── 0. MACD 신호 수집 (히스토그램 포함) ──
        from src.teams.intraday_macd.engine import get_macd_details
        macd_details  = get_macd_details(ticker, max_age_minutes=6)
        macd_sig      = macd_details["signal"]
        hist_3m_now   = macd_details["hist_3m"] or 0.0
        macd_bullish  = macd_sig in _MACD_BULLISH
        macd_bearish  = macd_sig in _MACD_BEARISH

        # 이전 사이클 대비 bullish 전환 여부 (bearish/None → bullish)
        prev_macd = self._macd_prev.get(ticker)
        macd_just_turned_bullish = macd_bullish and (prev_macd not in _MACD_BULLISH)
        self._macd_prev[ticker] = macd_sig  # 이번 상태 저장 (다음 사이클용)

        # 이전 사이클 히스토그램 — 모멘텀 피크 감지
        hist_3m_prev = self._macd_hist_prev.get(ticker, hist_3m_now)
        self._macd_hist_prev[ticker] = hist_3m_now

        # ── 1. MACD 조기 손절 (최우선) ─────────
        # 1a. MACD 역행 (bearish) + 손익 ≥ 기준선 → 즉시 컷
        if settings.MACD_EARLY_EXIT_ENABLED and macd_bearish:
            min_loss = settings.MACD_EARLY_EXIT_MIN_LOSS_PCT  # 기본 0.0 → 브레이크이븐에서 컷
            if pnl_pct <= -min_loss:
                logger.warning(
                    f"[MACD 조기손절] {ticker} | MACD 역행 + 손익 {pnl_pct:+.2f}% | "
                    f"기준 -{min_loss:.1f}% | {quantity}주 전량 청산"
                )
                _delete_trailing_stop(ticker)
                from src.utils.notifier import notify
                notify(
                    f"⚡ <b>[MACD 조기손절]</b> {name}({ticker})\n"
                    f"분봉 MACD 역행 감지 + 손익 {pnl_pct:+.2f}%\n"
                    f"{quantity}주 즉시 청산"
                )
                return self._place_sell(
                    ticker=ticker, quantity=quantity, current_price=current_price,
                    action="stop_loss", reason=f"MACD 조기손절 (bearish, 손익 {pnl_pct:+.2f}%)",
                    avg_price=avg_price, name=name,
                )

        # 1b. MACD 중립 (bullish 아님) + 손실 -2% 초과 → 모멘텀 소멸 조기 컷
        #     트레일링 -3%까지 기다리지 않고 모멘텀 없는 손실은 빠르게 정리
        _neutral_cut_pct = _p("neutral_cut_pct", 2.0)
        if not macd_bullish and pnl_pct <= -_neutral_cut_pct:
            logger.warning(
                f"[중립 조기컷] {ticker} | MACD 비강세({macd_sig}) + 손익 {pnl_pct:+.2f}% | "
                f"기준 -{_neutral_cut_pct:.1f}% | {quantity}주 전량 청산"
            )
            _delete_trailing_stop(ticker)
            from src.utils.notifier import notify
            notify(
                f"✂️ <b>[모멘텀 소멸 컷]</b> {name}({ticker})\n"
                f"MACD {macd_sig} + 손익 {pnl_pct:+.2f}%\n"
                f"모멘텀 없는 손실 — {quantity}주 조기 청산"
            )
            return self._place_sell(
                ticker=ticker, quantity=quantity, current_price=current_price,
                action="stop_loss", reason=f"중립 조기컷 (MACD {macd_sig}, 손익 {pnl_pct:+.2f}%)",
                avg_price=avg_price, name=name,
            )

        # ── 2. 트레일링 스톱 ────────────────────
        ts = _load_trailing_stop(ticker)
        if ts:
            # MACD bearish면 trailing 간격 절반으로 타이트하게
            tight = macd_bearish
            updated_floor = _update_trailing_floor(
                ticker, ts, current_price, avg_price, quantity, tight=tight
            )
            trailing_floor = updated_floor

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
                    ticker=ticker, quantity=quantity, current_price=current_price,
                    action="stop_loss", reason=f"트레일링 스톱 발동 (손절선 {trailing_floor:,.0f}원)",
                    avg_price=avg_price, name=name,
                )

            # ── 3. 스마트 물타기 (일시 눌림 반등 기대) ──
            dip_min  = _p("dip_buy_min_loss",  _DIP_BUY_MIN_LOSS)
            dip_max  = _p("dip_buy_max_loss",  _DIP_BUY_MAX_LOSS)
            dip_max_count = int(_p("dip_buy_max_count", _DIP_BUY_MAX))
            dip_buy_count = int(ts.get("dip_buy_count", 0))
            if (
                dip_min >= pnl_pct >= dip_max
                and not macd_bearish
                and dip_buy_count < dip_max_count
                and risk_level < 4
            ):
                hot = _check_hotlist_for_dip(ticker)
                if hot:
                    dip_qty = max(1, int(quantity * _p("dip_buy_qty_ratio", _DIP_BUY_QTY_RATIO)))
                    vol_ratio = hot.get("volume_ratio", 0)
                    logger.info(
                        f"[스마트 물타기] {ticker} | 눌림 {pnl_pct:+.2f}% | "
                        f"MACD {macd_sig} | 거래량 {vol_ratio:.1f}배 | "
                        f"{dip_qty}주 추가 ({dip_buy_count+1}/{_DIP_BUY_MAX}회)"
                    )
                    from src.utils.notifier import notify
                    notify(
                        f"💧 <b>[스마트 물타기]</b> {hot.get('name', ticker)}({ticker})\n"
                        f"눌림 {pnl_pct:+.2f}% | MACD {macd_sig} | 거래량 {vol_ratio:.1f}배\n"
                        f"{dip_qty}주 추가 — 반등 기대 ({dip_buy_count+1}/{_DIP_BUY_MAX}회)"
                    )
                    result = self._place_buy(
                        ticker=ticker,
                        quantity=dip_qty,
                        current_price=current_price,
                        reason=f"스마트 물타기 (눌림 {pnl_pct:+.2f}%, MACD {macd_sig}, 거래량 {vol_ratio:.1f}배)",
                    )
                    if result:
                        _increment_dip_buy(ticker)
                        # 새 평단 갱신 (손절선은 내리지 않음)
                        new_avg = (avg_price * quantity + current_price * dip_qty) / (quantity + dip_qty)
                        _update_entry_price(ticker, new_avg)
                    return result

            # ── 3.5. 물타기 후 MACD 반등 탈출 ────
            # 물타기를 한 번이라도 했고 + MACD가 bearish/neutral에서 bullish로 전환된 순간 +
            # 목표가 미달이더라도 소폭 수익 구간이면 바로 전량 익절 후 탈출
            # → 손실 복구 확정 후 재진입 전략 (물타기 후 눌리다 반등 시 탈출 최적 타이밍)
            dip_buy_done = int(ts.get("dip_buy_count", 0)) > 0
            reversal_min = _p("macd_reversal_exit_min", _MACD_REVERSAL_EXIT_MIN_PNL)
            if (
                dip_buy_done
                and macd_just_turned_bullish
                and reversal_min <= pnl_pct <= _MACD_REVERSAL_EXIT_MAX_PNL
            ):
                logger.info(
                    f"[MACD 반등 탈출] {ticker} | 물타기 이력 있음 | "
                    f"MACD {prev_macd} → {macd_sig} (bullish 전환) | "
                    f"손익 {pnl_pct:+.2f}% — 목표가 미달이지만 조기 익절"
                )
                _delete_trailing_stop(ticker)
                from src.utils.notifier import notify
                notify(
                    f"🔄 <b>[MACD 반등 탈출]</b> {ticker}\n"
                    f"물타기 후 MACD {prev_macd} → {macd_sig} 전환 감지\n"
                    f"손익 {pnl_pct:+.2f}% | {quantity}주 전량 익절 탈출\n"
                    f"(목표가 미달이지만 손실 복구 확정 후 재진입 전략)"
                )
                return self._place_sell(
                    ticker=ticker, quantity=quantity, current_price=current_price,
                    action="take_profit", reason=f"물타기 후 MACD 반등 탈출 ({prev_macd}→{macd_sig}, 손익 {pnl_pct:+.2f}%)",
                    avg_price=avg_price, name=name,
                )

            # ── 3.6. 동적 스캘핑 — 실시간 모멘텀 소진도 기반 부분 익절 + 재진입 ──
            #
            # 단순 고정 임계값 대신, 매 사이클 아래 신호들로 "소진도(exhaustion)" 산출:
            #   - MACD 히스토그램이 피크 찍고 감소 중  → 모멘텀 약화 시작
            #   - MACD 히스토그램이 음수로 전환        → 모멘텀 역전
            #   - MACD 신호가 sell_pre / sell          → 추세 반전 확인
            #   - 수익률 자체가 매우 높음 (≥5%)        → 수익 잠금 urgency
            #
            # 소진도 기반으로 임계값·매도비율 동적 결정:
            #   - 소진도 낮음 (모멘텀 살아있음): 더 높은 임계값, 적은 비율 매도 (30%)
            #   - 소진도 높음 (모멘텀 죽어가는 중): 낮은 임계값, 많은 비율 매도 (80%)
            #
            # 재진입: 단순 가격 -1% 대신 MACD buy_pre 신호 또는 충분한 눌림+MACD 회복
            # ────────────────────────────────────────────────────────────────────────
            ts_scalp_exit = float(ts.get("scalp_exit_price") or 0)
            ts_scalp_qty  = int(ts.get("scalp_exit_qty") or 0)

            # ── 3.6a. 부분 익절 판단 ─────────────────────────────────────────
            if ts_scalp_exit == 0 and partial_sold == 0 and pnl_pct > 0:
                # 모멘텀 소진도 계산 (0.0 ~ 1.0)
                exhaustion  = 0.0
                exh_signals = []

                # MACD 히스토그램 방향 (핵심 — 분봉 기준 실시간 모멘텀 판단)
                if macd_bearish:
                    # sell_pre/sell: 추세가 이미 역전, 즉시 비중 축소
                    exhaustion += 0.50
                    exh_signals.append(f"MACD역행({macd_sig})")
                elif hist_3m_now < 0 and hist_3m_prev >= 0:
                    # 히스토그램이 양수→음수 전환 (모멘텀 반전 확인)
                    exhaustion += 0.40
                    exh_signals.append("MACD음전환")
                elif hist_3m_now > 0 and hist_3m_now < hist_3m_prev:
                    # 양수 구간에서 감소 (피크 지남 = 모멘텀 약화 시작)
                    exhaustion += 0.25
                    exh_signals.append("MACD피크감소")

                # 수익률 자체도 반영 (고수익 = 수익 잠금 urgency)
                if pnl_pct >= 7.0:
                    exhaustion += 0.20
                    exh_signals.append(f"고수익+{pnl_pct:.1f}%")
                elif pnl_pct >= 4.5:
                    exhaustion += 0.10
                    exh_signals.append(f"수익+{pnl_pct:.1f}%")

                exhaustion = min(1.0, exhaustion)

                # 소진도 0.2 이상일 때만 스캘핑 고려
                if exhaustion >= 0.20:
                    # 임계값 동적 조정: 소진도 높을수록 낮은 수익에서도 익절
                    #   exhaustion=0.20 → effective = base × 0.89  (거의 그대로)
                    #   exhaustion=0.50 → effective = base × 0.73
                    #   exhaustion=1.00 → effective = base × 0.45  (절반 가까이 낮춤)
                    commission    = _p("commission_rate", 0.35)   # 왕복 수수료+세금
                    base_thr      = _p("scalp_profit_pct", 2.0)
                    effective_thr = base_thr * max(0.40, 1.0 - exhaustion * 0.55)
                    # 최소 임계값: 수수료 × 2 (왕복 손익분기 + 버퍼)
                    effective_thr = max(effective_thr, commission * 2)

                    if pnl_pct >= effective_thr:
                        # 매도 비율 동적 조정: 소진도 낮으면 적게, 높으면 많이
                        #   exhaustion=0.20 → ratio = 0.30 (모멘텀 남아있으면 30%만)
                        #   exhaustion=0.50 → ratio = 0.45
                        #   exhaustion=1.00 → ratio = 0.75
                        base_ratio      = _p("scalp_sell_ratio", 0.50)
                        effective_ratio = min(0.80, max(0.30, base_ratio - 0.20 + exhaustion * 0.50))
                        scalp_qty       = max(1, int(quantity * effective_ratio))
                        exh_str         = " | ".join(exh_signals) if exh_signals else "기준충족"

                        logger.info(
                            f"[동적 스캘핑] {ticker} | 수익 {pnl_pct:+.2f}% ≥ 임계 +{effective_thr:.1f}% | "
                            f"소진도 {exhaustion:.2f} ({exh_str}) | "
                            f"{scalp_qty}주 ({effective_ratio*100:.0f}%) 부분 익절"
                        )
                        from src.utils.notifier import notify
                        reload_dip = _p("scalp_reload_dip", 1.5)
                        notify(
                            f"✂️ <b>[동적 스캘핑]</b> {name}({ticker})\n"
                            f"수익 {pnl_pct:+.2f}% | 소진도 {exhaustion:.2f}\n"
                            f"신호: {exh_str}\n"
                            f"{scalp_qty}주 ({effective_ratio*100:.0f}%) 부분 익절\n"
                            f"재진입: MACD buy_pre 또는 -{reload_dip:.1f}% 눌림 시"
                        )
                        execute(
                            """UPDATE trailing_stop
                               SET scalp_exit_price = ?, scalp_exit_qty = ?, updated_at = CURRENT_TIMESTAMP
                               WHERE ticker = ?""",
                            (current_price, scalp_qty, ticker),
                        )
                        return self._place_sell(
                            ticker=ticker, quantity=scalp_qty, current_price=current_price,
                            action="take_profit",
                            reason=f"동적 스캘핑 {pnl_pct:+.2f}% (소진도{exhaustion:.2f}/{exh_str})",
                            avg_price=avg_price, name=name,
                        )

            # ── 3.6b. 재진입 판단 ────────────────────────────────────────────
            # 단순 -1% 가격 트리거 대신:
            #   우선순위 1: MACD buy_pre 전환 + 최소 눌림 (-0.3%) → 모멘텀 재개 신호
            #   우선순위 2: 충분한 눌림 (-scalp_reload_dip%) + MACD bearish 아닐 때
            if ts_scalp_exit > 0 and ts_scalp_qty > 0:
                from_exit_pct = (current_price - ts_scalp_exit) / ts_scalp_exit * 100

                reload = False
                reload_reason = ""

                if macd_sig == "buy_pre" and from_exit_pct <= -0.3:
                    # MACD buy_pre: 모멘텀 재개 신호 — 작은 눌림에도 재진입
                    reload = True
                    reload_reason = f"MACD buy_pre + 눌림{from_exit_pct:.1f}%"
                elif from_exit_pct <= -_p("scalp_reload_dip", 1.5) and not macd_bearish:
                    # 충분한 가격 눌림 + MACD 역행 아닐 때
                    reload = True
                    reload_reason = f"눌림{from_exit_pct:.1f}% (MACD:{macd_sig})"

                if reload:
                    logger.info(
                        f"[스캘핑 재진입] {ticker} | {reload_reason} | "
                        f"exit {ts_scalp_exit:,.0f}→현재 {current_price:,.0f} | {ts_scalp_qty}주 재매수"
                    )
                    from src.utils.notifier import notify
                    notify(
                        f"🔄 <b>[스캘핑 재진입]</b> {name}({ticker})\n"
                        f"{reload_reason}\n"
                        f"익절가 {ts_scalp_exit:,.0f}원 → 현재 {current_price:,.0f}원\n"
                        f"{ts_scalp_qty}주 재매수"
                    )
                    execute(
                        """UPDATE trailing_stop
                           SET scalp_exit_price = NULL, scalp_exit_qty = 0, updated_at = CURRENT_TIMESTAMP
                           WHERE ticker = ?""",
                        (ticker,),
                    )
                    result = self._place_buy(
                        ticker=ticker, quantity=ts_scalp_qty,
                        current_price=current_price,
                        reason=f"스캘핑 재진입 ({reload_reason})",
                    )
                    if result:
                        new_avg = (avg_price * quantity + current_price * ts_scalp_qty) / (quantity + ts_scalp_qty)
                        _update_entry_price(ticker, new_avg)
                    return result

            # ── 3.7. 불타기 (Fire Buy — 모멘텀 지속 확인 후 공격적 비중 추가) ──
            fire_trigger = _p("fire_buy_trigger_pct", _FIRE_BUY_TRIGGER_PCT)
            fire_vol_min = _p("fire_buy_vol_min",     _FIRE_BUY_VOL_MIN)
            scale_in_max = int(_p("scale_in_max_count", _SCALE_IN_MAX))
            scale_in_count = int(ts.get("scale_in_count", 0))
            if (
                pnl_pct >= fire_trigger
                and macd_bullish
                and scale_in_count < scale_in_max
                and risk_level < 4
            ):
                hot = _check_hotlist_for_fire(ticker)
                if hot and float(hot.get("volume_ratio") or 0) >= fire_vol_min:
                    fire_qty = max(1, int(quantity * _p("fire_buy_qty_ratio", _FIRE_BUY_QTY_RATIO)))
                    vol_ratio = float(hot.get("volume_ratio") or 0)
                    logger.info(
                        f"[불타기] {ticker} | 수익 {pnl_pct:+.2f}% | MACD {macd_sig} | "
                        f"거래량 {vol_ratio:.1f}배 | {fire_qty}주 추가 ({scale_in_count+1}/{_SCALE_IN_MAX}회)"
                    )
                    from src.utils.notifier import notify
                    notify(
                        f"🔥 <b>[불타기]</b> {hot.get('name', ticker)}({ticker})\n"
                        f"수익 {pnl_pct:+.2f}% + MACD {macd_sig} + 거래량 {vol_ratio:.1f}배\n"
                        f"{fire_qty}주 추가 — 모멘텀 지속 확인 ({scale_in_count+1}/{_SCALE_IN_MAX}회)"
                    )
                    result = self._place_buy(
                        ticker=ticker,
                        quantity=fire_qty,
                        current_price=current_price,
                        reason=f"불타기 (수익 {pnl_pct:+.2f}%, MACD {macd_sig}, 거래량 {vol_ratio:.1f}배)",
                    )
                    if result:
                        _increment_scale_in(ticker)
                        new_avg = (avg_price * quantity + current_price * fire_qty) / (quantity + fire_qty)
                        _update_entry_price(ticker, new_avg)
                    return result

            # ── 4. 피라미딩 (Hot List 없어도 됨 — 수익 +3% 이상) ──────
            scale_trigger = _p("scale_in_trigger_pct", _SCALE_IN_TRIGGER_PCT)
            if (
                pnl_pct >= scale_trigger
                and macd_bullish
                and scale_in_count < scale_in_max
                and risk_level < 4
            ):
                scale_qty = max(1, int(quantity * _p("scale_in_qty_ratio", _SCALE_IN_QTY_RATIO)))
                logger.info(
                    f"[피라미딩] {ticker} | 수익 {pnl_pct:+.2f}% + MACD {macd_sig} | "
                    f"{scale_qty}주 추가 매수 ({scale_in_count+1}/{_SCALE_IN_MAX}회)"
                )
                from src.utils.notifier import notify
                notify(
                    f"📈 <b>[피라미딩]</b> {ticker}\n"
                    f"수익 {pnl_pct:+.2f}% + MACD 강세 → {scale_qty}주 추가\n"
                    f"({scale_in_count+1}/{_SCALE_IN_MAX}회차)"
                )
                result = self._place_buy(
                    ticker=ticker,
                    quantity=scale_qty,
                    current_price=current_price,
                    reason=f"피라미딩 (수익 {pnl_pct:+.2f}%, MACD {macd_sig})",
                )
                if result:
                    _increment_scale_in(ticker)
                    new_avg = (avg_price * quantity + current_price * scale_qty) / (quantity + scale_qty)
                    _update_entry_price(ticker, new_avg)
                return result

        else:
            # 트레일링 스톱 미등록 포지션 → 고정 손절
            if pnl_pct <= -stop_loss_pct:
                logger.warning(
                    f"[손절] {ticker} | 손익 {pnl_pct:+.2f}% | "
                    f"기준 -{stop_loss_pct:.1f}% | {quantity}주 전량"
                )
                return self._place_sell(
                    ticker=ticker, quantity=quantity, current_price=current_price,
                    action="stop_loss", reason=f"손익 {pnl_pct:+.2f}% ≤ -{stop_loss_pct:.1f}%",
                    avg_price=avg_price, name=name,
                )

        # ── 4.5. 장마감 자동 청산 (단타 전략 핵심) ───────────────
        # 당일 매수·매도 원칙: 오버나잇은 예외적으로만 허용
        #
        # 14:50 ~ 15:20 (마감 30분~10분 전): 수익권이면 무조건 익절
        #   - 오버나잇 허용 예외: MACD 강세 + 수익 3% 이상 (강한 모멘텀)
        # 15:20 이후 (마감 10분 전): 손익 무관 전량 청산 (오버나잇 금지)
        _now = datetime.now()
        _hm = _now.hour * 100 + _now.minute
        _commission = _p("commission_rate", 0.35)  # 왕복 수수료 + 세금 (%)

        if 1450 <= _hm < 1520:
            # 수익 = 수수료 커버 후 실질 이익
            net_pnl = pnl_pct - _commission
            if net_pnl > 0:
                # 오버나잇 허용 예외: MACD 강세 + 순수익 3% 이상
                overnight_ok = macd_bullish and net_pnl >= 3.0
                if not overnight_ok:
                    logger.info(
                        f"[장마감 익절] {ticker} | 순수익 {net_pnl:+.2f}% "
                        f"(수익 {pnl_pct:+.2f}% - 수수료 {_commission:.2f}%) | "
                        f"MACD:{macd_sig} | {quantity}주 전량 청산"
                    )
                    _delete_trailing_stop(ticker)
                    from src.utils.notifier import notify
                    notify(
                        f"🔔 <b>[장마감 익절]</b> {name}({ticker})\n"
                        f"수익 {pnl_pct:+.2f}% | 14:50 마감 전 전량 청산\n"
                        f"(오버나잇 조건 미충족: MACD 또는 수익 3% 미만)"
                    )
                    return self._place_sell(
                        ticker=ticker, quantity=quantity, current_price=current_price,
                        action="take_profit", reason=f"장마감 익절 (순수익 {net_pnl:+.2f}%)",
                        avg_price=avg_price, name=name,
                    )

        elif _hm >= 1520:
            # 마감 10분 전: 손익 무관 전량 강제 청산
            action_type = "take_profit" if pnl_pct > 0 else "stop_loss"
            logger.warning(
                f"[장마감 강제청산] {ticker} | 15:20 경과 | 손익 {pnl_pct:+.2f}% | {quantity}주"
            )
            _delete_trailing_stop(ticker)
            from src.utils.notifier import notify
            notify(
                f"⏰ <b>[장마감 강제청산]</b> {name}({ticker})\n"
                f"15:20 경과 — 오버나잇 방지 전량 청산\n"
                f"손익 {pnl_pct:+.2f}%"
            )
            return self._place_sell(
                ticker=ticker, quantity=quantity, current_price=current_price,
                action=action_type, reason=f"장마감 강제청산 15:20 (손익 {pnl_pct:+.2f}%)",
                avg_price=avg_price, name=name,
            )

        # ── 5. 타임컷 ────────────────────────────
        if held_days > _MAX_HOLD_DAYS:
            logger.warning(
                f"[타임컷] {ticker} | {held_days}영업일 보유 | "
                f"손익 {pnl_pct:+.2f}% | {quantity}주 전량"
            )
            _delete_trailing_stop(ticker)
            return self._place_sell(
                ticker=ticker, quantity=quantity, current_price=current_price,
                action="time_cut", reason=f"{held_days}영업일 초과 ({_MAX_HOLD_DAYS}일 기준)",
                avg_price=avg_price, name=name,
            )

        # ── 6. 동적 익절 (MACD 연동) ─────────────
        # 익절 목표 도달 시:
        #   - MACD bullish  → 익절 보류. 대신 손절선을 매수가+1% 이상으로 강제 상향 (수익 보호)
        #   - MACD bearish/neutral → 즉시 분할 매도
        for tp_pct, tp_label, tp_cond in [
            (_TAKE_PROFIT_2_PCT, "2차", partial_sold >= 1),
            (_TAKE_PROFIT_1_PCT, "1차", partial_sold == 0),
        ]:
            if pnl_pct >= tp_pct and tp_cond:
                if macd_bullish and ts:
                    # 익절 보류 — 손절선을 매수가+1%로 상향해 수익 보호
                    lock_floor = avg_price * 1.01
                    current_floor = float(ts["trailing_floor"])
                    if lock_floor > current_floor:
                        execute(
                            "UPDATE trailing_stop SET trailing_floor = ?, updated_at = CURRENT_TIMESTAMP WHERE ticker = ?",
                            (lock_floor, ticker),
                        )
                        from src.infra.stop_order_manager import update_stop_order
                        update_stop_order(ticker, quantity, lock_floor)
                        logger.info(
                            f"[익절 보류] {ticker} | +{pnl_pct:.1f}% 달성 but MACD {macd_sig} 강세 | "
                            f"손절선 → {lock_floor:,.0f}원 (매수가+1%) 으로 상향, 홀딩 유지"
                        )
                        from src.utils.notifier import notify
                        notify(
                            f"⏫ <b>[익절 보류 — MACD 강세]</b> {ticker}\n"
                            f"수익 {pnl_pct:+.2f}% (목표 {tp_pct}% 도달) but MACD {macd_sig}\n"
                            f"손절선 {lock_floor:,.0f}원으로 상향 — 더 높은 가격 노림"
                        )
                    return None  # 이번 사이클 아무 행동 없음

                # MACD neutral/bearish → 분할 매도 실행
                sell_qty = max(1, quantity // 3)
                logger.info(f"[익절 {tp_label}] {ticker} | 손익 {pnl_pct:+.2f}% | {sell_qty}주")
                return self._place_sell(
                    ticker=ticker, quantity=sell_qty, current_price=current_price,
                    action="take_profit", reason=f"{tp_label} 익절 {pnl_pct:+.2f}% ≥ +{tp_pct:.0f}%",
                    avg_price=avg_price, name=name,
                )

        return None

    # ──────────────────────────────────────────
    # WebSocket 구독 관리
    # ──────────────────────────────────────────

    def _sync_ws_subscriptions(self, positions: list[dict]) -> None:
        """
        현재 보유 포지션 기준으로 WebSocket 구독을 동기화.
        - 새로 들어온 종목 → 구독 추가
        - 청산된 종목 → 구독 해제
        """
        try:
            from src.infra.kis_websocket import KISWebSocket
            ws = KISWebSocket()
        except Exception:
            return

        current_tickers = {pos["ticker"] for pos in positions}

        # 신규 구독
        for ticker in current_tickers - self._ws_subscribed:
            ws.subscribe(ticker, self._on_ws_price_tick)
            self._ws_subscribed.add(ticker)
            logger.debug(f"[WS] 신규 구독: {ticker}")

        # 청산된 종목 해제
        for ticker in self._ws_subscribed - current_tickers:
            ws.unsubscribe(ticker)
            self._ws_subscribed.discard(ticker)
            self._ws_triggered.discard(ticker)
            logger.debug(f"[WS] 구독 해제 (청산): {ticker}")

    def _on_ws_price_tick(self, ticker: str, current_price: float) -> None:
        """
        WebSocket 실시간 체결가 콜백.
        tick마다 호출 — 손절선 돌파 즉시 시장가 매도.

        폴링과 독립적으로 실행되므로 중복 매도 방지 필수.
        """
        ts = _load_trailing_stop(ticker)
        if not ts:
            return

        trailing_floor = float(ts["trailing_floor"])
        if current_price > trailing_floor:
            return  # 정상 범위 — 아무것도 안 함

        # 손절선 돌파 감지
        try:
            from src.infra.kis_websocket import KISWebSocket
            ws = KISWebSocket()
            if not ws.mark_selling(ticker):
                return  # 이미 다른 스레드에서 매도 처리 중
        except Exception:
            return

        self._ws_triggered.add(ticker)
        qty = self._qty_cache.get(ticker, 0)
        if qty <= 0:
            ws.clear_selling(ticker)
            self._ws_triggered.discard(ticker)
            return

        logger.warning(
            f"[WS 실시간 손절] {ticker} | 현재가 {current_price:,.0f} ≤ "
            f"손절선 {trailing_floor:,.0f} | {qty}주 즉시 청산"
        )
        _delete_trailing_stop(ticker)

        from src.utils.notifier import notify
        notify(
            f"⚡ <b>[실시간 손절 — WS]</b> {ticker}\n"
            f"현재가 {current_price:,.0f}원 ≤ 손절선 {trailing_floor:,.0f}원\n"
            f"{qty}주 즉시 시장가 매도"
        )
        self._place_sell(
            ticker=ticker,
            quantity=qty,
            current_price=current_price,
            action="stop_loss",
            reason=f"WS 실시간 트레일링 스톱 (손절선 {trailing_floor:,.0f}원)",
        )

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
        avg_price: float = 0.0,
        name: str = "",
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
                avg_price=avg_price,
                name=name,
            )

            logger.info(
                f"매도 주문 완료 [{action}] {ticker} {quantity}주 "
                f"@ {exec_price:,.0f}원 | 주문번호 {order_no}"
            )

            # 매도 완료 → WebSocket 구독 해제 + 트리거 플래그 정리
            try:
                from src.infra.kis_websocket import KISWebSocket
                ws = KISWebSocket()
                ws.unsubscribe(ticker)
                ws.clear_selling(ticker)
            except Exception:
                pass
            self._ws_subscribed.discard(ticker)
            self._ws_triggered.discard(ticker)
            self._qty_cache.pop(ticker, None)

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
            # 실패 시 selling 플래그 해제 (재시도 가능하게)
            try:
                from src.infra.kis_websocket import KISWebSocket
                KISWebSocket().clear_selling(ticker)
            except Exception:
                pass
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

        # 예수금 확인 — 1주 살 돈도 없으면 매수 금지
        required = current_price * quantity
        available_cash = _fetch_available_cash()
        if available_cash < current_price:
            logger.warning(
                f"매수 취소 [{ticker}] 예수금 부족: "
                f"필요 {required:,.0f}원 / 가용 {available_cash:,.0f}원 (1주={current_price:,.0f}원)"
            )
            from src.utils.notifier import notify
            notify(
                f"⚠️ <b>[예수금 부족]</b> {ticker} 매수 취소\n"
                f"필요: {required:,.0f}원 | 가용: {available_cash:,.0f}원\n"
                f"사유: {reason}"
            )
            return None

        # 예수금이 1주는 살 수 있지만 전체 수량은 부족한 경우 — 수량 조정
        if available_cash < required:
            adjusted_qty = max(1, int(available_cash // current_price))
            logger.info(
                f"매수 수량 조정 [{ticker}] 예수금 부족: "
                f"{quantity}주 → {adjusted_qty}주 (가용 {available_cash:,.0f}원)"
            )
            quantity = adjusted_qty

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

            # 알림은 caller(_evaluate_position)에서 이미 notify()로 발송함 — 중복 방지
            logger.info(
                f"추가매수 완료 {ticker} {quantity}주 "
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

def _fetch_available_cash() -> float:
    """KIS 잔고 API에서 주문 가능 현금(예수금) 조회."""
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
            priority=RequestPriority.TRADING,
        )
        output2 = resp.get("output2", [{}])
        cash = float((output2[0] if output2 else {}).get("ord_psbl_cash", 0) or 0)
        return cash
    except Exception as e:
        logger.warning(f"예수금 조회 실패 (매수 계속 진행): {e}")
        return float("inf")  # 조회 실패 시 차단하지 않음


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


def _fetch_positions_from_snapshot() -> list[dict]:
    """
    KIS 잔고 API 실패 시 폴백.
    DB position_snapshot 최신 레코드 + KIS 현재가 API(개별)로 포지션 구성.
    현재가 API도 실패하면 스냅샷 마지막 가격 사용.
    trailing_stop 테이블에 있는 종목만 대상 (실제 보유 포지션으로 간주).
    """
    try:
        # trailing_stop에 등록된 종목 = 현재 보유 중인 종목
        ts_rows = fetch_all("SELECT ticker FROM trailing_stop")
        if not ts_rows:
            return []

        tickers = [row["ticker"] for row in ts_rows]
        positions = []

        for ticker in tickers:
            # 최신 스냅샷 조회
            snap = fetch_one(
                "SELECT * FROM position_snapshot WHERE ticker = ? ORDER BY snapshot_at DESC LIMIT 1",
                (ticker,),
            )
            if not snap:
                continue

            quantity = snap["quantity"]
            if quantity <= 0:
                continue

            avg_price = float(snap["avg_price"])

            # KIS 현재가 개별 조회 시도
            current_price = _fetch_current_price_safe(ticker, fallback=float(snap["current_price"]))
            pnl_pct = (current_price / avg_price - 1) * 100 if avg_price > 0 else 0.0

            snap_dict = dict(snap)
            positions.append({
                "ticker": ticker,
                "name": snap_dict.get("name") or ticker,
                "quantity": quantity,
                "avg_price": avg_price,
                "current_price": current_price,
                "pnl_pct": pnl_pct,
                "held_days": _calc_held_days(ticker),
                "partial_sold": _count_partial_sells(ticker),
            })

        return positions

    except Exception as e:
        logger.warning(f"DB 스냅샷 폴백 실패: {e}")
        return []


def _fetch_current_price_safe(ticker: str, fallback: float) -> float:
    """KIS 현재가 조회. 실패 시 fallback 가격 반환."""
    try:
        gw = KISGateway()
        resp = gw.request(
            method="GET",
            path="/uapi/domestic-stock/v1/quotations/inquire-price",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            tr_id="FHKST01010100",
            priority=RequestPriority.POSITION_MONITOR,
        )
        price = float(resp.get("output", {}).get("stck_prpr", 0) or 0)
        return price if price > 0 else fallback
    except Exception:
        return fallback


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
    ticker: str, ts: dict, current_price: float, avg_price: float, quantity: int,
    tight: bool = False,
) -> float:
    """
    트레일링 손절선 업데이트.
    수익이 TRAILING_TRIGGER% 이상이면 손절선을 현재가 기준 TRAILING_FLOOR% 아래로 상향.
    tight=True (MACD bearish) 이면 간격을 절반으로 줄여 더 타이트하게 추적.
    손절선은 절대 내려가지 않음.
    손절선이 실제로 올라간 경우 거래소 사전 손절 주문도 취소 후 재제출.
    """
    current_floor = float(ts["trailing_floor"])
    highest = float(ts["highest_price"])

    new_highest = max(highest, current_price)

    gain_pct = (current_price / avg_price - 1) * 100
    floor_gap = _TRAILING_TIGHT_FLOOR if tight else _TRAILING_FLOOR

    if gain_pct >= _TRAILING_TRIGGER:
        candidate_floor = current_price * (1 - floor_gap / 100)
        new_floor = max(current_floor, candidate_floor)
    elif tight and gain_pct > 0:
        # MACD 약화 + 수익권: 수익 전액 기준으로도 타이트하게
        candidate_floor = current_price * (1 - floor_gap / 100)
        new_floor = max(current_floor, candidate_floor)
    else:
        new_floor = current_floor

    execute(
        """
        UPDATE trailing_stop
        SET trailing_floor = ?, highest_price = ?, updated_at = CURRENT_TIMESTAMP
        WHERE ticker = ?
        """,
        (new_floor, new_highest, ticker),
    )

    if new_floor > current_floor:
        tight_label = " [MACD타이트]" if tight else ""
        logger.info(
            f"[트레일링{tight_label}] {ticker} 손절선 상향: {current_floor:,.0f} → {new_floor:,.0f}원 "
            f"(현재가 {current_price:,.0f}, 수익 {gain_pct:+.1f}%)"
        )
        from src.infra.stop_order_manager import update_stop_order
        update_stop_order(ticker, quantity, new_floor)

    return new_floor


def _increment_scale_in(ticker: str) -> None:
    """피라미딩 실행 횟수 증가."""
    try:
        execute(
            "UPDATE trailing_stop SET scale_in_count = scale_in_count + 1, updated_at = CURRENT_TIMESTAMP WHERE ticker = ?",
            (ticker,),
        )
    except Exception:
        pass


def _check_hotlist_for_dip(ticker: str) -> dict | None:
    """
    스마트 물타기 조건 확인: Hot List에 최근 15분 이내 등재 + 거래량 비율 충족.
    조건 충족 시 해당 hot_list 레코드 반환, 아니면 None.
    """
    try:
        row = fetch_one(
            """
            SELECT ticker, name, volume_ratio, reason FROM hot_list
            WHERE ticker = ?
              AND created_at >= datetime('now', '-15 minutes')
              AND (volume_ratio IS NULL OR volume_ratio >= ?)
            ORDER BY created_at DESC LIMIT 1
            """,
            (ticker, _DIP_BUY_VOL_MIN),
        )
        return dict(row) if row else None
    except Exception:
        return None


def _increment_dip_buy(ticker: str) -> None:
    """스마트 물타기 실행 횟수 증가."""
    try:
        execute(
            "UPDATE trailing_stop SET dip_buy_count = dip_buy_count + 1, updated_at = CURRENT_TIMESTAMP WHERE ticker = ?",
            (ticker,),
        )
    except Exception:
        pass


def _check_hotlist_for_fire(ticker: str) -> dict | None:
    """
    불타기 조건 확인: Hot List에 최근 30분 이내 등재 (거래량은 호출 측에서 별도 확인).
    등재 시 해당 hot_list 레코드 반환, 아니면 None.
    """
    try:
        row = fetch_one(
            """
            SELECT ticker, name, volume_ratio, signal_type, reason FROM hot_list
            WHERE ticker = ?
              AND created_at >= datetime('now', '-30 minutes')
            ORDER BY created_at DESC LIMIT 1
            """,
            (ticker,),
        )
        return dict(row) if row else None
    except Exception:
        return None


def _update_entry_price(ticker: str, new_avg: float) -> None:
    """피라미딩 후 평균 매수가 갱신."""
    try:
        execute(
            "UPDATE trailing_stop SET entry_price = ?, updated_at = CURRENT_TIMESTAMP WHERE ticker = ?",
            (new_avg, ticker),
        )
    except Exception:
        pass


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
    avg_price: float = 0.0,
    name: str = "",
) -> None:
    """trades 테이블에 매매 이력 기록 (pnl 포함)."""
    pnl_pct = ((exec_price / avg_price) - 1) * 100 if avg_price > 0 else None
    pnl_amt = (exec_price - avg_price) * quantity if avg_price > 0 else None
    execute(
        """
        INSERT INTO trades
            (date, ticker, name, action, order_type, exec_price,
             quantity, status, pnl, pnl_pct, signal_source, strategy_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(date.today()),
            ticker,
            name,
            action,
            "market",
            exec_price,
            quantity,
            "filled",
            pnl_amt,
            pnl_pct,
            signal_source,
            reason,
        ),
    )
