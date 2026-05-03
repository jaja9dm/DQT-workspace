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

_INTERVAL_SEC = 60          # 1분 주기

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
        # WebSocket 구독 해제 후 연결 종료
        try:
            from src.infra.kis_websocket import KISWebSocket
            ws = KISWebSocket()
            for ticker in list(self._ws_subscribed):
                ws.unsubscribe(ticker)
            ws.stop()
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
        # None = API 오류, [] = 정상이지만 보유 없음
        kis_positions_raw = _fetch_positions()
        kis_api_ok = kis_positions_raw is not None
        positions = kis_positions_raw if kis_api_ok else []
        if not positions:
            snapshot_positions = _fetch_positions_from_snapshot()
            if snapshot_positions:
                positions = snapshot_positions
                logger.warning(
                    f"KIS 잔고 API 실패 → DB 스냅샷 폴백 ({len(positions)}종목) — 가격 stale 가능"
                )
        if not positions:
            # KIS API 정상 응답인데 잔고 0 → trailing_stop에 남은 좀비 레코드 정리
            if kis_api_ok:
                _reconcile_zombie_trailing_stops(self)
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

        # 5.5. 섹터 업그레이드 교체 — 동일 섹터에 더 강한 Hot List 종목이 있으면 약한 포지션 매도
        try:
            from src.infra.sector_rotation import get_sector
            from src.infra.database import fetch_all
            hl_rows = fetch_all(
                "SELECT ticker, name, signal_type FROM hot_list "
                "WHERE created_at >= datetime('now','-15 minutes') "
                "ORDER BY created_at DESC",
            )
            hl_tickers = {r["ticker"] for r in hl_rows}
            for pos in list(positions):
                pos_ticker = pos["ticker"]
                if pos_ticker in hl_tickers:
                    continue  # 현재 포지션이 Hot List에 있으면 유지
                pos_pnl = pos.get("pnl_pct", 0.0)
                if pos_pnl >= 1.5:
                    continue  # 수익 1.5% 이상이면 교체 안 함
                pos_sector = get_sector(pos_ticker)
                if not pos_sector or pos_sector == "기타":
                    continue
                # 같은 섹터에 Hot List 종목이 있으면 교체
                for hl in hl_rows:
                    if get_sector(hl["ticker"]) == pos_sector and hl["ticker"] != pos_ticker:
                        logger.info(
                            f"[섹터 업그레이드] {pos_ticker}({pos_sector}) → "
                            f"{hl['ticker']} 교체 | 현재손익 {pos_pnl:+.2f}%"
                        )
                        from src.utils.notifier import notify
                        notify(
                            f"🔄 <b>[섹터 교체]</b> {pos.get('name', pos_ticker)}({pos_ticker})\n"
                            f"섹터 [{pos_sector}] 내 더 강한 종목 {hl.get('name', hl['ticker'])}({hl['ticker']}) 감지\n"
                            f"현재 손익 {pos_pnl:+.2f}% → 청산 후 교체"
                        )
                        self._place_sell(
                            ticker=pos_ticker,
                            quantity=pos["quantity"],
                            current_price=pos["current_price"],
                            action="time_cut",
                            reason=f"섹터 업그레이드 교체 ({pos_sector} → {hl['ticker']})",
                            avg_price=pos["avg_price"],
                            name=pos.get("name", pos_ticker),
                        )
                        positions = [p for p in positions if p["ticker"] != pos_ticker]
                        break
        except Exception as _se:
            logger.debug(f"섹터 업그레이드 체크 실패: {_se}")

        # 5.6. 슬롯 건강 교체 — domestic_stock 엔진이 replace_requested 플래그를 세운 슬롯 처리
        try:
            from datetime import date as _date
            from src.infra.database import fetch_all as _fa
            _today = _date.today().isoformat()
            _replace_rows = _fa(
                """
                SELECT slot, ticker, replace_reason, health_score
                FROM slot_assignments
                WHERE trade_date = ? AND status = 'active' AND replace_requested = 1
                """,
                (_today,),
            )
            _pos_map = {p["ticker"]: p for p in positions}
            for _rr in _replace_rows:
                _slot_ticker = _rr["ticker"]
                _pos = _pos_map.get(_slot_ticker)
                if not _pos:
                    # 포지션 없음(아직 미매수) → 플래그만 해제, 슬롯 재탐색 허용
                    from src.teams.domestic_stock.engine import release_slot
                    release_slot(_rr["slot"])
                    logger.info(
                        f"[슬롯 건강 재배정] {_rr['slot']}/{_slot_ticker} — "
                        f"미매수 상태에서 건강 악화, 슬롯 해제 | {_rr['replace_reason']}"
                    )
                    continue

                _pnl = _pos.get("pnl_pct", 0.0)
                _score = _rr["health_score"] or 0.0

                # 수익이 충분하면 트레일링 스톱에 맡기고 교체 보류
                from src.teams.domestic_stock.engine import _SLOT_HEALTH_SAFE_PNL
                if _pnl >= _SLOT_HEALTH_SAFE_PNL:
                    # 플래그 해제 (다음 평가 때 다시 체크)
                    from src.infra.database import execute as _ex
                    _ex(
                        "UPDATE slot_assignments SET replace_requested = 0 WHERE slot = ?",
                        (_rr["slot"],),
                    )
                    logger.debug(
                        f"슬롯 교체 보류 [{_rr['slot']}/{_slot_ticker}] — "
                        f"수익 {_pnl:+.1f}% ≥ {_SLOT_HEALTH_SAFE_PNL}% (트레일링 스톱에 위임)"
                    )
                    continue

                logger.warning(
                    f"[슬롯 건강 교체] {_rr['slot']} / {_slot_ticker} | "
                    f"건강점수 {_score:.0f}점 | 손익 {_pnl:+.2f}% | {_rr['replace_reason']}"
                )
                from src.utils.notifier import notify
                notify(
                    f"🔁 <b>[슬롯 교체]</b> {_pos.get('name', _slot_ticker)}({_slot_ticker})"
                    f" [{_rr['slot']}슬롯]\n"
                    f"건강점수 {_score:.0f}점 | 손익 {_pnl:+.2f}%\n"
                    f"사유: {_rr['replace_reason']}"
                )
                _sell_result = self._place_sell(
                    ticker=_slot_ticker,
                    quantity=_pos["quantity"],
                    current_price=_pos["current_price"],
                    action="time_cut",
                    reason=f"슬롯 건강 교체 ({_rr['replace_reason']})",
                    avg_price=_pos["avg_price"],
                    name=_pos.get("name", _slot_ticker),
                )
                if _sell_result:
                    positions = [p for p in positions if p["ticker"] != _slot_ticker]
                    # _place_sell 내부에서 release_slot() 이미 호출됨
        except Exception as _she:
            logger.debug(f"슬롯 건강 교체 체크 실패: {_she}")

        # 6. 초과 포지션 정리 (보유 수 > max_positions)
        from src.teams.research.param_tuner import get_param
        max_pos = int(get_param("max_positions", 3.0))
        if len(positions) > max_pos:
            excess = len(positions) - max_pos

            # 제거 우선순위 스코어 계산 (높을수록 먼저 청산)
            # 1순위: MACD 역행(bearish) — 추세 반전 확인 종목
            # 2순위: 손익 마이너스
            # 3순위: Hot List 미포함 (최근 30분 이내 등재 없음)
            # 4순위: 보유 기간 긴 종목
            def _evict_score(p: dict) -> tuple:
                ticker = p["ticker"]
                macd_d = get_macd_details(ticker, max_age_minutes=10)
                is_bearish = macd_d["signal"] in _MACD_BEARISH
                is_losing  = p["pnl_pct"] < 0
                in_hotlist = fetch_one(
                    "SELECT 1 FROM hot_list WHERE ticker=? AND created_at >= datetime('now','-30 minutes')",
                    (ticker,),
                ) is not None
                held = p.get("held_days", 0)
                # 튜플 비교: 앞 요소가 클수록 먼저 청산
                return (
                    int(is_bearish),   # MACD 역행이면 1 (최우선)
                    int(is_losing),    # 손실 중이면 1
                    int(not in_hotlist),  # Hot List 없으면 1
                    held,              # 보유 기간 (길수록 먼저)
                    -p["pnl_pct"],     # 손익률 낮을수록 먼저 (부호 반전)
                )

            sorted_pos = sorted(positions, key=_evict_score, reverse=True)

            for pos in sorted_pos[:excess]:
                ticker = pos["ticker"]
                macd_d = get_macd_details(ticker, max_age_minutes=10)
                evict_reasons = []
                if macd_d["signal"] in _MACD_BEARISH:
                    evict_reasons.append(f"MACD {macd_d['signal']}")
                if pos["pnl_pct"] < 0:
                    evict_reasons.append(f"손실 {pos['pnl_pct']:+.2f}%")
                evict_reasons.append(f"보유 {pos.get('held_days',0)}일")
                reason_str = " | ".join(evict_reasons)

                logger.info(
                    f"[초과 포지션 정리] {ticker} | {reason_str} | "
                    f"보유 {len(positions)}종목 > 최대 {max_pos}종목"
                )
                from src.utils.notifier import notify
                notify(
                    f"📉 <b>[초과 포지션 정리]</b> {pos.get('name', ticker)}({ticker})\n"
                    f"보유 {len(positions)}종목 → 최대 {max_pos}종목 초과\n"
                    f"{reason_str} | {pos['quantity']}주 전량 청산"
                )
                result = self._place_sell(
                    ticker=ticker,
                    quantity=pos["quantity"],
                    current_price=pos["current_price"],
                    action="time_cut",
                    reason=f"초과 포지션 정리 ({reason_str})",
                    avg_price=pos["avg_price"],
                    name=pos.get("name", ticker),
                )
                if result:
                    positions = [p for p in positions if p["ticker"] != ticker]

        # 7. 개별 종목 감시 (WebSocket이 이미 트리거한 종목은 스킵)
        # MACD 신호를 포지션 전체 ticker에 대해 한 번에 프리로드 (N쿼리 → 1쿼리)
        from src.teams.intraday_macd.engine import preload_macd_cache
        all_tickers = [p["ticker"] for p in positions]
        macd_cache = preload_macd_cache(all_tickers, max_age_minutes=20)

        actions = []
        for pos in positions:
            if pos["ticker"] in self._ws_triggered:
                logger.debug(f"[WS 트리거됨] {pos['ticker']} — 폴링 스킵")
                continue
            action = self._evaluate_position(pos, stop_loss_pct, level, macd_cache=macd_cache)
            if action:
                actions.append(action)

        return actions

    # ──────────────────────────────────────────
    # 포지션 평가
    # ──────────────────────────────────────────

    def _evaluate_position(
        self, pos: dict, stop_loss_pct: float, risk_level: int, macd_cache: dict | None = None
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
        from src.teams.intraday_macd.engine import macd_details_from_cache, get_macd_details
        if macd_cache is not None:
            macd_details = macd_details_from_cache(macd_cache, ticker, max_age_minutes=6)
        else:
            macd_details = get_macd_details(ticker, max_age_minutes=6)
        macd_sig      = macd_details["signal"]
        hist_3m_now   = macd_details["hist_3m"] or 0.0
        macd_bullish  = macd_sig in _MACD_BULLISH
        macd_bearish  = macd_sig in _MACD_BEARISH
        # sell_prep: 5분봉 히스토그램 양수 고점 꺾임 — sell_pre 1~2봉 선행하는 조기 경고
        # (bearish는 아니므로 조기손절 트리거 안 함 — trailing 타이트 + 소진도 가속만)
        macd_sell_prep = macd_sig == "sell_prep"
        from_negative  = macd_details.get("from_negative", False)

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
            # tight 조건: bearish OR sell_prep(5분봉 피크 꺾임) OR 14:00 이후
            _now_hm = datetime.now().hour * 100 + datetime.now().minute
            tight = macd_bearish or macd_sell_prep or (_now_hm >= 1400 and pnl_pct > 0)
            if macd_sell_prep and pnl_pct > 0:
                logger.debug(f"[sell_prep 타이트] {ticker} 5분봉 MACD 고점 꺾임 — 손절선 간격 절반 적용")
            elif _now_hm >= 1400 and pnl_pct > 0 and not macd_bearish:
                logger.debug(f"[14:00 이후 타이트] {ticker} 수익 {pnl_pct:+.2f}% — 트레일링 간격 절반 적용")
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
            # dip_max: ATR·거래량 압력 기반 동적 트리거 (-N% 이상 하락 시 발동)
            # 단, 당일 손절 1회 이상 발생 시 물타기 금지 (추가 리스크 차단)
            _today_sl = fetch_one(
                "SELECT COUNT(*) AS cnt FROM trades WHERE date=? AND action='stop_loss'",
                (str(date.today()),),
            )
            _today_sl_cnt = int(_today_sl["cnt"]) if _today_sl else 0

            dip_min  = _p("dip_buy_min_loss",  _DIP_BUY_MIN_LOSS)
            dip_max_dynamic = -_get_dynamic_ladder_pct(ticker, ts)
            dip_max  = max(dip_max_dynamic, _p("dip_buy_max_loss", _DIP_BUY_MAX_LOSS))
            dip_max_count = int(_p("dip_buy_max_count", _DIP_BUY_MAX))
            dip_buy_count = int(ts.get("dip_buy_count", 0))
            if (
                dip_min >= pnl_pct >= dip_max
                and not macd_bearish
                and dip_buy_count < dip_max_count
                and risk_level < 4
                and _today_sl_cnt == 0   # 당일 손절 이력 없을 때만 물타기 허용
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

                # MACD 히스토그램 방향 + 연속 sell_pre 누적 (핵심 모멘텀 소진 판단)
                from src.teams.intraday_macd.engine import consec_sell_from_cache, get_consecutive_sell_pre
                if macd_cache is not None:
                    consec_sell = consec_sell_from_cache(macd_cache, ticker)
                else:
                    consec_sell = get_consecutive_sell_pre(ticker, max_age_minutes=20)

                if macd_bearish:
                    # sell_pre: 3분봉+5분봉 모두 히스토그램 하강 (AND 조건) → 비중 축소
                    exhaustion += 0.45
                    exh_signals.append(f"MACD역행({macd_sig})")
                elif macd_sell_prep:
                    # sell_prep: 5분봉 히스토그램 양수 고점 꺾임 — sell_pre보다 1~2봉 선행
                    # 사용자 패턴: 5분봉 MACD 피크에서 청산 준비 → 즉시 손절선 타이트 + 소진도 상승
                    exhaustion += 0.38
                    exh_signals.append("5분봉MACD피크꺾임")
                elif hist_3m_now < 0 and hist_3m_prev >= 0:
                    # 히스토그램이 양수→음수 전환 (모멘텀 반전 확인)
                    exhaustion += 0.40
                    exh_signals.append("MACD음전환")
                elif hist_3m_now > 0 and hist_3m_now < hist_3m_prev:
                    # 양수 구간에서 감소 (피크 지남 = 모멘텀 약화 시작)
                    exhaustion += 0.25
                    exh_signals.append("MACD피크감소")

                # 연속 sell_pre 누적: "파란 바가 하나둘 생긴다" = 수급 이탈 중
                # 사이클 1회(3분)마다 sell_pre가 반복되면 소진도 추가 가산
                if consec_sell >= 2:
                    bonus = min(0.30, 0.12 * (consec_sell - 1))  # 최대 +0.30
                    exhaustion += bonus
                    exh_signals.append(f"연속sell_pre {consec_sell}회(+{bonus:.2f})")

                # 거래량 하락세: 최근 3봉 평균 < 이전 3봉 평균 → 모멘텀 소진 신호
                try:
                    from src.infra.database import fetch_all as _fa
                    _vol_rows = _fa(
                        "SELECT volume FROM intraday_candles WHERE ticker = ? "
                        "ORDER BY bar_time DESC LIMIT 6",
                        (ticker,),
                    )
                    if len(_vol_rows) >= 6:
                        _recent_vol = sum(int(r["volume"]) for r in _vol_rows[:3]) / 3
                        _earlier_vol = sum(int(r["volume"]) for r in _vol_rows[3:6]) / 3
                        if _earlier_vol > 0 and _recent_vol < _earlier_vol * 0.70:
                            exhaustion += 0.15
                            exh_signals.append(f"거래량감소({_recent_vol/_earlier_vol:.2f}x)")
                except Exception:
                    pass

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

        # ── 4.5. 장마감 자동 청산 ─────────────────────────────────
        # 14:50 ~ 수익권: 손익 실현 (수수료 커버 후 이익 구간)
        # 15:20 이후 : 손익 무관 전량 강제 청산
        _now = datetime.now()
        _hm = _now.hour * 100 + _now.minute
        _commission = _p("commission_rate", 0.35)

        if 1450 <= _hm < 1520:
            net_pnl = pnl_pct - _commission
            if net_pnl > 0:
                logger.info(
                    f"[장마감 익절] {ticker} | 순수익 {net_pnl:+.2f}% | {quantity}주 전량 청산"
                )
                _delete_trailing_stop(ticker)
                from src.utils.notifier import notify
                notify(
                    f"🔔 <b>[장마감 익절]</b> {name}({ticker})\n"
                    f"수익 {pnl_pct:+.2f}% | 14:50 마감 전 전량 청산"
                )
                return self._place_sell(
                    ticker=ticker, quantity=quantity, current_price=current_price,
                    action="take_profit", reason=f"장마감 익절 (순수익 {net_pnl:+.2f}%)",
                    avg_price=avg_price, name=name,
                )

        elif _hm >= 1520:
            action_type = "take_profit" if pnl_pct > 0 else "stop_loss"
            logger.warning(
                f"[장마감 강제청산] {ticker} | 15:20 경과 | 손익 {pnl_pct:+.2f}% | {quantity}주"
            )
            _delete_trailing_stop(ticker)
            from src.utils.notifier import notify
            notify(
                f"⏰ <b>[장마감 강제청산]</b> {name}({ticker})\n"
                f"15:20 경과 — 전량 청산 | 손익 {pnl_pct:+.2f}%"
            )
            return self._place_sell(
                ticker=ticker, quantity=quantity, current_price=current_price,
                action=action_type, reason=f"장마감 강제청산 15:20 (손익 {pnl_pct:+.2f}%)",
                avg_price=avg_price, name=name,
            )

        # ── 5-A. 12:00 이후 3분봉 60일선 이탈 청산 ──────────────
        # 12:00 이후에는 신규 매수 없음 → 기존 포지션 수익 지키기 모드.
        # 3분봉 종가(≈현재가)가 60일 이동평균 아래로 내려오면 지지 붕괴 판단 → 도망.
        # 수익 중인 포지션에만 적용 (손절선은 별도 존재하므로 손실 포지션은 제외).
        _now_hm_ma = datetime.now().hour * 100 + datetime.now().minute
        if _now_hm_ma >= 1200 and pnl_pct > 0:
            ma60 = _fetch_ma60(ticker)
            if ma60 is not None and ma60 > 0 and current_price < ma60:
                logger.warning(
                    f"[60일선 이탈] {ticker} | 현재가 {current_price:,.0f} < MA60 {ma60:,.0f} "
                    f"| 12:00↑ 지지 붕괴 → 수익 {pnl_pct:+.2f}% 확정 청산"
                )
                _delete_trailing_stop(ticker)
                from src.utils.notifier import notify
                notify(
                    f"📉 <b>[60일선 이탈 청산]</b> {name}({ticker})\n"
                    f"현재가 {current_price:,.0f}원 < 60일선 {ma60:,.0f}원\n"
                    f"12:00 이후 지지 붕괴 → 수익 {pnl_pct:+.2f}% 확정"
                )
                return self._place_sell(
                    ticker=ticker, quantity=quantity, current_price=current_price,
                    action="take_profit",
                    reason=f"12:00↑ 60일선 이탈 ({current_price:,.0f} < MA60 {ma60:,.0f})",
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
        # 13:30 이후: 익절 목표를 50%로 낮춤 (장마감 전 수익 확정 우선)
        # 예: +5% 목표 → 13:30 이후 +2.5%부터 익절 시작
        _now_hm2 = datetime.now().hour * 100 + datetime.now().minute
        _closing_phase = _now_hm2 >= 1330  # 13:30 이후 = 수익 확정 우선 구간
        _tp1 = _TAKE_PROFIT_1_PCT * (0.5 if _closing_phase else 1.0)
        _tp2 = _TAKE_PROFIT_2_PCT * (0.5 if _closing_phase else 1.0)
        if _closing_phase and pnl_pct > 0:
            logger.debug(
                f"[수익 확정 구간] {ticker} 13:30↑ — 익절 목표 {_TAKE_PROFIT_1_PCT}%→{_tp1:.1f}% 적용"
            )

        # 익절 목표 도달 시:
        #   - MACD bullish  → 익절 보류. 대신 손절선을 매수가+1% 이상으로 강제 상향 (수익 보호)
        #   - MACD bearish/neutral → 즉시 분할 매도
        for tp_pct, tp_label, tp_cond in [
            (_tp2, "2차", partial_sold >= 1),
            (_tp1, "1차", partial_sold == 0),
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
            avg_price=float(ts.get("entry_price") or 0),
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
                avg_price=pos.get("avg_price", 0.0),
                name=pos.get("name", ""),
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
        from src.infra.stop_order_manager import cancel_stop_order, get_stop_order_price
        cancel_result = cancel_stop_order(ticker)

        # stop 주문이 이미 KIS에서 자동 체결된 경우 — 포지션은 이미 없음
        if cancel_result == "filled":
            stop_price = get_stop_order_price(ticker) or current_price
            logger.warning(
                f"[Stop Order 자동체결] {ticker} | 손절가 {stop_price:,.0f}원으로 "
                f"KIS 자동 체결 확인 — 매도 기록 후 포지션 청산 처리"
            )
            _record_trade(
                ticker=ticker,
                action="stop_loss",
                quantity=quantity,
                exec_price=stop_price,
                signal_source="stop_order_auto",
                reason="KIS 사전 손절 주문 자동 체결",
                avg_price=avg_price,
                name=name,
            )
            self._cleanup_after_sell(ticker)
            return {
                "ticker": ticker,
                "action": "stop_loss",
                "quantity": quantity,
                "exec_price": stop_price,
                "order_no": "",
                "reason": "KIS 사전 손절 주문 자동 체결",
            }

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

            self._cleanup_after_sell(ticker)
            return {
                "ticker": ticker,
                "action": action,
                "quantity": quantity,
                "exec_price": exec_price,
                "order_no": order_no,
                "reason": reason,
            }

        except Exception as e:
            err_str = str(e)
            # "잔고내역이 없습니다" = KIS 자동 손절이 이미 체결돼 포지션이 없는 상태
            if "잔고내역이 없습니다" in err_str or "잔고내역" in err_str:
                from src.infra.stop_order_manager import get_stop_order_price
                stop_price = get_stop_order_price(ticker) or current_price
                logger.warning(
                    f"[포지션 이미 청산] {ticker} | KIS 잔고 없음 → "
                    f"손절가 {stop_price:,.0f}원으로 매도 기록 후 청산 처리"
                )
                _record_trade(
                    ticker=ticker,
                    action="stop_loss",
                    quantity=quantity,
                    exec_price=stop_price,
                    signal_source="stop_order_auto",
                    reason="KIS 잔고없음 — 사전 손절 주문 자동 체결 추정",
                    avg_price=avg_price,
                    name=name,
                )
                self._cleanup_after_sell(ticker)
                return {
                    "ticker": ticker,
                    "action": "stop_loss",
                    "quantity": quantity,
                    "exec_price": stop_price,
                    "order_no": "",
                    "reason": "KIS 잔고없음 — 사전 손절 주문 자동 체결 추정",
                }
            logger.error(f"매도 주문 실패 [{ticker}]: {e}")
            # 실패 시 selling 플래그 해제 (재시도 가능하게)
            try:
                from src.infra.kis_websocket import KISWebSocket
                KISWebSocket().clear_selling(ticker)
            except Exception:
                pass
            return None

    def _cleanup_after_sell(self, ticker: str) -> None:
        """매도 완료(또는 이미 청산 확인) 후 내부 상태 정리."""
        # trailing_stop 삭제 (이미 삭제됐어도 무해)
        _delete_trailing_stop(ticker)

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

        # 슬롯 해제 — 포지션 청산 시 해당 슬롯을 재탐색 가능 상태로
        try:
            from src.teams.domestic_stock.engine import release_slot, get_slot_for_ticker
            _slot = get_slot_for_ticker(ticker)
            if _slot:
                release_slot(_slot)
        except Exception:
            pass


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


def _fetch_positions() -> list[dict] | None:
    """
    KIS API에서 보유 포지션 목록 조회.
    보유 수량 0인 종목 제외.

    Returns:
        list[dict] → 정상 조회 (빈 리스트 포함)
        None       → API 오류 (KIS 서버 500 등)
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
        return None  # API 오류 — 정상 0건과 구별


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
    """현재 포지션을 position_snapshot 테이블에 저장.
    ticker당 최신 1행 유지, 청산된 종목(현재 positions에 없는 ticker)은 삭제.
    """
    now = datetime.now().isoformat(timespec="seconds")
    current_tickers = {pos["ticker"] for pos in positions}

    # 청산된 포지션(현재 보유 목록에 없는 ticker) 스냅샷 삭제
    if current_tickers:
        placeholders = ",".join("?" * len(current_tickers))
        execute(
            f"DELETE FROM position_snapshot WHERE ticker NOT IN ({placeholders})",
            tuple(current_tickers),
        )
    else:
        execute("DELETE FROM position_snapshot")

    for pos in positions:
        execute("DELETE FROM position_snapshot WHERE ticker = ?", (pos["ticker"],))
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


def _fetch_ma60(ticker: str) -> float | None:
    """
    FDR 일봉 캐시에서 60일 이동평균 조회.
    캐시 미존재 시 FDR 직접 호출 (당일 1회만 발생).

    Returns:
        ma60 가격 또는 None (데이터 부족)
    """
    from datetime import timedelta
    import FinanceDataReader as fdr
    import pandas as pd

    try:
        today_str = datetime.now().strftime("%Y%m%d")
        # collector의 FDR 캐시 재사용 시도
        try:
            from src.teams.domestic_stock.collector import _fdr_cache, _fdr_cache_lock
            with _fdr_cache_lock:
                cached = _fdr_cache.get(ticker)
            if cached and cached[0] == today_str:
                df = cached[1]
                if len(df) >= 60:
                    return float(df["Close"].astype(float).rolling(60).mean().iloc[-1])
        except Exception:
            pass

        # 캐시 없으면 직접 조회 (최소 90일)
        end   = datetime.now().date()
        start = end - timedelta(days=130)
        df = fdr.DataReader(ticker, start, end)
        if df is None or len(df) < 60:
            return None
        return float(df["Close"].astype(float).rolling(60).mean().iloc[-1])
    except Exception:
        return None


def _calc_atr(ticker: str, n: int = 14) -> float | None:
    """
    intraday_candles에서 ATR(n) 계산 (True Range 평균).
    데이터 부족(< n+1봉)이면 None 반환.
    """
    rows = fetch_all(
        """
        SELECT high, low, close FROM intraday_candles
        WHERE ticker = ?
        ORDER BY bar_time DESC
        LIMIT ?
        """,
        (ticker, n + 1),
    )
    if len(rows) < n + 1:
        return None

    true_ranges = []
    for i in range(len(rows) - 1):
        curr = rows[i]
        prev = rows[i + 1]
        tr = max(
            float(curr["high"]) - float(curr["low"]),
            abs(float(curr["high"]) - float(prev["close"])),
            abs(float(curr["low"])  - float(prev["close"])),
        )
        true_ranges.append(tr)

    atr = sum(true_ranges) / len(true_ranges)
    ref_price = float(rows[0]["close"])
    return (atr / ref_price * 100) if ref_price > 0 else None


def _calc_volume_pressure(ticker: str) -> str:
    """
    최근 분봉 거래량 추세로 매수·매도 압력 판단.

    Returns:
        "bearish" — 최근 5봉 거래량이 이전 10봉 평균 대비 1.5배↑ + 가격 하락
        "bullish" — 최근 5봉 거래량이 이전 10봉 평균 대비 1.5배↑ + 가격 상승
        "neutral" — 그 외
    """
    rows = fetch_all(
        """
        SELECT close, volume FROM intraday_candles
        WHERE ticker = ?
        ORDER BY bar_time DESC
        LIMIT 15
        """,
        (ticker,),
    )
    if len(rows) < 15:
        return "neutral"

    recent  = rows[:5]
    base    = rows[5:15]

    avg_recent = sum(int(r["volume"]) for r in recent) / len(recent)
    avg_base   = sum(int(r["volume"]) for r in base)   / len(base)

    if avg_base == 0 or avg_recent < avg_base * 1.5:
        return "neutral"

    price_now   = float(recent[0]["close"])
    price_start = float(recent[-1]["close"])
    if price_now < price_start:
        return "bearish"   # 거래량 급증 + 가격 하락 = 매도 압력
    return "bullish"       # 거래량 급증 + 가격 상승 = 매수 압력


def _get_dynamic_floor_pct(ticker: str, ts: dict, macd_tight: bool,
                            gain_pct: float = 0.0) -> float:
    """
    ATR·거래량 압력·MACD·수익률 기반 동적 trailing floor 간격 결정.

    [포지션-1] ATR 연동: 실제 변동성에 비례한 최소 간격
    [포지션-2] 2단계 트레일링:
      - gain < 8%:  초기 보수적 (ATR × 1.2 — noise 방지)
      - 8~15%:      ATR × 1.5  (추세 따라가기, 더 넓은 여유)
      - gain >= 15%: ATR × 2.0 (큰 추세 극대화, 더 여유 있게)
    """
    stored_floor = float(ts.get("floor_pct") or _TRAILING_FLOOR)
    atr_pct = _calc_atr(ticker)

    if atr_pct is not None:
        # [포지션-2] 수익 구간별 ATR 배율 조정
        if gain_pct >= 15.0:
            atr_mult = 2.0   # 대추세: 넓은 간격으로 끝까지 보유
        elif gain_pct >= 8.0:
            atr_mult = 1.5   # 중간 추세: 여유 있게 추적
        else:
            atr_mult = 1.2   # 초기 구간: 보수적 (noise 손절 방지)
        atr_floor = max(1.5, min(5.0, atr_pct * atr_mult))
    else:
        atr_floor = stored_floor

    vol_pressure = _calc_volume_pressure(ticker)

    if macd_tight:
        floor = atr_floor / 2    # MACD bearish: 절반 간격으로 바짝 추적
    elif vol_pressure == "bearish":
        floor = atr_floor * 0.7  # 매도 압력: 손절선 더 빠르게 올리기
    else:
        floor = atr_floor

    # 진입 시 설정한 floor_pct보다 최소 절반은 유지 (너무 타이트해서 noise 손절 방지)
    return round(max(stored_floor * 0.4, floor), 2)


def _get_dynamic_ladder_pct(ticker: str, ts: dict) -> float:
    """
    ATR·거래량 압력 기반 사다리 매수 트리거 % 동적 결정.

    - ATR이 클수록: 더 큰 하락에서만 추가 진입 (노이즈 구분)
    - 거래량 매도 압력 감지 시: 사다리 매수 스킵 신호 반환 (999%)
    - 기본: settings.LADDER_TRIGGER_PCT
    """
    vol_pressure = _calc_volume_pressure(ticker)
    if vol_pressure == "bearish":
        return 999.0  # 매도 압력 급등 시 사다리 매수 전면 차단

    atr_pct = _calc_atr(ticker)
    base = float(_p("ladder_trigger_pct", _LADDER_TRIGGER))
    if atr_pct is not None:
        # ATR × 3~4배 지점에서 물타기 (의미 있는 하락인지 구분)
        dynamic = max(5.0, min(15.0, atr_pct * 3.5))
        return round(min(base, dynamic), 1)  # 더 보수적인(큰) 값 사용
    return base


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

    # 동적 트리거: 진입 시 설정값 사용
    trigger = float(ts.get("trigger_pct") or _TRAILING_TRIGGER)

    # [포지션-1/2] ATR·거래량 압력·수익률 기반 동적 floor
    floor_gap = _get_dynamic_floor_pct(ticker, ts, macd_tight=tight, gain_pct=gain_pct)

    if gain_pct >= trigger:
        candidate_floor = current_price * (1 - floor_gap / 100)
        new_floor = max(current_floor, candidate_floor)
    elif tight and gain_pct > 0:
        # MACD 약화 + 수익권: 즉시 타이트하게 추적
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
    # 종목별 누적 통계 갱신 (ticker_stats)
    if pnl_pct is not None:
        try:
            _update_ticker_stats(ticker, name, pnl_pct)
        except Exception as _e:
            logger.debug(f"ticker_stats 갱신 실패 [{ticker}]: {_e}")


def _update_ticker_stats(ticker: str, name: str, pnl_pct: float) -> None:
    """매도 체결 시 ticker_stats 누적 통계 갱신 (avg_win/loss, best_entry_hour 포함)."""
    is_win = 1 if pnl_pct > 0 else 0
    row = fetch_one("SELECT * FROM ticker_stats WHERE ticker = ?", (ticker,))

    if row:
        total  = row["total_trades"] + 1
        wins   = row["win_count"] + is_win
        losses = row["loss_count"] + (1 - is_win)
        new_avg_pnl = (row["avg_pnl_pct"] * row["total_trades"] + pnl_pct) / total

        # 이익/손실 평균 누적 (Kelly 분자·분모)
        old_win_pct  = float(row["avg_win_pct"]  or 0.0)
        old_loss_pct = float(row["avg_loss_pct"] or 0.0)
        if is_win:
            new_avg_win  = (old_win_pct * row["win_count"] + pnl_pct) / wins if wins else pnl_pct
            new_avg_loss = old_loss_pct
        else:
            new_avg_win  = old_win_pct
            new_avg_loss = (old_loss_pct * row["loss_count"] + abs(pnl_pct)) / losses if losses else abs(pnl_pct)

        # best_entry_hour: 이긴 거래의 진입 시각 집계 → 가장 빈도 높은 시간대
        best_hour = row["best_entry_hour"]
        if is_win:
            best_hour = _compute_best_entry_hour(ticker)

        execute(
            """
            UPDATE ticker_stats
            SET total_trades = ?, win_count = ?, loss_count = ?,
                win_rate = ?, avg_pnl_pct = ?, avg_win_pct = ?, avg_loss_pct = ?,
                best_entry_hour = ?, name = ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE ticker = ?
            """,
            (
                total, wins, losses,
                wins / total,
                round(new_avg_pnl, 3),
                round(new_avg_win, 3),
                round(new_avg_loss, 3),
                best_hour,
                name,
                ticker,
            ),
        )
    else:
        best_hour = None
        if is_win:
            best_hour = _compute_best_entry_hour(ticker)
        execute(
            """
            INSERT INTO ticker_stats
                (ticker, name, total_trades, win_count, loss_count, win_rate,
                 avg_pnl_pct, avg_win_pct, avg_loss_pct, best_entry_hour)
            VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker, name,
                is_win, 1 - is_win,
                float(is_win),
                round(pnl_pct, 3),
                round(pnl_pct, 3) if is_win else 0.0,
                round(abs(pnl_pct), 3) if not is_win else 0.0,
                best_hour,
            ),
        )


def _reconcile_zombie_trailing_stops(engine_instance) -> None:
    """
    KIS API 정상 응답인데 잔고가 0일 때 호출.
    trailing_stop에 남아있는 레코드 = KIS가 자동 손절을 체결했지만
    시스템이 통보받지 못한 좀비 포지션.
    매도 이력을 기록하고 trailing_stop을 정리한다.
    """
    try:
        zombie_rows = fetch_all("SELECT * FROM trailing_stop")
        if not zombie_rows:
            return
        from src.infra.stop_order_manager import get_stop_order_price
        for row in zombie_rows:
            row = dict(row)
            ticker = row["ticker"]
            entry_price = float(row.get("entry_price") or 0)
            stop_price = (
                get_stop_order_price(ticker)
                or float(row.get("trailing_floor") or entry_price)
            )
            # 수량·종목명: position_snapshot → trades(최근 buy) 순으로 조회
            snap = fetch_one(
                "SELECT quantity, name FROM position_snapshot WHERE ticker=?", (ticker,)
            )
            buy_rec = fetch_one(
                "SELECT quantity, name FROM trades WHERE ticker=? AND action='buy' "
                "AND date=DATE('now','localtime') ORDER BY id DESC LIMIT 1",
                (ticker,),
            )
            qty = int((snap["quantity"] if snap else None) or (buy_rec["quantity"] if buy_rec else 0))
            name = (snap["name"] if snap else None) or (buy_rec["name"] if buy_rec else ticker)
            logger.warning(
                f"[좀비 포지션 정리] {ticker} | KIS 잔고 0인데 trailing_stop 존재 "
                f"→ 손절가 {stop_price:,.0f}원으로 자동 체결 처리"
            )
            # 같은 날 sell 기록이 있으면 중복 기록 방지
            existing_sell = fetch_one(
                "SELECT 1 FROM trades WHERE ticker=? "
                "AND action IN ('sell','stop_loss','stop_order_auto') "
                "AND date=DATE('now','localtime')",
                (ticker,),
            )
            if not existing_sell and qty > 0:
                _record_trade(
                    ticker=ticker,
                    action="stop_loss",
                    quantity=qty,
                    exec_price=stop_price,
                    signal_source="stop_order_auto",
                    reason="KIS 잔고 0 확인 — 사전 손절 주문 자동 체결 (복구 처리)",
                    avg_price=entry_price,
                    name=name,
                )
            _delete_trailing_stop(ticker)
            try:
                engine_instance._ws_subscribed.discard(ticker)
                engine_instance._ws_triggered.discard(ticker)
                engine_instance._qty_cache.pop(ticker, None)
                from src.infra.kis_websocket import KISWebSocket
                KISWebSocket().unsubscribe(ticker)
            except Exception:
                pass
            try:
                from src.teams.domestic_stock.engine import release_slot, get_slot_for_ticker
                _slot = get_slot_for_ticker(ticker)
                if _slot:
                    release_slot(_slot)
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"좀비 trailing_stop 정리 실패: {e}")


def _compute_best_entry_hour(ticker: str) -> int | None:
    """
    trade_context에서 이 종목의 승리 거래 진입 시각을 조회해
    가장 빈도 높은 시간대(시 단위)를 반환한다.
    데이터 부족 시 None 반환.

    NOTE: trade_context.trade_id는 매수 레코드 id → 매수 레코드엔 pnl이 없음.
    같은 날 해당 티커에 수익 매도(take_profit)가 있으면 해당 진입 시각을 승리로 집계.
    """
    try:
        rows = fetch_all(
            """
            SELECT tc.entry_hhmm
            FROM trade_context tc
            WHERE tc.ticker = ?
              AND tc.entry_hhmm IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM trades t
                  WHERE t.ticker = tc.ticker
                    AND t.date   = tc.trade_date
                    AND t.pnl_pct > 0
                    AND t.action IN ('take_profit', 'partial_exit')
              )
            """,
            (ticker,),
        )
        if not rows:
            return None
        hour_counts: dict[int, int] = {}
        for r in rows:
            hhmm = str(r["entry_hhmm"]).zfill(4)
            hour = int(hhmm[:2])
            hour_counts[hour] = hour_counts.get(hour, 0) + 1
        return max(hour_counts, key=hour_counts.get)
    except Exception:
        return None
