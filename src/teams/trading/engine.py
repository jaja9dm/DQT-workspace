"""
engine.py — 매매팀 메인 엔진

역할:
  Hot List 종목을 대상으로 다단계 게이트를 통과한 경우에만
  Claude에 최종 매수 판단을 요청하고 KIS API로 주문을 실행한다.

게이트 구조 (순서대로, 하나라도 실패 시 진입 차단):
  Gate 0.  장 시작 오프닝 게이트 — 9:00 즉시 매수 vs 9:10 대기 판단 (Claude)
  Gate 1.  리스크 레벨 — Level 4↑이면 신규 진입 제한
  Gate 2.  글로벌 시황 — korea_market_outlook == 'negative'이면 차단
  Gate 3.  국내 시황 — market_score < -0.3이면 차단
  Gate 4.  Hot List — DB에서 최신 Hot List 읽기
  Gate 4.2 Hot List 품질 필터 — 거래량비·RSI·모멘텀 복합 AND 조건
  Gate 4.5 MACD 방향 필터 + 장초반 진입 품질 — sell_pre 차단, 09:30 전 눌림·소진 확인
  Gate 5.  Claude 최종 판단 — 매수 여부 + 예상 목표가·손절가

오프닝 게이트 (Gate 0):
  9:00 직후 첫 사이클에서 Claude가 시황을 평가.
  "진짜 좋다" → 즉시 매수 허용.
  "관망 필요" → 9:10까지 신규 매수 차단, 텔레그램 알림.
  9:10 이후에는 시황 무관하게 매수 재개 (스케줄러가 강제 트리거).

분할 매수 (3회):
  1차: 40% 즉시 실행
  2차: 35% — 1차 체결 확인 후 5분 이내 또는 -1% 하락 시 추가 진입
  3차: 25% — 2차 이후 추가 하락(-1%) 시 진입

실행 주기: 5분마다 (국내 주식팀과 동기)
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, RequestPriority
from src.teams.risk.engine import get_current_risk
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_INTERVAL_SEC = 300          # 5분
_KIS_ORDER_PATH = "/uapi/domestic-stock/v1/trading/order-cash"
_KIS_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"

# 분할 매수 비율 — 1차에 60% 집중, 나머지 분할
_TRANCHE_RATIOS = [0.60, 0.25, 0.15]

# ─────────────────────────────────────────────────────────────────────
# 매수 판단 정적 시스템 프롬프트 — 캐시 대상 (1024 토큰 이상)
#
# 매 5분 주기마다 동일하게 전송되는 판단 기준 · 룰 · 출력 형식.
# cache_control=ephemeral 로 5분 이내 재호출 시 토큰 ~70% 절감.
# ─────────────────────────────────────────────────────────────────────
_BUY_SYSTEM_PROMPT = """당신은 국내 주식 퀀트 트레이딩 시스템의 최종 매수 판단 AI입니다.
Hot List에서 선별된 후보 종목들을 받아 실제 주문 실행 여부를 결정합니다.
이 판단은 실제 자금이 투입되므로 근거 있는 결정이 중요합니다.

## 역할
- 매 5분 주기로 최대 3개 후보 종목을 일괄 검토합니다.
- 각 종목에 대해 즉시 매수(buy: true) 또는 보류(buy: false)를 결정합니다.
- 종목별로 목표 수익률(target_pct)과 손절 기준(stop_pct)을 제시합니다.

## 입력 데이터 필드 설명
- signal_type: 선정 근거 유형 (volume_surge=거래량 급등, breakout=볼린저밴드 돌파, momentum=모멘텀)
- price_change_pct: 당일 등락률 (%)
- volume_ratio: 최근 평균 대비 거래량 배수 (1.0=평균, 3.0=3배)
- rsi: 상대강도지수 (0~100). 30이하=과매도, 70이상=과열
- reason: Hot List 선정 시 기록된 근거
- sentiment_score: 최근 뉴스 감성 점수 (-1.0 부정 ~ +1.0 긍정)
- sentiment_direction: 뉴스 감성 방향 (bullish/bearish/neutral)

## 매수 기준 — 전략별 판단

signal_type에 따라 아래 전략 기준을 적용하세요.

### 전략 D: opening_plunge_rebound (오프닝 급락 반등) — 삼성SDI 패턴
- 장 시작 후 5~20분 내 시가 대비 -3% 이상 급락 후 반등 중
- 전일비 등락률 무관 (플러스여도 됨 — 시가 대비 급락이 핵심)
- 3분봉 AND 5분봉 동시 buy_pre 필수 (한 쪽만으론 불충분)
- target_pct: 2~4% (빠른 수익 확정, 당일 청산)
- stop_pct: 1.5% (급락 지속 시 빠른 컷)
- 10:30 이후는 오프닝 타이밍 종료 — 진입 불가

### 전략 A: gap_up_breakout (갭업 돌파)
- OBV 양수 + 갭업이면 즉시 buy: true
- target_pct: 갭업 비율의 30~50% (갭 +15%면 목표 +4~7%)
- stop_pct: 1.5% 고정 (갭업 후 되돌림 빠른 컷)
- RSI 80↑이어도 OBV 양수 + 거래량 폭발이면 허용
- 11:00~13:00: MACD buy_pre 확인 시만 허용 / 13:00 이후 차단

### 전략 B: pullback_rebound (눌림목 반등)
- 오늘 -0.5%~-5% 하락 중 시가 대비 반등 + OBV 양수 → buy: true
- target_pct: 3~4% (빠른 수익 확정, 당일 청산 원칙)
- stop_pct: 1.5% (타이트 — 반등 실패 시 즉시 컷)
- RSI 55↑이면 보류 (이미 오른 구간에서 반등 진입은 위험)
- 기술적 반등 신호 없으면 보류

### 전략 C: market_momentum (시장 강세 편승)
- KOSPI 강세 + 외인+기관 동시 순매수 + RSI 45~70 → buy: true
- target_pct: 4~6% (시장 상승분 먹기)
- stop_pct: 2.0% (KOSPI 꺾이면 즉시 이탈)
- 등락 +6% 초과 종목은 보류 (이미 많이 올라 추격 위험)
- 외인 또는 기관 중 하나라도 순매도면 보류

### 일반 신호 (volume_surge / breakout / momentum)
- volume_surge + RSI 45~70: target 6~8%, stop 2%
- breakout 신호: target 7~10%, stop 2.5%
- momentum 약한 신호: target 4~5%, stop 2%
- 글로벌 리스크 6↑ 또는 시황 낮음: 목표 -1~2% 축소

**중요: 손익비(target_pct / stop_pct) ≥ 2.0 이상으로 설정하세요.**

## 공통 보류 기준
- 글로벌 리스크 8 이상
- 국내 시황 점수 -0.2 이하
- 감성 점수 -0.5 이하 (강한 부정 뉴스)
- pullback_rebound/market_momentum 외 종목에서 RSI 75 초과

예시: target_pct=6, stop_pct=2 → 손익비 3:1 ✅
      target_pct=4, stop_pct=3 → 손익비 1.33:1 ❌ (buy=false 권고)

## 응답 형식 (JSON만 출력, 마크다운 불가)
{
  "decisions": [
    {
      "ticker": "<6자리 종목코드>",
      "buy": <true|false>,
      "reason": "<판단 근거 30자 이내>",
      "target_pct": <목표 수익률, 양수 숫자>,
      "stop_pct": <손절 기준, 양수 숫자>
    }
  ]
}

종목이 여러 개이면 decisions 배열에 순서대로 모두 포함하세요."""

# 게이트 임계값
_MARKET_SCORE_GATE = -0.5     # 국내 시황 최소 점수 (스캘핑·단타: -0.5 이하만 차단)
_RISK_LEVEL_GATE = 4          # 이 레벨 이상이면 신규 진입 금지


class TradingEngine:
    """매매팀 엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="trading-engine",
        )
        self._today_tickers: set[str] = set()   # 당일 이미 매수한 종목 (중복 방지)
        self._macd_reentry_ok: set[str] = set()  # MACD 조기손절 후 재진입 허용 종목

        # 오프닝 게이트 관련
        self._opening_gate_checked: bool = False  # 당일 오프닝 게이트 판단 완료 여부
        self._buy_allowed_from: datetime | None = None  # 매수 허용 시작 시각 (None=즉시)

    def start(self) -> None:
        logger.info("매매팀 엔진 시작")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=15)
        logger.info("매매팀 엔진 종료")

    def reset_opening_gate(self) -> None:
        """오프닝 게이트 해제 — 09:10 재점검 시 스케줄러가 호출."""
        self._buy_allowed_from = None
        logger.info("오프닝 게이트 해제 — 매수 재개")

    # ──────────────────────────────────────────
    # 메인 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                # 날짜 변경 시 당일 중복 방지 목록 초기화
                if date.today().isoformat() not in getattr(self, "_today_str", ""):
                    self._today_str = date.today().isoformat()
                    self._today_tickers.clear()
                    self._macd_reentry_ok.clear()
                    self._opening_gate_checked = False
                    self._buy_allowed_from = None

                self.run_once()
            except Exception as e:
                logger.error(f"매매팀 오류: {e}", exc_info=True)
            self._stop_event.wait(timeout=_INTERVAL_SEC)

    def run_once(self) -> list[dict]:
        """
        1회 실행: 게이트 체크 → Hot List 조회 → Claude 판단 → 주문 실행.

        Returns:
            실행된 주문 목록
        """
        # ── Gate 0: 오프닝 게이트 ───────────────
        now = datetime.now()

        # 신규 매수 마감 (13:30 이후 — 장 후반 추가 진입 지양, 청산 여유 확보)
        _hm = now.hour * 100 + now.minute
        if _hm >= 1330:
            logger.debug(f"13:30 이후 신규 매수 차단 ({now.strftime('%H:%M')})")
            return []

        if not self._opening_gate_checked:
            # 첫 사이클에서 시황 평가 후 즉시 매수 or 9:10 대기 결정
            self._opening_gate_checked = True
            immediate = self._check_opening_gate()
            if not immediate:
                # 9:10 (09:10:00) 이후부터 매수 허용
                self._buy_allowed_from = now.replace(hour=9, minute=10, second=0, microsecond=0)
                logger.info(f"오프닝 게이트: 관망 — {self._buy_allowed_from.strftime('%H:%M')}부터 매수 허용")
                return []

        if self._buy_allowed_from and now < self._buy_allowed_from:
            logger.debug(f"오프닝 게이트 대기 중 — {self._buy_allowed_from.strftime('%H:%M')}까지 신규 매수 차단")
            return []

        # ── Gate 0.5: 일일 손실 한도 / 연속 손절 차단 ──────────────
        # 복리의 적: 하루 큰 손실. 아래 중 하나라도 해당하면 신규 진입 중단.
        #   ① 당일 실현 P&L 합계 ≤ -2% (예수금 기준)
        #   ② 당일 stop_loss 횟수 ≥ 2회 (오늘은 시장이 맞지 않는 날)
        _daily_pnl_limit_pct = -2.0  # 당일 누적 손실 -2% 이상이면 신규 진입 금지
        _daily_stoploss_limit = 2    # 당일 손절 2회 이상이면 신규 진입 금지
        today_str = str(date.today())

        # 당일 손절 횟수
        sl_row = fetch_one(
            "SELECT COUNT(*) AS cnt FROM trades WHERE date=? AND action='stop_loss'",
            (today_str,),
        )
        daily_sl_count = int(sl_row["cnt"]) if sl_row else 0
        if daily_sl_count >= _daily_stoploss_limit:
            logger.info(
                f"Gate 0.5 차단: 당일 손절 {daily_sl_count}회 ≥ {_daily_stoploss_limit}회 "
                f"— 오늘 신규 진입 중단"
            )
            return []

        # 당일 실현 P&L 합계 (매수 예수금 기준 근사: pnl 합계 / 초기 예수금)
        pnl_row = fetch_one(
            "SELECT COALESCE(SUM(pnl), 0) AS total_pnl FROM trades WHERE date=? AND pnl IS NOT NULL",
            (today_str,),
        )
        daily_pnl = float(pnl_row["total_pnl"]) if pnl_row else 0.0
        if daily_pnl < 0:
            avail = _fetch_available_cash()
            if avail > 0:
                daily_pnl_pct = daily_pnl / avail * 100
                if daily_pnl_pct <= _daily_pnl_limit_pct:
                    logger.info(
                        f"Gate 0.5 차단: 당일 실현 손익 {daily_pnl_pct:+.2f}% ≤ {_daily_pnl_limit_pct}% "
                        f"— 오늘 신규 진입 중단 (손실 보호)"
                    )
                    return []

        # ── Gate 1: 리스크 레벨 ─────────────────
        risk = get_current_risk()
        level = risk.get("risk_level", 1)
        position_limit_pct = risk.get("position_limit_pct", 100)
        max_single_pct = risk.get("max_single_trade_pct", 5.0)

        if level >= _RISK_LEVEL_GATE:
            logger.info(f"Gate 1 차단: 리스크 레벨 {level} — 신규 진입 금지")
            return []

        # ── Gate 1.5: 최대 보유 종목 수 ──────────
        from src.teams.research.param_tuner import get_param
        max_pos = int(get_param("max_positions", 3.0))
        open_count = _count_open_positions()
        if open_count >= max_pos:
            logger.debug(f"Gate 1.5 차단: 현재 {open_count}종목 보유 (최대 {max_pos}종목) — 신규 진입 금지")
            return []

        # ── Gate 2: 글로벌 시황 ──────────────────
        global_ctx = _load_global_context()
        if global_ctx.get("korea_market_outlook") == "negative":
            logger.info("Gate 2 차단: 글로벌 시황 부정적 — 진입 보류")
            return []

        # ── Gate 3: 국내 시황 ────────────────────
        market_ctx = _load_market_context()
        market_score = market_ctx.get("market_score", 0.0)
        if market_score < _MARKET_SCORE_GATE:
            logger.info(f"Gate 3 차단: 국내 시황 점수 {market_score:.2f} — 진입 보류")
            return []

        # ── Gate 3.5: 장중 실시간 지수 방향 ──────
        # DB에 저장된 최신 시황 데이터에서 KOSPI/KOSDAQ 당일 등락률 확인.
        # 시장이 실제로 하락 중이면 시황 점수와 무관하게 신규 진입 차단.
        kospi_live = _load_live_index_change()
        if kospi_live is not None and kospi_live < -0.5:
            logger.info(
                f"Gate 3.5 차단: 장중 KOSPI {kospi_live:+.2f}% — "
                f"실시간 하락 중 신규 진입 보류"
            )
            return []

        # ── Gate 4: Hot List ─────────────────────
        hot_list = _load_hot_list()
        if not hot_list:
            logger.debug("Gate 4: Hot List 비어있음 — 대기")
            return []

        # 이미 당일 매수한 종목 제외
        # 단, MACD 조기손절 후 재진입 허용 종목(buy_pre 신호 복귀)은 재진입 가능
        from src.teams.intraday_macd.engine import (
            get_latest_macd_signal,
            get_macd_from_negative as _get_from_neg,
        )
        # 전일 손절 종목 쿨다운: 손절 후 1 거래일은 재진입 금지 (한온시스템 패턴 차단)
        _cooldown_days = 1
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        _prev_sl_tickers: set[str] = set()
        prev_sl_rows = fetch_all(
            "SELECT DISTINCT ticker FROM trades WHERE date=? AND action='stop_loss'",
            (yesterday,),
        )
        if prev_sl_rows:
            _prev_sl_tickers = {r["ticker"] for r in prev_sl_rows}
            if _prev_sl_tickers:
                logger.info(
                    f"[전일 손절 쿨다운] {_cooldown_days}일 재진입 금지: "
                    f"{', '.join(sorted(_prev_sl_tickers))}"
                )

        candidates = []
        for h in hot_list:
            ticker = h["ticker"]
            # 전일 손절 종목 쿨다운 체크
            if ticker in _prev_sl_tickers:
                logger.info(f"[쿨다운 차단] {ticker} — 전일 손절 이력, {_cooldown_days}일 재진입 금지")
                continue
            if ticker not in self._today_tickers:
                candidates.append(h)
            elif ticker in self._macd_reentry_ok:
                # 재진입 허용 종목: MACD buy_pre 신호 확인 후 진입
                # 단, 당일 손절(stop_loss) 발생 종목은 재진입 완전 차단
                was_stopped = fetch_one(
                    "SELECT id FROM trades WHERE ticker=? AND date=? AND action='stop_loss' LIMIT 1",
                    (ticker, str(date.today())),
                )
                if was_stopped:
                    logger.info(f"[재진입 차단] {ticker} — 당일 손절 이력 있음, MACD 재진입 불가")
                    self._macd_reentry_ok.discard(ticker)
                    continue
                macd_sig = get_latest_macd_signal(ticker, max_age_minutes=6)
                if macd_sig == "buy_pre" and not _has_open_position(ticker):
                    from_neg = _get_from_neg(ticker)
                    if from_neg:
                        logger.info(
                            f"[MACD 음수회복 재진입] {ticker} — 5분봉 hist 음수→회복 확인, "
                            f"강한 반전 신호 (풀사이즈 재매수)"
                        )
                    else:
                        logger.info(f"[MACD 재진입] {ticker} — MACD buy_pre 복귀 (사이즈 25% 축소)")
                    self._today_tickers.discard(ticker)
                    self._macd_reentry_ok.discard(ticker)
                    # 재진입 품질 플래그 태깅 (사이징 조정용)
                    h = dict(h)
                    h["_is_reentry"] = True
                    h["_from_negative"] = from_neg
                    candidates.append(h)

        if not candidates:
            return []

        # ── Gate 4.2: Hot List 품질 필터 (복합 AND 조건) ──────────
        # RSI 과열 구간: 완전 차단 대신 포지션 축소(rsi_hot 플래그)로 모멘텀 참여
        #   RSI 72~82 → 진입 허용, 1차 매수 50%·손절 1.5%로 제한
        #   RSI 82 초과 → 완전 차단 (극단적 과열)
        # OBV 역행 고RSI → 수급 없는 가짜 상승 → 차단
        from src.teams.research.param_tuner import get_param as _gp
        _min_vol      = _gp("hot_list_min_vol_ratio", 2.0)
        _max_rsi_hard = _gp("hot_list_max_rsi",       82.0)  # 완전 차단 상한 (82)
        _max_rsi_soft = _gp("hot_list_rsi_hot_limit", 72.0)  # 포지션 축소 시작 (72)
        _min_rsi      = _gp("hot_list_min_rsi",       35.0)  # 붕괴 하한 (35로 상향)

        filtered_candidates = []
        for c in candidates:
            tk        = c["ticker"]
            vol_ratio = c.get("volume_ratio")    or 0.0
            price_chg = c.get("price_change_pct") or 0.0
            rsi       = c.get("rsi")             or 50.0
            obv_slope = c.get("obv_slope")       or 0.0
            reason    = c.get("reason")          or ""

            day_range_pos = c.get("day_range_pos") or 0.5
            # 갭업 돌파 여부: signal_type 또는 price_chg >= 8% 판단
            is_gap_up = (
                c.get("signal_type") == "gap_up_breakout"
                or price_chg >= 8.0
            )

            signal_type  = c.get("signal_type", "momentum")
            is_pullback  = signal_type == "pullback_rebound"
            is_mkt_mom   = signal_type == "market_momentum"
            is_op_plunge = signal_type == "opening_plunge_rebound"

            # 완전 차단 조건 (전략별 예외 적용)
            fails = []

            # 전략D: 오프닝 급락 반등 — 시가 대비 intraday 변동이 핵심, 전일비 등락률 무관
            # 10:30 이후는 오프닝 타이밍이 지난 것 → 차단
            if is_op_plunge and _hm >= 1030:
                fails.append(f"오프닝 급락 반등 10:30 이후 차단 (현재 {now.strftime('%H:%M')})")

            if vol_ratio < _min_vol:
                # 갭업/눌림/강세편승/오프닝급락은 거래량 기준 1.2x로 완화
                _vol_min_eff = 1.2 if (is_gap_up or is_pullback or is_mkt_mom or is_op_plunge) else _min_vol
                if vol_ratio < _vol_min_eff:
                    fails.append(f"거래량비 {vol_ratio:.1f}x < {_vol_min_eff:.1f}x")
            # 눌림목 반등 / 오프닝 급락: 하락 종목이므로 등락률 ≤ 0 차단 면제
            if price_chg <= 0 and not (is_pullback or is_op_plunge):
                fails.append(f"등락률 {price_chg:+.2f}% ≤ 0")
            if rsi > _max_rsi_hard:
                # 갭업 돌파 + OBV 양수이면 RSI 90까지 허용
                _rsi_hard_eff = 90.0 if (is_gap_up and obv_slope > 0) else _max_rsi_hard
                if rsi > _rsi_hard_eff:
                    fails.append(f"RSI {rsi:.0f} > {_rsi_hard_eff:.0f} (극단 과열)")
            # 눌림목 반등 / 오프닝 급락: RSI 낮은 것이 정상 — 과매도 하한선 면제
            if rsi < _min_rsi and not (is_pullback or is_op_plunge):
                fails.append(f"RSI {rsi:.0f} < {_min_rsi:.0f} (과매도 붕괴)")
            if obv_slope < 0 and rsi > 70:
                # 갭업 돌파/강세편승 종목은 OBV 역행 차단 완화
                if not (is_gap_up or is_mkt_mom):
                    fails.append(f"OBV 역행+고RSI {rsi:.0f} (수급 없는 상승)")
            # 당일 고가권 추격 매수 차단 — 갭업 돌파 + OBV 양수이면 예외
            # 오프닝 급락 반등은 고가권에서 내려온 것 → 예외
            if price_chg >= 3.0 and day_range_pos >= 0.90 and obv_slope <= 0:
                if not (is_gap_up or is_op_plunge):
                    fails.append(
                        f"고가권 추격 차단 (등락 {price_chg:+.1f}% + 범위위치 {day_range_pos:.2f} + OBV↓)"
                    )
            # 갭업 돌파 종목 시간 제한:
            # 11:00 이후 ~ 13:00: MACD buy_pre 확인 시 뒤늦은 추격 허용 (Gate 4.5에서 검증)
            # 13:00 이후: 완전 차단 (너무 늦은 추격 — 갭 되돌림 위험)
            if is_gap_up and _hm >= 1300:
                fails.append(f"갭업 돌파 13:00 이후 진입 차단 (현재 {now.strftime('%H:%M')})")

            if fails:
                logger.info(f"Gate 4.2 차단: {tk} — {' | '.join(fails)}")
                continue

            # RSI 과열 구간(72~82): 진입 허용하되 포지션 축소 플래그
            rsi_hot = rsi > _max_rsi_soft
            c = dict(c)
            c["rsi_hot"] = rsi_hot
            if rsi_hot:
                logger.info(
                    f"Gate 4.2 RSI 과열 허용: {tk} RSI {rsi:.0f} "
                    f"→ 1차 매수 50% + 손절 1.5% 적용"
                )
            # Claude reason에 "RSI과열_포지션50%" 포함 시 자동 인식
            if rsi_hot and "RSI과열_포지션50%" not in reason:
                c["reason"] = (reason + " [RSI과열_포지션50%]").strip()

            filtered_candidates.append(c)

        candidates = filtered_candidates
        if not candidates:
            logger.debug("Gate 4.2: 복합 조건 통과 종목 없음")
            return []

        # ── Gate 4.5: MACD 방향 필터 + 장초반 진입 품질 체크 ──
        # [공통] MACD sell_pre: 히스토그램 양수에서 하강 중 → 수급 이탈 초기 → 진입 금지
        # [09:30 전] 단순 시간 대기 대신 지표 기반 유동 판단:
        #   ① MACD buy_pre 필수 (히스토그램 음수에서 상승 = 반등 확인)
        #   ② 진입 품질: 눌림 확인(현재가 < 시가×0.99) + 매도 소진 or 바닥 형성
        from src.teams.intraday_macd.engine import (
            get_latest_macd_signal as _get_macd,
            get_macd_dual_confirm as _get_macd_dual,
        )
        now_hm = now.hour * 100 + now.minute
        gated_candidates = []
        for c in candidates:
            tk = c["ticker"]
            sig_type = c.get("signal_type", "momentum")
            macd_now = _get_macd(tk, max_age_minutes=6)
            if macd_now == "sell_pre":
                logger.info(
                    f"Gate 4.5 차단: {tk} MACD sell_pre "
                    f"(히스토그램 양수 하강 중 — 수급 이탈 신호) — 진입 보류"
                )
                continue

            # 전략D: 오프닝 급락 반등 — 3분봉+5분봉 듀얼 확인 + 실제 급락 검증
            if sig_type == "opening_plunge_rebound":
                # ① 3분봉 AND 5분봉 동시 buy_pre 필수 (사용자 방식: 양쪽 모두 같은 방향)
                dual_ok = _get_macd_dual(tk, max_age_minutes=6)
                if not dual_ok:
                    logger.info(
                        f"Gate 4.5 차단: {tk} 오프닝 급락반등 — "
                        f"3분봉+5분봉 듀얼 buy_pre 미충족 (단일 신호 불충분)"
                    )
                    continue
                # ② 실제 오프닝 급락 기록 확인 (캔들 데이터 검증)
                plunge_ok, plunge_reason = _check_opening_plunge(tk)
                if not plunge_ok:
                    logger.info(
                        f"Gate 4.5 차단: {tk} 오프닝 급락 미확인 — {plunge_reason}"
                    )
                    continue
                logger.info(
                    f"Gate 4.5 통과: {tk} 오프닝 급락반등 듀얼MACD+급락확인 "
                    f"({now.strftime('%H:%M')}) — {plunge_reason}"
                )
                gated_candidates.append(c)
                continue

            # 갭업 종목 11:00~13:00: MACD buy_pre 확인 시만 뒤늦은 진입 허용
            is_c_gap_up = sig_type == "gap_up_breakout" or float(c.get("change_pct") or 0) >= 8.0
            if is_c_gap_up and 1100 <= now_hm < 1300:
                if macd_now != "buy_pre":
                    logger.info(
                        f"Gate 4.5 차단: {tk} 갭업 종목 {now.strftime('%H:%M')} — "
                        f"MACD buy_pre 미확인 ({macd_now}) — 뒤늦은 추격 보류"
                    )
                    continue
                logger.info(
                    f"Gate 4.5 통과: {tk} 갭업 종목 MACD buy_pre 뒤늦은 진입 허용 "
                    f"({now.strftime('%H:%M')})"
                )
            # 09:30 이전: 장 초반 변동성 구간 — 단순 시간 대기 대신 지표 기반 유동 판단
            if now_hm < 930 and tk not in self._macd_reentry_ok:
                # ① MACD buy_pre 필수: 히스토그램이 음수에서 상승 중이어야 함
                if macd_now != "buy_pre":
                    logger.info(
                        f"Gate 4.5 차단: {tk} 장초반 MACD 미확인 ({macd_now}) "
                        f"— buy_pre 신호 대기"
                    )
                    continue
                # ② 진입 품질 체크: 눌림 확인 + 매도 소진 or 바닥 형성
                quality_ok, quality_reason = _check_opening_dip_quality(tk)
                if not quality_ok:
                    logger.info(
                        f"Gate 4.5 차단: {tk} 장초반 진입 품질 미충족 — {quality_reason}"
                    )
                    continue
                logger.info(
                    f"Gate 4.5 통과: {tk} 장초반 진입 품질 확인 ({now.strftime('%H:%M')}) "
                    f"— {quality_reason}"
                )
            gated_candidates.append(c)
        candidates = gated_candidates

        if not candidates:
            return []

        # ── 가용 예수금 조회 ─────────────────────
        available_cash = _fetch_available_cash()
        if available_cash <= 0:
            logger.warning("예수금 부족 — 매수 불가")
            return []

        # 리스크 레벨에 따라 실제 사용 가능 예수금 제한
        usable_cash = available_cash * position_limit_pct / 100

        # ── Gate 5: Claude 최종 판단 (배치) ──────
        # momentum_score 기준 정렬된 상위 3종목만 (집중 투자)
        batch = candidates[:3]
        decisions = self._ask_claude_batch(
            items=batch,
            market_score=market_score,
            global_risk_score=global_ctx.get("global_risk_score", 5),
            risk_level=level,
        )  # {ticker: decision_dict}

        orders = []
        for item in batch:
            ticker = item["ticker"]
            decision = decisions.get(ticker, {"buy": False, "reason": "판단 없음", "target_pct": 5.0, "stop_pct": 5.0})

            if not decision.get("buy"):
                logger.info(f"Claude 매수 보류: {ticker} — {decision.get('reason', '')}")
                continue

            # ── R/R 필터: 손익비 2:1 미만 차단 ────────────────────────────
            target_pct = float(decision.get("target_pct") or 5.0)
            stop_pct_d = float(decision.get("stop_pct") or 2.0)
            rr_ratio = target_pct / stop_pct_d if stop_pct_d > 0 else 0
            if rr_ratio < 2.0:
                logger.info(
                    f"R/R 필터 차단: {ticker} 목표 {target_pct:.1f}% / 손절 {stop_pct_d:.1f}% "
                    f"= {rr_ratio:.2f}:1 (최소 2.0:1 필요)"
                )
                continue

            # 1주당 금액 조회
            current_price = _fetch_current_price(ticker)
            if current_price <= 0:
                continue

            # 종목당 투자 한도 — momentum_score 기반 동적 사이징
            # momentum_score 0~100 → 곱승 0.7~1.5 (선형 보간)
            # 점수 낮은 종목에 자금 덜 배분, 강한 신호에 집중
            mscore = float(item.get("momentum_score") or 0.0)
            ms_mult = 0.7 + (mscore / 100.0) * 0.8   # 0 → 0.7x, 100 → 1.5x
            ms_mult = max(0.7, min(1.5, ms_mult))
            base_invest = usable_cash * max_single_pct / 100
            max_invest = base_invest * ms_mult
            if mscore > 0:
                logger.info(
                    f"동적 사이징: {ticker} momentum={mscore:.0f} → "
                    f"투자비중 ×{ms_mult:.2f} ({base_invest/1e4:.0f}만→{max_invest/1e4:.0f}만원)"
                )

            # RSI 과열 종목: 1차 매수 비중 50%로 축소 (40% → 20% 실효)
            rsi_hot = item.get("rsi_hot", False)
            # 재진입 종목: 포지션 25% 축소 (리서치 기반 — 재진입 성공률 관리)
            # from_negative(음수→회복)이면 신뢰도 높아 축소 없음
            is_reentry = item.get("_is_reentry", False)
            reentry_mult = 1.0 if not is_reentry else (1.0 if item.get("_from_negative") else 0.75)
            if is_reentry and reentry_mult < 1.0:
                logger.info(f"재진입 사이즈 축소: {ticker} × {reentry_mult} (단순 buy_pre 재진입)")
            t1_ratio = _TRANCHE_RATIOS[0] * (0.5 if rsi_hot else 1.0) * reentry_mult
            tranche1_amt = max_invest * t1_ratio
            qty = max(1, int(tranche1_amt / current_price))

            result = self._place_buy(
                ticker=ticker,
                name=item.get("name", ""),
                quantity=qty,
                current_price=current_price,
                tranche=1,
                decision=decision,
                tight_stop=rsi_hot,   # RSI 과열 → 손절선 1.5%로 타이트
            )
            if result:
                orders.append(result)
                self._today_tickers.add(ticker)
                self._macd_reentry_ok.add(ticker)

                # RSI 과열 종목은 2·3차 분할 매수 비중도 50% 축소
                self._schedule_tranches(
                    ticker=ticker,
                    name=item.get("name", ""),
                    entry_price=current_price,
                    max_invest=max_invest * (0.5 if rsi_hot else 1.0),
                    decision=decision,
                )

        return orders

    # ──────────────────────────────────────────
    # 오프닝 게이트 (Gate 0)
    # ──────────────────────────────────────────

    def _check_opening_gate(self) -> bool:
        """
        장 시작 직후 시황을 평가하여 즉시 매수 가능 여부를 반환.

        Claude에게 현재 글로벌·국내 시황, 리스크 레벨을 종합하여 판단 요청.

        Returns:
            True  → 즉시 매수 허용 (시장이 진짜 좋음)
            False → 9:10까지 관망 권고
        """
        from src.utils.notifier import notify

        global_ctx  = _load_global_context()
        market_ctx  = _load_market_context()
        risk        = get_current_risk()
        risk_level  = risk.get("risk_level", 3)
        market_score = market_ctx.get("market_score", 0.0)
        global_risk  = global_ctx.get("global_risk_score", 5)
        outlook      = global_ctx.get("korea_market_outlook", "neutral")

        prompt = f"""당신은 국내 주식 퀀트 트레이더입니다.
오전 9시 장 시작 직후입니다. 지금 즉시 매수를 진행할지, 아니면 9시 10분까지 관망할지 판단하세요.

## 현재 시황
- 리스크 레벨: {risk_level}/5 (5가 최대 위험)
- 글로벌 리스크 점수: {global_risk}/10
- 한국 시장 전망: {outlook}
- 국내 시황 점수: {market_score:+.2f} (-1.0 약세 ~ +1.0 강세)

## 즉시 매수 기준 (아래 조건 대부분 충족 시 권고)
- 리스크 레벨 ≤ 3
- 글로벌 리스크 점수 ≤ 5
- 국내 시황 점수 ≥ 0.0 (neutral 이상이면 충분)
- 한국 시장 전망 neutral 또는 positive

## 즉시 매수 기준 (엄격하게 판단)
아래 조건을 모두 충족해야 즉시 매수(true):
- 리스크 레벨 ≤ 2
- 글로벌 리스크 점수 ≤ 4
- 국내 시황 점수 ≥ +0.2 (명확한 강세)
- 한국 시장 전망 positive

그 외에는 모두 관망(false)으로 선택하세요.

JSON만 응답:
{{"immediate": <true|false>, "reason": "<근거 30자 이내>"}}"""

        try:
            response = _client.messages.create(
                model=settings.CLAUDE_MODEL_FAST,
                max_tokens=200,
                temperature=0,
                timeout=30.0,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            raw = _extract_json(raw)
            result = json.loads(raw)
            immediate = bool(result.get("immediate", False))
            reason    = result.get("reason", "")

            if immediate:
                msg = f"✅ <b>[오프닝 게이트]</b> 즉시 매수 허용\n{reason}"
                logger.info(f"오프닝 게이트: 즉시 매수 — {reason}")
            else:
                msg = f"⏳ <b>[오프닝 게이트]</b> 9:10까지 관망\n{reason}\n(9:10 이후 자동 재개)"
                logger.info(f"오프닝 게이트: 관망 — {reason}")

            notify(msg)
            return immediate

        except Exception as e:
            logger.warning(f"오프닝 게이트 Claude 판단 실패: {e} — 기본값 관망")
            notify("⏳ <b>[오프닝 게이트]</b> Claude 판단 불가 — 9:10까지 관망 (기본값)")
            return False

    # ──────────────────────────────────────────
    # Claude 매수 판단 (배치 + 프롬프트 캐싱)
    # ──────────────────────────────────────────

    def _ask_claude_batch(
        self,
        items: list[dict],
        market_score: float,
        global_risk_score: int,
        risk_level: int,
    ) -> dict[str, dict]:
        """
        후보 종목 전체를 1번의 Claude 호출로 일괄 판단.

        기존 종목별 1회 호출(N번) → 배치 1회 호출로 줄이고,
        정적 판단 기준(_BUY_SYSTEM_PROMPT)을 캐시로 재사용.

        Returns:
            {ticker: {"buy": bool, "reason": str, "target_pct": float, "stop_pct": float}}
        """
        if not items:
            return {}

        # 후보별 동적 데이터 조립
        stock_lines = []
        for item in items:
            ticker = item["ticker"]
            sentiment = _load_sentiment(ticker)
            mscore = item.get("momentum_score") or 0.0
            drp    = item.get("day_range_pos") or 0.5
            obv    = item.get("obv_slope") or 0.0
            stock_lines.append(
                f"- 티커: {ticker} ({item.get('name', '')})\n"
                f"  신호: {item.get('signal_type', '')} | "
                f"등락: {item.get('price_change_pct', 0):+.1f}% | "
                f"거래량: {item.get('volume_ratio', 0):.1f}배 | "
                f"RSI: {item.get('rsi', 50):.0f} | "
                f"모멘텀점수: {mscore:.0f}/100 | "
                f"당일범위위치: {drp:.2f} | "
                f"OBV기울기: {'↑' if obv > 0 else '↓'}{obv:+.2f}\n"
                f"  선정근거: {item.get('reason', '')} | "
                f"감성: {sentiment.get('avg_score', 0):+.2f}({sentiment.get('direction', 'neutral')})"
            )

        user_content = (
            f"## 현재 매크로 컨텍스트\n"
            f"- 리스크 레벨: {risk_level}/5\n"
            f"- 글로벌 리스크: {global_risk_score}/10\n"
            f"- 국내 시황 점수: {market_score:+.2f}\n\n"
            f"## 매수 판단 후보 ({len(items)}종목)\n"
            + "\n".join(stock_lines)
            + f"\n\n위 {len(items)}종목 전체에 대해 decisions 배열로 응답하세요."
        )

        _default = {"buy": False, "reason": "Claude 오류", "target_pct": 5.0, "stop_pct": 5.0}

        try:
            response = _client.messages.create(
                model=settings.CLAUDE_MODEL_MAIN,
                max_tokens=512,
                temperature=settings.CLAUDE_TEMPERATURE,
                timeout=30.0,
                system=[
                    {
                        "type": "text",
                        "text": _BUY_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_content}],
            )
            raw = response.content[0].text.strip()
            raw = _extract_json(raw)
            result = json.loads(raw)

            # 캐시 히트 로깅
            usage = response.usage
            cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
            if cache_read:
                logger.debug(f"매수판단 캐시 히트: {cache_read}토큰 절감")

            # ticker 키 딕셔너리로 변환 + hot_list 메타데이터 보강 (동적 파라미터용)
            item_meta = {item["ticker"]: item for item in items}
            decisions: dict[str, dict] = {}
            for d in result.get("decisions", []):
                t = d.get("ticker", "")
                if t:
                    meta = item_meta.get(t, {})
                    d["rsi"] = meta.get("rsi", 50.0)
                    d["volume_ratio"] = meta.get("volume_ratio", 1.0)
                    d["signal_type"] = meta.get("signal_type", "")
                    d["market_score"] = market_score
                    decisions[t] = d
                    logger.info(
                        f"매수 판단 [{t}] → {'BUY' if d.get('buy') else 'PASS'} | {d.get('reason', '')}"
                    )

            # 판단 누락 종목은 기본값 보류
            for item in items:
                if item["ticker"] not in decisions:
                    logger.warning(f"매수 판단 누락 [{item['ticker']}] — 보류 처리")
                    decisions[item["ticker"]] = _default

            return decisions

        except Exception as e:
            logger.error(f"Claude 배치 매수 판단 오류: {e}")
            from src.utils.notifier import check_claude_error
            check_claude_error(e, "매매팀 배치 판단")
            return {item["ticker"]: _default for item in items}

    # ──────────────────────────────────────────
    # 분할 매수 지연 실행
    # ──────────────────────────────────────────

    def _schedule_tranches(
        self,
        ticker: str,
        name: str,
        entry_price: float,
        max_invest: float,
        decision: dict,
    ) -> None:
        """2차(35%)·3차(25%) 분할 매수를 별도 스레드에서 지연 실행."""

        def _execute_tranches():
            for tranche_no, ratio in [(2, _TRANCHE_RATIOS[1]), (3, _TRANCHE_RATIOS[2])]:
                # 5분 대기 후 현재가 확인
                time.sleep(300)
                if self._stop_event.is_set():
                    break

                # 13:30 이후 추가 분할매수 차단
                _now_hm = datetime.now().hour * 100 + datetime.now().minute
                if _now_hm >= 1330:
                    logger.info(f"분할매수 {tranche_no}차 중단: 13:30 이후 추가 진입 지양")
                    break

                current = _fetch_current_price(ticker)
                if current <= 0:
                    break

                # 1차 진입가 대비 -1% 이상 하락 시에만 추가 진입
                drop_pct = (current - entry_price) / entry_price * 100
                if drop_pct > -1.0:
                    logger.debug(
                        f"분할 매수 {tranche_no}차 보류: {ticker} "
                        f"하락폭 {drop_pct:.2f}% < -1% 기준"
                    )
                    continue

                # 리스크 레벨 재확인
                risk = get_current_risk()
                if risk.get("risk_level", 1) >= _RISK_LEVEL_GATE:
                    logger.info(f"분할 매수 {tranche_no}차 중단: 리스크 레벨 상승")
                    break

                # 장중 KOSPI 방향 재확인 — 하락장에서 추가 물타기 방지
                kospi_now = _load_live_index_change()
                if kospi_now is not None and kospi_now < -0.5:
                    logger.info(
                        f"분할 매수 {tranche_no}차 중단: KOSPI {kospi_now:+.2f}% 하락 중 — 추가 진입 보류"
                    )
                    break

                amt = max_invest * ratio
                qty = max(1, int(amt / current))
                self._place_buy(
                    ticker=ticker,
                    name=name,
                    quantity=qty,
                    current_price=current,
                    tranche=tranche_no,
                    decision=decision,
                )

        t = threading.Thread(
            target=_execute_tranches,
            daemon=True,
            name=f"tranche-{ticker}",
        )
        t.start()

    # ──────────────────────────────────────────
    # KIS 매수 주문
    # ──────────────────────────────────────────

    def _place_buy(
        self,
        ticker: str,
        name: str,
        quantity: int,
        current_price: float,
        tranche: int,
        decision: dict,
        tight_stop: bool = False,   # RSI 과열 종목 → 손절선 1.5%로 타이트
    ) -> dict | None:
        """KIS API 시장가 매수 주문 + trades 테이블 저장."""
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
                    "ORD_DVSN": "01",       # 시장가
                    "ORD_QTY": str(quantity),
                    "ORD_UNPR": "0",
                    "ALGO_NO": "",
                },
                tr_id=tr_id,
                priority=RequestPriority.TRADING,
            )
            order_no = resp.get("output", {}).get("ODNO", "")

            execute(
                """
                INSERT INTO trades
                    (date, ticker, name, action, order_type, order_price,
                     exec_price, quantity, tranche, status, signal_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(date.today()),
                    ticker,
                    name,
                    "buy",
                    "market",
                    current_price,
                    current_price,
                    quantity,
                    tranche,
                    "filled",
                    "trading_engine",
                ),
            )

            logger.info(
                f"매수 주문 완료 [{tranche}차] {ticker}({name}) "
                f"{quantity}주 @ {current_price:,.0f}원 | 주문번호 {order_no}"
            )

            from src.utils.notifier import notify
            notify(
                f"🟢 <b>[{tranche}차 매수 체결]</b> {name}({ticker})\n"
                f"💰 {quantity}주 @ {current_price:,.0f}원\n"
                f"📝 {decision.get('reason', '')}"
            )

            # 1차 매수 시 트레일링 스톱 초기화 + 거래소 사전 손절 주문 제출
            if tranche == 1:
                stop_pct, trigger_pct, floor_pct = _calc_dynamic_trail_params(
                    signal_type=decision.get("signal_type", ""),
                    rsi=decision.get("rsi", 50.0),
                    volume_ratio=decision.get("volume_ratio", 1.0),
                    target_pct=decision.get("target_pct", 5.0),
                    stop_pct=decision.get("stop_pct", 2.0),
                    market_score=decision.get("market_score", 0.0),
                )
                # RSI 과열 → 손절선 강제 1.5% (동적 계산값보다 타이트하게)
                if tight_stop:
                    from src.teams.research.param_tuner import get_param as _gp2
                    stop_pct = _gp2("initial_stop_min_pct", 1.5)
                    logger.info(f"RSI 과열 타이트 손절 적용: {ticker} → {stop_pct}%")
                _init_trailing_stop(
                    ticker, current_price,
                    stop_pct=stop_pct,
                    trigger_pct=trigger_pct,
                    floor_pct=floor_pct,
                )
                initial_floor = current_price * (1 - stop_pct / 100)
                from src.infra.stop_order_manager import place_stop_order
                place_stop_order(ticker, quantity, initial_floor)

            return {
                "ticker": ticker,
                "name": name,
                "tranche": tranche,
                "quantity": quantity,
                "exec_price": current_price,
                "order_no": order_no,
                "reason": decision.get("reason", ""),
            }

        except Exception as e:
            logger.error(f"매수 주문 실패 [{ticker}]: {e}")
            return None


# ──────────────────────────────────────────────
# DB / KIS 데이터 로드 헬퍼
# ──────────────────────────────────────────────

def _extract_json(raw: str) -> str:
    """Claude 응답에서 JSON 부분만 추출 (코드블록·순수 JSON 모두 처리)."""
    import re
    m = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", raw)
    if m:
        return m.group(1).strip()
    start, end = raw.find("{"), raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        return raw[start:end + 1]
    return raw


def _check_opening_plunge(ticker: str) -> tuple[bool, str]:
    """
    오프닝 급락 확인: 09:00~09:20 구간에서 시가 대비 -3% 이상 급락이 있었는지 검증.
    삼성SDI 패턴: 갭업 오픈 후 5분 내 -5% 급락 → V자 반등 진입 타점.

    intraday_candles의 초기 봉에서 최저점을 시가와 비교.
    Returns: (급락 확인 여부, 사유 문자열)
    """
    rows = fetch_all(
        """
        SELECT bar_time, open, low, close FROM intraday_candles
        WHERE ticker = ?
          AND bar_time >= '090000'
          AND bar_time <= '092000'
        ORDER BY bar_time ASC
        LIMIT 20
        """,
        (ticker,),
    )
    if not rows:
        return False, "오프닝 캔들 데이터 없음"

    open_price = float(rows[0]["open"])
    if open_price <= 0:
        return False, "시가 데이터 없음"

    min_low = min(float(r["low"]) for r in rows if float(r["low"]) > 0)
    plunge_pct = (min_low - open_price) / open_price * 100

    if plunge_pct <= -3.0:
        return True, f"오프닝 급락 {plunge_pct:.1f}% 확인 (시가:{open_price:,.0f}→저점:{min_low:,.0f})"
    return False, f"오프닝 급락 미달 ({plunge_pct:.1f}%, 기준 -3%)"


def _check_opening_dip_quality(ticker: str) -> tuple[bool, str]:
    """
    장 초반(09:30 전) buy_pre 신호 발생 시 추가 진입 품질 검증.

    단순 시간 대기가 아니라 지표 기반으로 유동적 판단:
      조건 1. 현재가 < 당일 시가 × 0.99  — 시초 고점 매수 방지, 눌림 후 반등 확인
      조건 2a. 매도 소진: 최근 3봉 평균 거래량 < 개장 초기 5봉 평균 × 0.75
      조건 2b. 바닥 확인: 최근 3봉 연속 저가 상승 (higher lows)
      → 2a 또는 2b 중 하나 충족 시 통과

    intraday_candles 테이블의 당일 봉(bar_time >= 090000) 사용.
    데이터 부족(<5봉)이면 대기(False) 반환.

    Returns:
        (통과 여부, 사유 문자열)
    """
    rows = fetch_all(
        """
        SELECT bar_time, open, high, low, close, volume
        FROM intraday_candles
        WHERE ticker = ?
          AND bar_time >= '090000'
        ORDER BY bar_time ASC
        """,
        (ticker,),
    )
    if len(rows) < 5:
        return False, f"장초반 데이터 부족 ({len(rows)}봉 < 5봉)"

    opening_price = float(rows[0]["open"])
    current_price = float(rows[-1]["close"])

    # 조건 1: 시가 대비 눌림 확인 (1% 이상 하락 후 반등이어야 함)
    if current_price >= opening_price * 0.99:
        return False, (
            f"눌림 미확인 — 현재가 {current_price:,.0f} ≥ 시가 {opening_price:,.0f} × 0.99 "
            f"(시초 고점권 진입 방지)"
        )

    # 조건 2a: 매도 소진 — 개장 초기 급등 거래량이 최근 봉에서 감소
    early_avg  = sum(float(r["volume"]) for r in rows[:5]) / 5
    recent_avg = sum(float(r["volume"]) for r in rows[-3:]) / 3
    vol_exhausted = early_avg > 0 and recent_avg < early_avg * 0.75

    # 조건 2b: 바닥 확인 — 최근 3봉 저가가 연속으로 상승 (매도 압력 약화)
    higher_lows = False
    if len(rows) >= 3:
        lows = [float(r["low"]) for r in rows[-3:]]
        higher_lows = lows[0] < lows[1] < lows[2]

    if not (vol_exhausted or higher_lows):
        ratio = recent_avg / early_avg if early_avg > 0 else 0
        return False, (
            f"매도 미소진 & 바닥 미확인 "
            f"(거래량비 {ratio:.2f}x, higher_lows={higher_lows})"
        )

    parts = []
    if vol_exhausted:
        parts.append(f"거래량 소진({recent_avg/early_avg:.2f}x)")
    if higher_lows:
        parts.append("연속 저가 상승")
    return True, " + ".join(parts)


def _load_global_context() -> dict:
    row = fetch_one(
        "SELECT global_risk_score, korea_market_outlook FROM global_condition ORDER BY created_at DESC LIMIT 1"
    )
    return dict(row) if row else {"global_risk_score": 5, "korea_market_outlook": "neutral"}


def _load_market_context() -> dict:
    row = fetch_one(
        "SELECT market_score, market_direction FROM market_condition ORDER BY created_at DESC LIMIT 1"
    )
    return dict(row) if row else {"market_score": 0.0, "market_direction": "neutral"}


def _load_live_index_change() -> float | None:
    """
    DB에서 가장 최근 국내 시황 기록의 KOSPI 당일 등락률 반환.
    market_condition.summary JSON의 'kospi' 필드 사용.
    데이터 없거나 파싱 실패 시 None 반환 (게이트 통과 처리).
    """
    import json as _json
    row = fetch_one(
        "SELECT summary FROM market_condition ORDER BY created_at DESC LIMIT 1"
    )
    if not row or not row["summary"]:
        return None
    try:
        summary = _json.loads(row["summary"])
        val = summary.get("kospi")
        return float(val) if val is not None else None
    except Exception:
        return None


def _load_hot_list() -> list[dict]:
    """최근 10분 이내 hot_list 항목 반환 (momentum_score 기준 내림차순)."""
    rows = fetch_all(
        """
        SELECT ticker, name, signal_type, volume_ratio, price_change_pct, rsi, reason,
               COALESCE(momentum_score, 0.0) AS momentum_score,
               COALESCE(obv_slope, 0.0) AS obv_slope,
               COALESCE(day_range_pos, 0.5) AS day_range_pos
        FROM hot_list
        WHERE created_at >= datetime('now', '-10 minutes')
        ORDER BY COALESCE(momentum_score, 0.0) DESC, volume_ratio DESC
        LIMIT 10
        """
    )
    return [dict(r) for r in rows]


def _load_sentiment(ticker: str) -> dict:
    """SentimentCache에서 종목 감성 평균 조회."""
    try:
        from src.infra.sentiment_cache import SentimentCache
        cache = SentimentCache()
        results = cache.get_by_ticker(ticker)
        if not results:
            return {"avg_score": 0.0, "direction": "neutral"}
        avg = sum(r["score"] for r in results) / len(results)
        # 최신 direction 반환
        direction = results[0]["direction"] if results else "neutral"
        return {"avg_score": round(avg, 3), "direction": direction}
    except Exception:
        return {"avg_score": 0.0, "direction": "neutral"}


def _fetch_available_cash() -> float:
    """KIS API에서 주문 가능 예수금 조회."""
    gw = KISGateway()
    acnt_no, acnt_prdt_cd = (settings.KIS_ACCOUNT_NO.split("-") + ["01"])[:2]
    tr_id = "VTTC8908R" if settings.KIS_MODE == "paper" else "TTTC8908R"
    _KIS_CASH_PATH = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"

    try:
        resp = gw.request(
            method="GET",
            path=_KIS_CASH_PATH,
            params={
                "CANO": acnt_no,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": "005930",    # 삼성전자 기준 더미 — 예수금만 필요
                "ORD_UNPR": "0",
                "ORD_DVSN": "01",
                "CMA_EVLU_AMT_ICLD_YN": "N",
                "OVRS_ICLD_YN": "N",
            },
            tr_id=tr_id,
            priority=RequestPriority.DATA_COLLECTION,
        )
        output = resp.get("output", {})
        return float(output.get("ord_psbl_cash", 0) or 0)
    except Exception as e:
        logger.warning(f"예수금 조회 실패: {e}")
        return 0.0


def _fetch_current_price(ticker: str) -> float:
    """KIS API에서 현재가 조회."""
    gw = KISGateway()
    try:
        resp = gw.request(
            method="GET",
            path=_KIS_PRICE_PATH,
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            tr_id="FHKST01010100",
            priority=RequestPriority.TRADING,
        )
        return float(resp.get("output", {}).get("stck_prpr", 0) or 0)
    except Exception:
        return 0.0


def _has_open_position(ticker: str) -> bool:
    """trailing_stop 테이블에 해당 종목 레코드가 있으면 보유 중으로 간주."""
    row = fetch_one("SELECT ticker FROM trailing_stop WHERE ticker = ?", (ticker,))
    return row is not None


def _count_open_positions() -> int:
    """현재 보유 종목 수 (trailing_stop 행 수)."""
    row = fetch_one("SELECT COUNT(*) AS cnt FROM trailing_stop")
    return int(row["cnt"]) if row else 0


def _calc_dynamic_trail_params(
    signal_type: str,
    rsi: float,
    volume_ratio: float,
    target_pct: float,
    stop_pct: float,
    market_score: float,
) -> tuple[float, float, float]:
    """
    종목·시황 특성에 따라 트레일링 스톱 파라미터를 동적으로 산출.

    Returns:
        (initial_stop_pct, trigger_pct, floor_pct)
        - initial_stop_pct: 진입 직후 초기 손절선 간격 (%)
        - trigger_pct: 이 수익률 도달 시 트레일링 시작 (%)
        - floor_pct: 트레일링 손절선 간격 (현재가 대비 %)
    """
    # ── 초기 손절선 ─────────────────────────────────────────────
    # Claude 제시 stop_pct를 베이스로 RSI·시황 보정
    # 기준값·하한·상한을 strategy_params에서 읽어 자동 튜닝 가능
    from src.teams.research.param_tuner import get_param as _gp
    _stop_base = _gp("initial_stop_pct",     2.0)  # 기준 초기 손절 %
    _stop_min  = _gp("initial_stop_min_pct", 1.5)  # 하한 (이 아래로 안 내림)
    _stop_max  = _gp("initial_stop_max_pct", 3.5)  # 상한 (이 위로 안 올림)

    initial_stop = max(_stop_min, min(_stop_max, stop_pct if stop_pct else _stop_base))

    if rsi > 65:
        initial_stop = min(initial_stop, _stop_min)   # 과열권 — 타이트
    elif rsi < 45:
        initial_stop = min(initial_stop + 0.5, _stop_max * 0.86)  # 약한 모멘텀 — 여유

    if market_score < -0.1:
        initial_stop = min(initial_stop, _stop_min)   # 하락장 — 손실 최소화

    # ── 트레일링 시작 수익률 ─────────────────────────────────
    # 목표의 절반 지점부터 손절선 올리기 시작 (목표에 가까울수록 수익 보호)
    trigger_pct = max(2.0, target_pct * 0.5)

    if signal_type == "breakout":
        trigger_pct = max(2.5, trigger_pct)     # 돌파 신호 — 약간 더 주가 오른 후 트리거
    elif signal_type == "volume_surge" and volume_ratio > 5:
        trigger_pct = max(1.5, trigger_pct * 0.7)  # 거래량 폭발 — 빠른 반전 대비 일찍 트리거

    # ── 트레일링 간격 ─────────────────────────────────────────
    # 변동성(거래량·RSI)이 클수록 간격 넓게, 작을수록 좁게
    if rsi > 65 or volume_ratio > 5:
        floor_pct = 2.0                         # 고변동성 — 타이트하게 추적
    elif volume_ratio > 3:
        floor_pct = 2.5
    else:
        floor_pct = 3.0                         # 저변동성 — 여유 있게

    if market_score < -0.1:
        floor_pct = min(floor_pct, 2.0)         # 하락장 — 수익 빠르게 보호

    return round(initial_stop, 1), round(trigger_pct, 1), round(floor_pct, 1)


def _init_trailing_stop(
    ticker: str,
    entry_price: float,
    stop_pct: float | None = None,
    trigger_pct: float | None = None,
    floor_pct: float | None = None,
) -> None:
    """
    매수 시 트레일링 스톱 레코드 초기화.
    동적 파라미터(stop_pct, trigger_pct, floor_pct)를 함께 저장.
    이미 존재하면 entry_price·floor만 갱신 (동적 파라미터는 최초 진입값 유지).
    """
    _stop    = stop_pct    if stop_pct    is not None else settings.TRAILING_INITIAL_STOP_PCT
    _trigger = trigger_pct if trigger_pct is not None else settings.TRAILING_TRIGGER_PCT
    _floor   = floor_pct   if floor_pct   is not None else settings.TRAILING_FLOOR_PCT

    initial_floor = entry_price * (1 - _stop / 100)
    existing = fetch_one("SELECT * FROM trailing_stop WHERE ticker = ?", (ticker,))
    if existing:
        new_floor = max(float(existing["trailing_floor"]), initial_floor)
        execute(
            """
            UPDATE trailing_stop
            SET entry_price = ?, highest_price = MAX(highest_price, ?),
                trailing_floor = ?, updated_at = CURRENT_TIMESTAMP
            WHERE ticker = ?
            """,
            (entry_price, entry_price, new_floor, ticker),
        )
    else:
        execute(
            """
            INSERT INTO trailing_stop
                (ticker, entry_price, trailing_floor, highest_price,
                 ladder_bought, scale_in_count, dip_buy_count,
                 trigger_pct, floor_pct)
            VALUES (?, ?, ?, ?, 0, 0, 0, ?, ?)
            """,
            (ticker, entry_price, initial_floor, entry_price, _trigger, _floor),
        )
    logger.info(
        f"트레일링 스톱 초기화 [{ticker}] 매수가={entry_price:,.0f} "
        f"초기손절={initial_floor:,.0f} (stop={_stop}% | trigger=+{_trigger}% | floor={_floor}%)"
    )
