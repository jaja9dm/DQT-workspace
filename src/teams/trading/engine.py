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
  1차: 60% 즉시 실행 (신뢰도 높은 1차에 집중)
  2차: 25% — 1차 체결 확인 후 5분 이내 또는 -1% 하락 시 추가 진입
  3차: 15% — 2차 이후 추가 하락(-1%) 시 진입

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
- 체결강도: 실시간 매수/매도 체결 비율 (100=균형, 130↑=FOMO 매수세, 80↓=매도 이탈)
- 호가불균형: 상위 5단계 매수잔량/매도잔량 비율 (1.5↑=매수 대기 두꺼움=위로 뚫릴 압력)
- 공매도비율: 당일 공매도 비중 (%). 5%↑ + 거래량급등이면 쇼트스퀴즈 가능성 → ⚡표시
- RS당일: 당일 KOSPI 대비 초과수익률 (%). 양수=시장 대비 강세
- RS5일: 5일 누적 KOSPI 대비 초과수익률 (%). 추세 지속성 판단
- 섹터: KRX 업종 분류. 🔥=당일 강세 섹터 (수급 몰림), 🧊=약세 섹터 (수급 이탈)

## 심리 파도 해석 지침
- 체결강도 130↑ + 호가불균형 1.3↑ = 강한 FOMO 진입 중 → target +1~2% 상향 가능
- 체결강도 90↓ = 매수세 약해지는 중 → stop_pct 0.5% 타이트하게
- ⚡쇼트스퀴즈후보 = 쇼트커버 폭발 가능 → 거래량+체결강도 동시 확인 후 target 상향
- 호가불균형 0.7↓ = 매도 벽 두꺼움 → 왠만하면 보류

## 상대강도·섹터 해석 지침
- RS당일 +3%↑ + 🔥섹터 = 수급이 집중되는 종목 → 신호 동일하면 우선 진입
- RS당일 -2%↓ + 🧊섹터 = 시장보다 더 빠지는 종목 → 왠만하면 보류
- RS5일 +5%↑ = 5일간 지속 강세 — 단기 추격이 아닌 추세 종목 → target 소폭 상향 가능
- RS5일 -5%↓ = 추세 약세 — 단발 급등이면 되돌림 위험 → stop_pct 타이트하게

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
- **14:00 이후 진입 금지** — 장 마감 1시간 전 눌림 반등은 세력 마무리 매도와 겹쳐 되돌림 빠름

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

## 과거 통계 활용 ([과거통계] 항목)
- 제공된 경우: 종목별 실적 데이터 (총 거래수·승률·평균손익·평균보유·최적신호)
- **승률 30% 미만 + 평균손익 마이너스**: 강한 기술지표에도 buy=false 권고 (패턴 실패 종목)
- **승률 65% 이상 + 최적신호 일치**: 동일 점수 종목 중 우선 진입 (검증된 패턴)
- **평균보유 60분↑**: 당일 청산이 어려운 종목 — target_pct를 보수적으로 설정
- [과거통계] 없으면 (신규 종목) 기술지표만으로 판단

## 공통 보류 기준
- 글로벌 리스크 8 이상
- 국내 시황 점수 -0.2 이하
- 감성 점수 해석:
  - -0.3 ~ -0.5: 주의 (target_pct -1% 하향 보수적 설정)
  - -0.5↓: 보류 권고 (강한 부정 뉴스 — 기술적 신호 무력화 위험)
  - 종목 특정 부정 뉴스(산업재해·소송·부도)는 수치 무관 보류
- pullback_rebound/market_momentum 외 종목에서 RSI 75 초과
- 14:00 이후 pullback_rebound 전략: 보류 (마감 근접 눌림 반등 신뢰도 급감)

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

# 매수 허용 마감 시각 (HHMM) — 12:00 이후 신규 매수/추가매수/재진입 전면 차단
_TIME_BUY_CUTOFF = 1200


_WATCHDOG_INTERVAL_SEC  = 45    # 관심종목 폴링 주기 (45초)
_WATCHDOG_EXEC_SURGE    = 145.0 # 체결강도 임계값 — 이 이상이면 FOMO 매수세 급등
_WATCHDOG_VOL_ACCEL     = 3.0   # 거래량 가속도 임계값 — 현재 페이스가 일평균의 3배↑


class TradingEngine:
    """매매팀 엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="trading-engine",
        )
        self._today_tickers: set[str] = set()         # 당일 이미 매수한 종목 (중복 방지)
        self._macd_reentry_ok: set[str] = set()       # MACD 재진입 허용 종목
        self._macd_reentry_count: dict[str, int] = {} # 종목별 당일 MACD 재진입 횟수 (최대 2회)
        # 수익 실현 후 MACD 재진입 감시 목록
        # {ticker: {"exit_price": float, "exit_time": datetime, "item": dict, "reentry_count": int}}
        self._reentry_watchlist: dict[str, dict] = {}

        # 오프닝 게이트 관련
        self._opening_gate_checked: bool = False  # 당일 오프닝 게이트 판단 완료 여부
        self._buy_allowed_from: datetime | None = None  # 매수 허용 시작 시각 (None=즉시)

        # 관심종목 거래량 급등 감시 — 45초 폴링
        self._force_run = threading.Event()           # 감시 스레드가 즉시 실행 요청 시 set
        self._watchdog_vol: dict[str, dict] = {}      # {ticker: {"vol": int, "ts": float}}
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name="trading-watchdog",
        )

    def start(self) -> None:
        logger.info("매매팀 엔진 시작")
        self._thread.start()
        self._watchdog_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=15)
        self._watchdog_thread.join(timeout=10)
        logger.info("매매팀 엔진 종료")

    def reset_opening_gate(self) -> None:
        """오프닝 게이트 해제 — 09:10 재점검 시 스케줄러가 호출."""
        self._buy_allowed_from = None
        logger.info("오프닝 게이트 해제 — 매수 재개")

    # ──────────────────────────────────────────
    # 관심종목 거래량/체결강도 감시 (45초 폴링)
    # ──────────────────────────────────────────

    def _watchdog_loop(self) -> None:
        """
        매수 대기 중인 hot_list 종목을 45초 주기로 경량 폴링.

        감지 조건 (둘 중 하나):
          A. exec_strength ≥ 145 — FOMO 매수세 폭발 (체결강도 급등)
          B. 1분 거래량 페이스가 당일 평균의 3배 이상 — 거래량 가속 폭발

        감지 시: _force_run 이벤트 set → 메인 루프가 즉시 run_once() 실행
        """
        from src.teams.domestic_stock.collector import _fetch_price_from_kis
        import time as _time

        _MARKET_OPEN_SEC = 9 * 3600  # 09:00 기준 일일 경과초 계산

        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=_WATCHDOG_INTERVAL_SEC)
            if self._stop_event.is_set():
                break

            _hm = datetime.now().hour * 100 + datetime.now().minute
            # 장 외 시간 / 매수 마감 / 관찰 모드(09:07 이전) 폴링 생략
            if not (907 <= _hm < _TIME_BUY_CUTOFF):
                continue

            # 현재 매수 대기 중인 hot_list 종목 (당일 미매수 + 최근 10분 이내)
            try:
                pending = [
                    row for row in _load_hot_list()
                    if row["ticker"] not in self._today_tickers
                ]
            except Exception:
                continue

            if not pending:
                continue

            now_ts = _time.time()
            now_dt = datetime.now()
            elapsed_since_open = max(60.0, (
                (now_dt.hour - 9) * 3600 + now_dt.minute * 60 + now_dt.second
            ))

            surge_tickers: list[tuple[str, str]] = []  # [(ticker, reason)]

            for item in pending:
                ticker = item["ticker"]
                try:
                    price, chg_pct, volume, _, _, _, _, _, _, exec_str = _fetch_price_from_kis(ticker)
                except Exception:
                    continue

                if volume <= 0:
                    continue

                # ── A. 체결강도 급등 감지 ──────────────────────────
                if exec_str >= _WATCHDOG_EXEC_SURGE:
                    surge_tickers.append((
                        ticker,
                        f"체결강도 {exec_str:.0f} ≥ {_WATCHDOG_EXEC_SURGE:.0f} (FOMO 매수세)"
                    ))
                    self._watchdog_vol[ticker] = {"vol": volume, "ts": now_ts}
                    continue

                # ── B. 거래량 가속 감지 ─────────────────────────────
                # 일평균 페이스(주/초) = 현재누적거래량 / 장 경과초
                avg_pace = volume / elapsed_since_open  # 주/초

                prev = self._watchdog_vol.get(ticker)
                if prev and (now_ts - prev["ts"]) >= 20:
                    delta_vol = volume - prev["vol"]
                    delta_sec = now_ts - prev["ts"]
                    if delta_vol > 0 and delta_sec > 0:
                        cur_pace = delta_vol / delta_sec  # 최근 N초 페이스
                        if avg_pace > 0 and (cur_pace / avg_pace) >= _WATCHDOG_VOL_ACCEL:
                            surge_tickers.append((
                                ticker,
                                f"거래량 가속 {cur_pace/avg_pace:.1f}x "
                                f"(최근{delta_sec:.0f}초 {delta_vol:,}주)"
                            ))

                self._watchdog_vol[ticker] = {"vol": volume, "ts": now_ts}

            if surge_tickers:
                for tk, reason in surge_tickers:
                    logger.warning(
                        f"[관심종목 급등 감지] {tk} — {reason} → 즉시 매수 사이클 트리거"
                    )
                self._force_run.set()  # 메인 루프에 즉시 실행 신호

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
                    self._macd_reentry_count.clear()
                    self._reentry_watchlist.clear()
                    self._watchdog_vol.clear()
                    self._opening_gate_checked = False
                    self._buy_allowed_from = None

                self.run_once()
                self._force_run.clear()  # 정상 사이클 완료 후 강제실행 플래그 초기화
            except Exception as e:
                logger.error(f"매매팀 오류: {e}", exc_info=True)

            # _force_run 이벤트 대기: 감시 스레드 신호 오면 즉시 깨어남
            # 아니면 5분 대기 (정상 주기)
            self._force_run.wait(timeout=_INTERVAL_SEC)
            if self._force_run.is_set():
                logger.info("관심종목 급등 감지 — 정규 주기 무시하고 즉시 매수 사이클 실행")

    def run_once(self) -> list[dict]:
        """
        1회 실행: 게이트 체크 → Hot List 조회 → Claude 판단 → 주문 실행.

        Returns:
            실행된 주문 목록
        """
        # ── Gate 0: 오프닝 게이트 ───────────────
        now = datetime.now()

        # 신규 매수 마감 (12:00 이후 — 오전 수급 타이밍 종료)
        # 12시 이후: 신규 진입·추가 매수 완전 차단. 기존 포지션 관리(손절·익절)만 유지.
        _hm = now.hour * 100 + now.minute
        if _hm >= _TIME_BUY_CUTOFF:
            logger.debug(f"12:00 이후 신규 매수 차단 ({now.strftime('%H:%M')}) — 포지션 관리 전용")
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
        if market_score <= _MARKET_SCORE_GATE:
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
                # MACD 재진입: 당일 최대 2회로 제한 (무한 재진입 방지)
                if self._macd_reentry_count.get(ticker, 0) >= 2:
                    logger.debug(f"[재진입 한도] {ticker} — 당일 MACD 재진입 2회 소진")
                    continue
                # 당일 손절 종목은 재진입 완전 차단
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

        # ── Gate 4.2: 진입 신뢰도 점수 필터 ─────────────────────────
        # 바이너리 AND 차단 → 연속형 점수(0~100) 기반 판단으로 전환.
        #
        # 점수 구성: 거래량(30) + RSI구간(20) + OBV기울기(20) + StochRSI(15) + 모멘텀/BB(15)
        # 절대 차단(hard_fail)은 유지 (RSI극단·거래량없음·OBV역행고RSI 등)
        # 점수 결과:
        #   ≥ 72점 → 풀사이즈 진입
        #   50~71점 → 75% 사이즈 진입 (경계 종목 — 기회 포기 대신 보수적 참여)
        #   < 50점 → 차단
        from src.teams.research.param_tuner import get_param as _gp
        _min_vol      = _gp("hot_list_min_vol_ratio", 2.0)
        _max_rsi_hard = _gp("hot_list_max_rsi",       82.0)
        _max_rsi_soft = _gp("hot_list_rsi_hot_limit", 72.0)
        _min_rsi      = _gp("hot_list_min_rsi",       35.0)

        # 피드백 루프 입력 — 최근 20일 신호 유형별 성과 (루프 외부에서 1회 조회)
        try:
            from src.teams.review.engine import _load_signal_feedback as _lsf
            _sig_feedback = _lsf(days=20)
        except Exception:
            _sig_feedback = {}

        # 현재 포트폴리오 섹터 분포 (섹터 집중 리스크 측정용)
        # position_snapshot = 현재 보유 종목, trade_context = 진입 시 섹터 기록
        try:
            _sector_rows = fetch_all(
                """
                SELECT tc.sector FROM position_snapshot ps
                JOIN trade_context tc ON tc.ticker = ps.ticker
                WHERE tc.sector IS NOT NULL
                GROUP BY ps.ticker
                """,
                (),
            )
            _portfolio_sector_counts: dict[str, int] = {}
            for _sr in _sector_rows:
                _s = str(_sr["sector"] or "")
                if _s:
                    _portfolio_sector_counts[_s] = _portfolio_sector_counts.get(_s, 0) + 1
        except Exception:
            _portfolio_sector_counts = {}

        filtered_candidates = []
        for c in candidates:
            tk        = c["ticker"]
            price_chg = float(c.get("price_change_pct") or 0.0)
            rsi       = float(c.get("rsi") or 50.0)
            reason    = c.get("reason") or ""

            signal_type  = c.get("signal_type", "momentum")
            is_gap_up    = signal_type == "gap_up_breakout" or price_chg >= 8.0
            is_pullback  = signal_type == "pullback_rebound"
            is_mkt_mom   = signal_type == "market_momentum"
            is_op_plunge = signal_type == "opening_plunge_rebound"

            _tk_stats      = _load_ticker_stats(tk)
            _sector_for_tk = str(c.get("sector") or "")
            _sector_cnt    = _portfolio_sector_counts.get(_sector_for_tk, 0) if _sector_for_tk else 0

            hard_fails, score, size_mult = _compute_entry_score(
                c=c,
                is_gap_up=is_gap_up,
                is_pullback=is_pullback,
                is_mkt_mom=is_mkt_mom,
                is_op_plunge=is_op_plunge,
                _hm=_hm,
                _min_vol=_min_vol,
                _max_rsi_hard=_max_rsi_hard,
                _max_rsi_soft=_max_rsi_soft,
                _min_rsi=_min_rsi,
                ticker_stats=_tk_stats,
                signal_feedback=_sig_feedback,
                sector_holdings=_sector_cnt,
            )

            if hard_fails:
                logger.info(f"Gate 4.2 차단: {tk} — {' | '.join(hard_fails)}")
                continue

            if size_mult == 0.0:
                logger.info(f"Gate 4.2 점수 차단: {tk} — 신뢰도 {score:.0f}/100 < 50")
                continue

            c = dict(c)
            c["_entry_score"] = score
            c["_score_size_mult"] = size_mult

            rsi_hot = rsi > _max_rsi_soft
            c["rsi_hot"] = rsi_hot
            if rsi_hot:
                logger.info(
                    f"Gate 4.2 RSI 과열 허용: {tk} RSI {rsi:.0f} "
                    f"→ 1차 매수 50% + 손절 1.5% 적용"
                )
                if "RSI과열_포지션50%" not in reason:
                    c["reason"] = (reason + " [RSI과열_포지션50%]").strip()

            if size_mult < 1.0:
                logger.info(
                    f"Gate 4.2 경계 통과: {tk} — 신뢰도 {score:.0f}/100 → 사이즈 ×{size_mult:.2f}"
                )
            else:
                logger.info(f"Gate 4.2 통과: {tk} — 신뢰도 {score:.0f}/100 (풀사이즈)")

            filtered_candidates.append(c)

        candidates = filtered_candidates
        if not candidates:
            logger.debug("Gate 4.2: 신뢰도 점수 통과 종목 없음")
            return []

        # ── Gate 4.3: 호가 잔량 불균형 + 공매도 비율 ──────────
        # 후보 종목 (최대 10개)에 한해 실시간 호가창 조회 (rate limit 안전)
        # 공매도 비율은 일별 KRX 캐시에서 무비용 조회
        from src.infra.short_selling import get_short_ratio as _get_sr
        _gw_ob = KISGateway()
        gate43_candidates = []
        for c in candidates:
            tk = c["ticker"]

            # 호가 잔량 불균형 조회
            ob = _gw_ob.get_orderbook(tk, priority=RequestPriority.TRADING)
            imbalance = ob.get("imbalance", 1.0)
            c = dict(c)
            c["_ob_imbalance"] = imbalance

            # 매도 벽이 압도적이면 차단 (눌림/오프닝급락은 면제 — 호가 역행이 정상)
            sig_type_43 = c.get("signal_type", "momentum")
            _exempt_43 = sig_type_43 in ("pullback_rebound", "opening_plunge_rebound")
            if imbalance < 0.6 and not _exempt_43:
                logger.info(
                    f"Gate 4.3 차단: {tk} 호가 불균형 {imbalance:.2f} "
                    f"(매도 잔량 {ob['ask_qty']:,} vs 매수 {ob['bid_qty']:,}) — 매도 벽 과다"
                )
                continue

            # 공매도 비율 조회 (KRX 일별 캐시)
            short_ratio = _get_sr(tk)
            c["_short_ratio"] = short_ratio

            # 공매도 비율이 높은 종목 + 거래량 급등 = 쇼트 스퀴즈 가능성 → 가산점 태깅
            c["_squeeze_candidate"] = short_ratio >= 5.0 and c.get("volume_ratio", 0) >= 3.0

            if imbalance >= 1.5:
                logger.info(
                    f"Gate 4.3 강한 매수 대기: {tk} 호가 {imbalance:.2f} "
                    + (f"[공매도 {short_ratio:.1f}% 쇼트스퀴즈 후보]" if c["_squeeze_candidate"] else "")
                )

            gate43_candidates.append(c)

        candidates = gate43_candidates
        if not candidates:
            logger.debug("Gate 4.3: 호가 잔량 필터 통과 종목 없음")
            return []

        # ── Gate 4.5: MACD 방향 필터 + 장초반 진입 품질 체크 ──
        # [공통] MACD sell_pre: 히스토그램 양수에서 하강 중 → 수급 이탈 초기 → 진입 금지
        # [09:30 전] 단순 시간 대기 대신 지표 기반 유동 판단:
        #   ① MACD buy_pre 필수 (히스토그램 음수에서 상승 = 반등 확인)
        #   ② 진입 품질: 눌림 확인(현재가 < 시가×0.99) + 매도 소진 or 바닥 형성
        from src.teams.intraday_macd.engine import (
            get_latest_macd_signal as _get_macd,
            get_macd_dual_confirm as _get_macd_dual,
            get_macd_signal_strength as _get_macd_strength,
        )
        now_hm = now.hour * 100 + now.minute
        gated_candidates = []
        for c in candidates:
            tk = c["ticker"]
            sig_type = c.get("signal_type", "momentum")
            macd_now = _get_macd(tk, max_age_minutes=6)
            macd_strength = _get_macd_strength(tk, max_age_minutes=6)
            if macd_now == "sell_pre":
                logger.info(
                    f"Gate 4.5 차단: {tk} MACD sell_pre (강도 {macd_strength:.0f}) "
                    f"— 수급 이탈 신호, 진입 보류"
                )
                continue
            # 신호 강도를 후보에 기록 (Claude 판단 및 사이징에 활용)
            c = dict(c)
            c["_macd_strength"] = macd_strength

            # Gate 4.5 VWAP 품질 필터: 모멘텀/갭업 종목은 현재가 ≥ VWAP 필수
            # 눌림목/오프닝급락은 VWAP 아래가 정상이므로 면제
            sig_type_c = c.get("signal_type", "momentum")
            _is_pullback_c  = sig_type_c == "pullback_rebound"
            _is_op_plunge_c = sig_type_c == "opening_plunge_rebound"
            if not (_is_pullback_c or _is_op_plunge_c):
                _vwap, _cur_px = _get_vwap_position(tk)
                if _vwap > 0 and _cur_px < _vwap * 0.995:
                    logger.info(
                        f"Gate 4.5 차단: {tk} 현재가 {_cur_px:,.0f} < VWAP {_vwap:,.0f} × 0.995 "
                        f"(수급 중심선 하회 — 모멘텀 약화)"
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
            is_c_gap_up = sig_type == "gap_up_breakout" or float(c.get("price_change_pct") or 0) >= 8.0
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
        # 12:00 이후는 run_once() 진입 자체가 차단되므로 여기까지 오면 항상 오전
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

            # 종목당 투자 한도 — momentum_score × 신뢰도 점수 × Kelly 이중 사이징
            # ms_mult:    momentum_score(0~130) → 0.7~1.5x (강한 신호에 집중)
            # score_mult: Gate 4.2 신뢰도(50~100) → 0.75~1.0x (경계 종목 보수적)
            # kelly_mult: 종목별 과거 통계 기반 Kelly fraction → 0.25~1.0x
            mscore = float(item.get("momentum_score") or 0.0)
            ms_mult = 0.7 + (mscore / 130.0) * 0.8   # 0 → 0.7x, 130 → 1.5x
            ms_mult = max(0.7, min(1.5, ms_mult))
            score_mult = float(item.get("_score_size_mult") or 1.0)

            # Kelly Criterion: kelly_f = W - (1-W)/R
            # W = 승률, R = 평균이익/평균손실 비율
            # 충분한 표본(≥5회) 있을 때만 적용, 아니면 중립(1.0)
            kelly_mult = 1.0
            _ts_kelly = _load_ticker_stats(ticker)
            if _ts_kelly and _ts_kelly.get("total_trades", 0) >= 5:
                _wr   = float(_ts_kelly.get("win_rate", 0.5))
                _wwin = float(_ts_kelly.get("avg_win_pct", 0.0))
                _wloss = float(_ts_kelly.get("avg_loss_pct", 0.0))
                if _wwin > 0 and _wloss > 0:
                    _rr = _wwin / _wloss
                    _kf = _wr - (1 - _wr) / _rr
                    kelly_mult = max(0.25, min(1.0, _kf))
                    if kelly_mult < 0.9:
                        logger.info(
                            f"Kelly 사이징: {ticker} W={_wr:.0%} win={_wwin:.1f}%/loss={_wloss:.1f}% "
                            f"→ Kelly={_kf:.2f} → ×{kelly_mult:.2f}"
                        )

            base_invest = usable_cash * max_single_pct / 100
            max_invest = base_invest * ms_mult * score_mult * kelly_mult
            if mscore > 0:
                logger.info(
                    f"동적 사이징: {ticker} momentum={mscore:.0f} "
                    f"×{ms_mult:.2f}(모멘텀) ×{score_mult:.2f}(신뢰도) ×{kelly_mult:.2f}(Kelly)"
                    f" → {max_invest/1e4:.0f}만원"
                )

            # 1차 매수 비중 결정 — RSI 과열 / 재진입 중 더 보수적인 쪽 하나만 적용
            # (둘 다 곱하면 0.60 × 0.5 × 0.75 = 0.225 → 의도한 것보다 과도한 축소)
            rsi_hot = item.get("rsi_hot", False)
            is_reentry = item.get("_is_reentry", False)
            reentry_mult = 1.0 if not is_reentry else (1.0 if item.get("_from_negative") else 0.75)
            if rsi_hot and is_reentry:
                # 둘 다 해당 시: 더 보수적인 쪽(0.5) 하나만 적용
                t1_size_mult = 0.5
                logger.info(f"사이즈 축소: {ticker} RSI과열+재진입 → 1차 50% (이중 페널티 방지)")
            elif rsi_hot:
                t1_size_mult = 0.5
                logger.info(f"사이즈 축소: {ticker} RSI과열 → 1차 50%")
            else:
                t1_size_mult = reentry_mult
                if is_reentry and reentry_mult < 1.0:
                    logger.info(f"사이즈 축소: {ticker} 재진입 × {reentry_mult} (단순 buy_pre)")
            t1_ratio = _TRANCHE_RATIOS[0] * t1_size_mult
            tranche1_amt = max_invest * t1_ratio
            qty = max(1, int(tranche1_amt / current_price))

            result = self._place_buy(
                ticker=ticker,
                name=item.get("name", ""),
                quantity=qty,
                current_price=current_price,
                tranche=1,
                decision=decision,
                tight_stop=rsi_hot,
                item_ctx=item,
            )
            if result:
                orders.append(result)
                self._today_tickers.add(ticker)
                self._macd_reentry_ok.add(ticker)
                if item.get("_is_reentry"):
                    self._macd_reentry_count[ticker] = self._macd_reentry_count.get(ticker, 0) + 1

                self._schedule_tranches(
                    ticker=ticker,
                    name=item.get("name", ""),
                    entry_price=current_price,
                    max_invest=max_invest * t1_size_mult,
                    decision=decision,
                )

        # ── 재진입 감시 업데이트 + 신호 실행 ────
        # 1. 당일 수익 실현 종목 → watchlist 추가
        # 2. watchlist 종목 중 MACD 골든크로스 확인 시 재진입
        self._update_reentry_watchlist()
        if open_count < max_pos:  # 포지션 여유 있을 때만 재진입 탐색
            reentry_orders = self._check_and_execute_reentry(
                usable_cash=usable_cash,
                max_single_pct=max_single_pct,
            )
            orders.extend(reentry_orders)

        return orders

    # ──────────────────────────────────────────
    # 오프닝 게이트 (Gate 0)
    # ──────────────────────────────────────────

    # ──────────────────────────────────────────
    # 수익 실현 후 MACD 기반 재진입
    # ──────────────────────────────────────────

    def _update_reentry_watchlist(self) -> None:
        """
        당일 수익 실현(take_profit/partial_exit) 종목을 재진입 감시 목록에 추가.
        이미 감시 중이거나 현재 포지션 보유 중이면 스킵.
        """
        today_str = str(date.today())
        profit_rows = fetch_all(
            """
            SELECT DISTINCT ticker, exec_price
            FROM trades
            WHERE date = ? AND action IN ('take_profit', 'partial_exit') AND pnl > 0
            ORDER BY id DESC
            """,
            (today_str,),
        )
        for row in profit_rows:
            ticker = row["ticker"]
            if ticker in self._reentry_watchlist:
                continue
            if _has_open_position(ticker):
                continue
            # 당일 손절 이력이 있으면 감시 제외
            was_stopped = fetch_one(
                "SELECT id FROM trades WHERE ticker=? AND date=? AND action='stop_loss' LIMIT 1",
                (ticker, today_str),
            )
            if was_stopped:
                continue
            # hot_list에서 최근 스냅샷 조회 (없으면 기본값)
            snap_row = fetch_one(
                """
                SELECT ticker, name, signal_type, volume_ratio, price_change_pct, rsi,
                       reason, momentum_score, obv_slope, day_range_pos,
                       stoch_rsi, bb_width_ratio, trading_value
                FROM hot_list WHERE ticker=? ORDER BY created_at DESC LIMIT 1
                """,
                (ticker,),
            )
            item_snapshot = dict(snap_row) if snap_row else {"ticker": ticker}
            self._reentry_watchlist[ticker] = {
                "exit_price": float(row["exec_price"] or 0),
                "exit_time": datetime.now(),
                "item": item_snapshot,
                "reentry_count": 0,
            }
            logger.info(f"[재진입 감시 등록] {ticker} — 수익 실현 후 MACD 재진입 대기")

    def _check_and_execute_reentry(
        self,
        usable_cash: float,
        max_single_pct: float,
    ) -> list[dict]:
        """
        재진입 감시 종목 중 3분봉+5분봉 MACD 동시 신호 + 거래량 급증 + RSI/VWAP 조건 충족 시 재진입.

        재진입 신호 (AND 조건 — 모두 충족해야 진입):
          1. 3분봉 AND 5분봉 모두 음수→양수 전환 (from_negative=True) — 양쪽 동시 반전만 유효
          2. 3분봉 AND 5분봉 개별 신호 모두 buy_pre (both_buy_pre=True) — 한 쪽만은 불충분
          3. 최근 2봉 평균 거래량 ≥ 이전 4봉 평균 × 1.3 — 거래량 재급증 확인
          4. RSI < 72 — 재상승 여력 충분 (75→72로 강화)
          5. 현재가 ≥ VWAP × 0.998 — 수급 중심선 위 (0.995→0.998로 강화)
          6. 1회 재진입만 허용 (당일)
        1분봉 MACD는 변동성이 너무 커 미사용 — 3분봉/5분봉만 참조.
        """
        from src.teams.intraday_macd.engine import get_macd_details as _get_macd_det

        orders: list[dict] = []
        to_remove: list[str] = []

        for ticker, watch in list(self._reentry_watchlist.items()):
            # 이미 재진입 완료 or 포지션 보유 중이면 제거
            if watch["reentry_count"] >= 1 or _has_open_position(ticker):
                to_remove.append(ticker)
                continue

            # 12:00 이후 재진입 금지 (오전 타이밍 종료)
            _now_hm = datetime.now().hour * 100 + datetime.now().minute
            if _now_hm >= _TIME_BUY_CUTOFF:
                to_remove.append(ticker)
                continue

            # ① 3분봉+5분봉 모두 음수→양수 전환 + 개별 신호 모두 buy_pre
            macd = _get_macd_det(ticker, max_age_minutes=6)
            if not macd.get("from_negative") or not macd.get("both_buy_pre"):
                logger.debug(
                    f"[재진입 보류] {ticker} — 3분봉/5분봉 동시 조건 미충족 "
                    f"(from_neg={macd.get('from_negative')}, "
                    f"sig_3m={macd.get('sig_3m')}, sig_5m={macd.get('sig_5m')})"
                )
                continue

            # ② 거래량 재급증 확인
            vol_rows = fetch_all(
                "SELECT volume FROM intraday_candles WHERE ticker=? ORDER BY bar_time DESC LIMIT 6",
                (ticker,),
            )
            if len(vol_rows) >= 4:
                recent_vol = sum(int(r["volume"]) for r in vol_rows[:2]) / 2
                prev_vol   = sum(int(r["volume"]) for r in vol_rows[2:6]) / 4
                if prev_vol > 0 and recent_vol < prev_vol * 1.3:
                    logger.debug(f"[재진입 보류] {ticker} — 거래량 재급증 미충족 ({recent_vol/prev_vol:.2f}x < 1.3x)")
                    continue

            # ③ RSI + VWAP 확인 (hot_list 스냅샷에서 RSI 읽기)
            item = watch["item"]
            rsi = float(item.get("rsi") or 55.0)
            if rsi >= 72.0:
                logger.debug(f"[재진입 보류] {ticker} — RSI {rsi:.0f} ≥ 72 (재상승 여력 부족)")
                continue

            vwap, cur_px = _get_vwap_position(ticker)
            if vwap > 0 and cur_px < vwap * 0.998:
                logger.debug(f"[재진입 보류] {ticker} — 현재가 {cur_px:,.0f} < VWAP {vwap:,.0f}×0.998 (수급 중심선 하회)")
                continue

            # 모든 조건 충족 → 재진입 실행
            current_price = _fetch_current_price(ticker)
            if current_price <= 0:
                continue

            # 재진입 사이즈: 원래 배분의 70% (보수적, 재진입 리스크 관리)
            mscore = float(item.get("momentum_score") or 0.0)
            ms_mult = 0.7 + (mscore / 130.0) * 0.8
            ms_mult = max(0.7, min(1.5, ms_mult))
            max_invest = usable_cash * max_single_pct / 100 * ms_mult * 0.70
            qty = max(1, int(max_invest * _TRANCHE_RATIOS[0] / current_price))

            logger.info(
                f"[MACD 재진입] {ticker} — 3분봉+5분봉 동시 음수→양수 전환 + both_buy_pre + 거래량↑ + VWAP 위 "
                f"| RSI {rsi:.0f} | 3m={macd.get('sig_3m')} 5m={macd.get('sig_5m')} "
                f"| 수량 {qty}주 @ {current_price:,.0f}"
            )

            # 재진입 판단: 간소화 (Claude 재호출 없이 지표 기반으로 직접 진입)
            fake_decision = {
                "buy": True,
                "reason": f"MACD 재진입 (from_neg+buy_pre+vol↑)",
                "target_pct": 3.0,
                "stop_pct": 1.5,
                "rsi": rsi,
                "signal_type": item.get("signal_type", "momentum"),
                "market_score": 0.0,
            }
            result = self._place_buy(
                ticker=ticker,
                name=item.get("name", ""),
                quantity=qty,
                current_price=current_price,
                tranche=1,
                decision=fake_decision,
                tight_stop=True,   # 재진입은 항상 타이트한 손절
            )
            if result:
                orders.append(result)
                watch["reentry_count"] += 1
                self._today_tickers.add(ticker)
                logger.info(f"[MACD 재진입 완료] {ticker} {qty}주 @ {current_price:,.0f}")

        for tk in to_remove:
            self._reentry_watchlist.pop(tk, None)

        return orders

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
            mscore        = item.get("momentum_score") or 0.0
            drp           = item.get("day_range_pos") or 0.5
            obv           = item.get("obv_slope") or 0.0
            entry_score   = item.get("_entry_score") or 0.0
            macd_str      = item.get("_macd_strength") or 50.0
            exec_st       = item.get("exec_strength") or 100.0
            ob_imbalance  = item.get("_ob_imbalance") or 1.0
            short_ratio   = item.get("_short_ratio") or 0.0
            is_squeeze    = item.get("_squeeze_candidate", False)
            squeeze_tag   = " ⚡쇼트스퀴즈후보" if is_squeeze else ""
            rs_daily      = float(item.get("rs_daily") or 0.0)
            rs_5d         = float(item.get("rs_5d") or 0.0)
            sector        = item.get("sector") or "기타"
            frgn_net      = int(item.get("frgn_net_buy") or 0)
            inst_net      = int(item.get("inst_net_buy") or 0)
            # 수급 태그
            if frgn_net > 0 and inst_net > 0:
                _supply_tag = f" 💰외인+기관동시매수(외인{frgn_net:+,}/기관{inst_net:+,})"
            elif frgn_net > 0:
                _supply_tag = f" 외인매수{frgn_net:+,}"
            elif inst_net > 0:
                _supply_tag = f" 기관매수{inst_net:+,}"
            elif frgn_net < 0 and inst_net < 0:
                _supply_tag = f" ⚠️외인+기관동시매도(외인{frgn_net:+,}/기관{inst_net:+,})"
            else:
                _supply_tag = ""
            # 종목 과거 통계
            _ts = _load_ticker_stats(ticker)
            _ts_tag = ""
            if _ts:
                _ts_tag = (
                    f"\n  [과거통계] {_ts['total_trades']}회 | "
                    f"승률{_ts['win_rate']:.0%} | "
                    f"평균손익{_ts['avg_pnl_pct']:+.1f}% | "
                    f"평균보유{_ts['avg_hold_minutes']:.0f}분"
                )
                if _ts.get("best_signal_type"):
                    _ts_tag += f" | 최적신호:{_ts['best_signal_type']}"
                if _ts.get("notes"):
                    _ts_tag += f" | {_ts['notes']}"
            # 섹터 강/약세 태그
            try:
                from src.infra.sector_rotation import get_hot_sectors, get_cold_sectors
                _sector_tag = " 🔥강세섹터" if sector in get_hot_sectors(3) else (
                              " 🧊약세섹터" if sector in get_cold_sectors(3) else ""
                )
            except Exception:
                _sector_tag = ""
            # ATR 기반 제안 손절가 계산 (Claude 참고용)
            _atr = float(item.get("atr_pct") or 0.0)
            _sig = item.get("signal_type", "")
            if _atr > 0:
                _atr_mult = 1.2 if _sig in ("pullback_rebound", "opening_plunge_rebound") else 1.5
                _atr_stop_suggest = round(max(1.0, min(3.5, _atr * _atr_mult)), 1)
                _atr_tag = f" | ATR({_atr:.2f}%)→제안손절{_atr_stop_suggest}%"
            else:
                _atr_tag = ""

            stock_lines.append(
                f"- 티커: {ticker} ({item.get('name', '')})\n"
                f"  신호: {_sig} | "
                f"등락: {item.get('price_change_pct', 0):+.1f}% | "
                f"거래량: {item.get('volume_ratio', 0):.1f}배 | "
                f"RSI: {item.get('rsi', 50):.0f} | "
                f"진입신뢰도: {entry_score:.0f}/100 | "
                f"MACD강도: {macd_str:.0f}/100 | "
                f"체결강도: {exec_st:.0f} | "
                f"호가불균형: {ob_imbalance:.2f} | "
                f"공매도비율: {short_ratio:.1f}%{squeeze_tag}\n"
                f"  모멘텀점수: {mscore:.0f}/130 | "
                f"당일범위위치: {drp:.2f} | "
                f"OBV기울기: {'↑' if obv > 0 else '↓'}{obv:+.2f}{_atr_tag}\n"
                f"  RS당일: {rs_daily:+.2f}% | RS5일: {rs_5d:+.2f}% | "
                f"섹터: {sector}{_sector_tag}{_supply_tag}\n"
                f"  선정근거: {item.get('reason', '')} | "
                f"감성: {sentiment.get('avg_score', 0):+.2f}({sentiment.get('direction', 'neutral')})"
                + _ts_tag
            )

        user_content = (
            f"## 현재 매크로 컨텍스트\n"
            f"- 리스크 레벨: {risk_level}/5\n"
            f"- 글로벌 리스크: {global_risk_score}/10\n"
            f"- 국내 시황 점수: {market_score:+.2f}\n"
            f"- 매수가능시간: 오전(09~12시) 전용 — 수급 활발 구간에서만 진입 허용\n\n"
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
                    d["atr_pct"] = meta.get("atr_pct", 0.0)  # ATR 손절 계산용
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
        tranche_limit: int = 3,   # 최대 분할 횟수 (마감 구간: 1로 제한)
    ) -> None:
        """2차(35%)·3차(25%) 분할 매수를 별도 스레드에서 지연 실행."""

        def _execute_tranches():
            tranches = [(2, _TRANCHE_RATIOS[1]), (3, _TRANCHE_RATIOS[2])]
            for tranche_no, ratio in tranches:
                if tranche_no > tranche_limit:
                    logger.info(f"분할매수 {tranche_no}차 스킵: tranche_limit={tranche_limit} (시간대 제한)")
                    break
                # 5분 대기 후 현재가 확인
                time.sleep(300)
                if self._stop_event.is_set():
                    break

                # 12:00 이후 추가 분할매수 차단 (오전 타이밍 종료)
                _now_hm = datetime.now().hour * 100 + datetime.now().minute
                if _now_hm >= _TIME_BUY_CUTOFF:
                    logger.info(f"분할매수 {tranche_no}차 중단: 12:00 이후 추가 진입 차단")
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
        item_ctx: dict | None = None,  # hot_list 신호 메타데이터 (자기학습용)
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

            trade_id = execute(
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

            # 1차 매수 시 신호 컨텍스트 저장 (자기학습 피드백 루프)
            if tranche == 1 and item_ctx:
                _now_hm = datetime.now().strftime("%H%M")
                try:
                    execute(
                        """
                        INSERT OR IGNORE INTO trade_context
                            (trade_id, ticker, trade_date, signal_type, rsi, entry_score,
                             momentum_score, rs_daily, rs_5d, sector, exec_strength,
                             ob_imbalance, entry_hhmm)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            trade_id,
                            ticker,
                            str(date.today()),
                            item_ctx.get("signal_type", ""),
                            float(item_ctx.get("rsi") or 50.0),
                            float(item_ctx.get("_entry_score") or 0.0),
                            float(item_ctx.get("momentum_score") or 0.0),
                            float(item_ctx.get("rs_daily") or 0.0),
                            float(item_ctx.get("rs_5d") or 0.0),
                            item_ctx.get("sector", ""),
                            float(item_ctx.get("exec_strength") or 100.0),
                            float(item_ctx.get("_ob_imbalance") or 1.0),
                            _now_hm,
                        ),
                    )
                except Exception as _ctx_err:
                    logger.debug(f"trade_context 저장 실패 [{ticker}]: {_ctx_err}")

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
                    atr_pct=decision.get("atr_pct", 0.0),
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

def _compute_entry_score(
    c: dict,
    is_gap_up: bool,
    is_pullback: bool,
    is_mkt_mom: bool,
    is_op_plunge: bool,
    _hm: int,
    _min_vol: float,
    _max_rsi_hard: float,
    _max_rsi_soft: float,
    _min_rsi: float,
    ticker_stats: dict | None = None,
    signal_feedback: dict | None = None,
    sector_holdings: int = 0,
) -> tuple[list[str], float, float]:
    """
    진입 신뢰도 점수 (0~110) + 하드 차단 사유 + 사이즈 배율.

    하드 차단(hard_fails) 있으면 score/size_mult 무관 진입 불가.

    점수 구성 (합계 최대 110pt → 100pt 캡):
      거래량    0~30pt — 거래량비 크기별 차등
      RSI 구간  0~20pt — 과열일수록 감점
      OBV 기울기 0~20pt — 방향·강도 반영
      StochRSI  0~15pt — 단기 과매수 감점
      모멘텀/BB 0~15pt — 모멘텀점수·BB폭 확대 보정
      체결강도  0~10pt — 심리 파도 (매수세 강도 직접 반영)

    사이즈 배율:
      score ≥ 72 → 1.00 (풀사이즈)
      score 50~71 → 0.75 (축소 진입)
      score < 50  → 차단 (hard fail과 동일)

    Returns: (hard_fails, score, size_mult)
    """
    hard_fails: list[str] = []

    vol_ratio    = float(c.get("volume_ratio") or 0.0)
    price_chg    = float(c.get("price_change_pct") or 0.0)
    rsi          = float(c.get("rsi") or 50.0)
    obv_slope    = float(c.get("obv_slope") or 0.0)
    stoch_rsi    = float(c.get("stoch_rsi") or 50.0)
    bb_ratio     = float(c.get("bb_width_ratio") or 1.0)
    mscore       = float(c.get("momentum_score") or 0.0)
    exec_strength = float(c.get("exec_strength") or 100.0)

    # ── HARD FAILS (전략/시간 무관 절대 차단) ──────────────────
    if is_op_plunge and _hm >= 1030:
        hard_fails.append(f"오프닝 급락 10:30 이후 차단 ({_hm // 100:02d}:{_hm % 100:02d})")
        return hard_fails, 0.0, 0.0

    if is_gap_up and _hm >= 1300:
        hard_fails.append(f"갭업 13:00 이후 진입 차단 ({_hm // 100:02d}:{_hm % 100:02d})")
        return hard_fails, 0.0, 0.0

    # RSI 극단 과열
    _rsi_hard_eff = 95.0 if (is_gap_up and obv_slope > 0) else _max_rsi_hard
    if rsi > _rsi_hard_eff:
        hard_fails.append(f"RSI {rsi:.0f} > {_rsi_hard_eff:.0f} (극단 과열)")
        return hard_fails, 0.0, 0.0

    # 과매도 붕괴 (눌림/오프닝급락 면제)
    if rsi < _min_rsi and not (is_pullback or is_op_plunge):
        hard_fails.append(f"RSI {rsi:.0f} < {_min_rsi:.0f} (과매도 붕괴)")
        return hard_fails, 0.0, 0.0

    # 거래량 절대 최소 (전략별 완화)
    _vol_floor = 1.2 if (is_gap_up or is_pullback or is_mkt_mom or is_op_plunge) else max(1.5, _min_vol * 0.75)
    if vol_ratio < _vol_floor:
        hard_fails.append(f"거래량비 {vol_ratio:.1f}x < {_vol_floor:.1f}x (최소 기준 미달)")
        return hard_fails, 0.0, 0.0

    # 하락 종목 차단 (눌림/오프닝급락 면제)
    if price_chg <= 0 and not (is_pullback or is_op_plunge):
        hard_fails.append(f"등락률 {price_chg:+.2f}% ≤ 0")
        return hard_fails, 0.0, 0.0

    # OBV 역행 + 고RSI (갭업/시장강세 면제)
    if obv_slope < 0 and rsi > 70 and not (is_gap_up or is_mkt_mom):
        hard_fails.append(f"OBV 역행+고RSI {rsi:.0f} (수급 없는 상승)")
        return hard_fails, 0.0, 0.0

    # StochRSI 극단 과매수 (갭업/오프닝급락 면제)
    if stoch_rsi > 88.0 and not (is_gap_up or is_op_plunge):
        hard_fails.append(f"StochRSI {stoch_rsi:.0f} > 88 (단기 극과매수)")
        return hard_fails, 0.0, 0.0

    # 체결강도 급락 — 매도세 압도 (눌림목·오프닝급락은 하락 체결 정상이므로 면제)
    if exec_strength < 75.0 and not (is_pullback or is_op_plunge):
        hard_fails.append(f"체결강도 {exec_strength:.0f} < 75 (매도세 압도 — 심리 이탈)")
        return hard_fails, 0.0, 0.0

    # 갭업 BB폭 미확대
    if is_gap_up and bb_ratio < 1.0:
        hard_fails.append(f"BB폭 미확대 {bb_ratio:.2f} < 1.0 (가짜 브레이크아웃)")
        return hard_fails, 0.0, 0.0

    # 고가권 추격 (갭업+OBV양수 또는 오프닝급락 면제)
    day_range_pos = float(c.get("day_range_pos") or 0.5)
    if price_chg >= 3.0 and day_range_pos >= 0.90 and obv_slope <= 0:
        if not (is_gap_up or is_op_plunge):
            hard_fails.append(
                f"고가권 추격 ({price_chg:+.1f}%, 범위위치 {day_range_pos:.2f}, OBV↓)"
            )
            return hard_fails, 0.0, 0.0

    # ── SCORED CONDITIONS (연속형 점수화) ──────────────────────
    score = 0.0

    # 거래량 (0~30pt) — 더 많을수록 고점수, 거래대금도 보조 반영
    if vol_ratio >= 5.0:
        score += 30.0
    elif vol_ratio >= 3.0:
        score += 22.0
    elif vol_ratio >= 2.0:
        score += 16.0
    elif vol_ratio >= 1.5:
        score += 10.0
    else:
        score += 5.0  # 1.2~1.5x — 최소 통과, 낮은 신뢰도

    trading_value = int(c.get("trading_value") or 0)
    if trading_value >= 200_000_000_000:   # 2000억↑
        score += 3.0
    elif trading_value >= 50_000_000_000:  # 500억↑
        score += 2.0
    elif trading_value >= 10_000_000_000:  # 100억↑
        score += 1.0

    # RSI 구간 (0~20pt) — 이상적 진입은 45~65
    if rsi < 45:
        score += 14.0  # 과매도 근처 — 반등 여지 크지만 붕괴 위험도
    elif rsi < 55:
        score += 20.0  # 최적 구간
    elif rsi < 65:
        score += 16.0
    elif rsi < 72:
        score += 11.0
    elif rsi <= _max_rsi_soft:
        score += 6.0   # 72~82 과열 구간
    else:
        score += 2.0   # 82~95 (갭업+OBV 예외로 통과한 경우)

    # OBV 기울기 (0~20pt) — 방향·강도 반영
    if obv_slope > 0.5:
        score += 20.0
    elif obv_slope > 0.1:
        score += 15.0
    elif obv_slope > 0:
        score += 10.0
    elif obv_slope > -0.1:
        score += 5.0
    else:
        score += 0.0  # 강한 OBV 하락

    # StochRSI (0~15pt) — 단기 모멘텀 여유
    if stoch_rsi < 30:
        score += 15.0
    elif stoch_rsi < 50:
        score += 12.0
    elif stoch_rsi < 65:
        score += 9.0
    elif stoch_rsi < 75:
        score += 6.0
    elif stoch_rsi < 85:
        score += 3.0
    else:
        score += 0.0

    # 모멘텀점수 + BB폭 (0~15pt)
    score += min(8.0, mscore / 130.0 * 8.0)  # 모멘텀 최대 8pt
    if is_gap_up:
        if bb_ratio >= 1.5:
            score += 7.0
        elif bb_ratio >= 1.2:
            score += 5.0
        elif bb_ratio >= 1.0:
            score += 2.0
    else:
        score += min(7.0, (bb_ratio - 1.0) * 14.0)  # BB폭 확대 비례

    # 체결강도 (0~10pt) — 군중 심리 파도 직접 반영
    # 130↑: 강한 FOMO 매수세 → 최대 가산 / 75~100: 중립~약세 → 감점
    if exec_strength >= 150.0:
        score += 10.0
    elif exec_strength >= 130.0:
        score += 8.0
    elif exec_strength >= 115.0:
        score += 5.0
    elif exec_strength >= 100.0:
        score += 3.0
    elif exec_strength >= 90.0:
        score += 1.0
    else:
        score += 0.0   # 75~90: 통과했지만 매수세 약함

    # 상대강도 RS (±8pt) — 하락장 하락 종목 차단 + 강세 종목 가산
    rs_daily = float(c.get("rs_daily") or 0.0)
    rs_5d    = float(c.get("rs_5d") or 0.0)
    # 하락장(KOSPI -1.5% 이하) + rs_daily < -2.0 = 시장보다 더 빠지는 종목 → 차단
    # (kospi_daily_chg 직접 참조 대신 price_chg + rs_daily 역산으로 추정)
    _implied_kospi = price_chg - rs_daily
    if _implied_kospi <= -1.5 and rs_daily < -2.0 and not (is_pullback or is_op_plunge):
        hard_fails.append(f"하락장 RS 역행 rs_daily={rs_daily:+.1f}% (KOSPI 추정 {_implied_kospi:+.1f}%)")
        return hard_fails, 0.0, 0.0
    if rs_daily >= 3.0:
        score += 8.0   # 강한 RS 초과 — KOSPI보다 3%↑
    elif rs_daily >= 1.5:
        score += 5.0
    elif rs_daily >= 0.5:
        score += 3.0
    elif rs_daily >= -0.5:
        score += 1.0
    else:
        score -= 2.0   # RS 역행 감점 (hard fail 미만)
    # rs_5d 추세 확인 가산 (최대 +3pt): 5일 누적 강세면 추세 지속 신뢰도 상승
    if rs_5d >= 5.0:
        score += 3.0
    elif rs_5d >= 2.0:
        score += 1.5
    score = max(0.0, score)

    # 섹터 보너스/패널티 (±5pt) — hot/cold 섹터 판단
    sector = str(c.get("sector") or "")
    if sector:
        try:
            from src.infra.sector_rotation import get_hot_sectors, get_cold_sectors
            _hot = get_hot_sectors(3)
            _cold = get_cold_sectors(3)
            if sector in _hot:
                score += 5.0   # 강세 섹터 가산
            elif sector in _cold:
                score -= 3.0   # 약세 섹터 감점
                score = max(0.0, score)
        except Exception:
            pass

    # 외인·기관 수급 (±8pt) — 동시 매수 = 가장 강한 확인 신호
    frgn_net = int(c.get("frgn_net_buy") or 0)
    inst_net  = int(c.get("inst_net_buy") or 0)
    if frgn_net > 0 and inst_net > 0:
        score += 8.0   # 외인+기관 동시 매수 — 세력 매집 확인
    elif frgn_net > 0:
        score += 4.0   # 외인 단독 매수
    elif inst_net > 0:
        score += 3.0   # 기관 단독 매수
    elif frgn_net < 0 and inst_net < 0:
        score -= 5.0   # 외인+기관 동시 매도 — 세력 이탈 경고
        score = max(0.0, score)

    score = min(100.0, score)

    # 과거 거래 통계 반영 (±3pt) — 같은 종목 검증된 패턴
    if ticker_stats and ticker_stats.get("total_trades", 0) >= 3:
        wr  = float(ticker_stats.get("win_rate", 0.5))
        apnl = float(ticker_stats.get("avg_pnl_pct", 0.0))
        if wr >= 0.65 and apnl > 0:
            score += 3.0   # 검증된 승률 종목 — 신뢰도 보강
        elif wr < 0.30 or apnl < -0.5:
            score -= 4.0   # 반복 패배 종목 — 기술적 신호에도 불구 불리
            score = max(0.0, score)

    # 신호 유형 피드백 루프 (±3pt) — 최근 N일 성과 기반 자기학습
    if signal_feedback:
        sig_type = str(c.get("signal_type") or "momentum")
        fb = signal_feedback.get(sig_type)
        if fb and fb.get("n", 0) >= 5:
            exp = float(fb.get("expectancy", 0.0))
            wr_fb = float(fb.get("win_rate", 0.5))
            if exp > 0.5 and wr_fb >= 0.60:
                score += 3.0   # 최근 N일 해당 신호 기대값 양호
            elif exp < -0.3 or wr_fb < 0.35:
                score -= 3.0   # 최근 N일 해당 신호 기대값 부진
                score = max(0.0, score)

    # 섹터 집중 리스크 패널티 (0 ~ -10pt)
    # 동일 섹터 보유가 많을수록 추가 진입 시 섹터 리스크 집중 → 점수 차감
    if sector_holdings >= 2:
        score -= 10.0  # 동일 섹터 2종목 이상 보유 — 집중 리스크 경고
        score = max(0.0, score)
    elif sector_holdings == 1:
        score -= 4.0   # 동일 섹터 1종목 보유 — 분산 권고
        score = max(0.0, score)

    score = min(100.0, score)

    # 시간대 가중치 (±5pt) — 수급 활동 밀도 반영
    # 오프닝·전반전: 하루 중 가장 강한 수급 활동 → 신호 신뢰도 최고
    # 점심: 수급 공백, 마감 근접: 청산 물량 출회 위험
    if 900 <= _hm < 1030:
        score += 5.0   # 오프닝 황금시간 — 갭업·급락·수급 집중
    elif 1030 <= _hm < 1130:
        score += 2.0   # 전반전 후반 — 추세 확립 구간
    elif 1130 <= _hm < 1330:
        score -= 2.0   # 점심 시간대 — 수급 공백, 허수 신호 증가
        score = max(0.0, score)
    elif _hm >= 1400:
        score -= 5.0   # 마감 근접 — 세력 청산 출회, 신규 진입 위험
        score = max(0.0, score)

    # best_entry_hour 가중치 (±5pt) — 종목별 과거 최고 성과 시간대
    # 이 종목이 특정 시간대에 잘 움직이는 패턴이 있으면 해당 시간에 가산
    _best_h = ticker_stats.get("best_entry_hour") if ticker_stats else None
    if _best_h is not None:
        _cur_hour = _hm // 100
        _hour_diff = abs(_cur_hour - int(_best_h))
        if _hour_diff <= 1:
            score += 5.0   # 최적 진입 시간대 ±1시간 이내 → 신뢰도 상향
        elif _hour_diff >= 3:
            score -= 3.0   # 최적 시간대와 3시간 이상 차이 → 패턴 불일치
            score = max(0.0, score)

    score = min(100.0, score)

    # 사이즈 배율
    if score >= 72.0:
        size_mult = 1.00
    elif score >= 50.0:
        size_mult = 0.75
    else:
        hard_fails.append(f"신뢰도 점수 {score:.0f}/100 < 50 (복합 신호 부족)")
        return hard_fails, score, 0.0

    return hard_fails, score, size_mult


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


def _get_vwap_position(ticker: str) -> tuple[float, float]:
    """
    당일 intraday_candles에서 VWAP 계산.
    Returns: (vwap, current_price) — 데이터 부족 시 (0.0, 0.0)
    """
    rows = fetch_all(
        """
        SELECT high, low, close, volume
        FROM intraday_candles
        WHERE ticker = ?
          AND bar_time >= '090000'
        ORDER BY bar_time ASC
        """,
        (ticker,),
    )
    if len(rows) < 3:
        return 0.0, 0.0
    cum_pv = 0.0
    cum_vol = 0.0
    for r in rows:
        typical = (float(r["high"]) + float(r["low"]) + float(r["close"])) / 3
        vol = float(r["volume"])
        cum_pv += typical * vol
        cum_vol += vol
    if cum_vol == 0:
        return 0.0, 0.0
    vwap = cum_pv / cum_vol
    current_price = float(rows[-1]["close"])
    return vwap, current_price


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
    """
    최근 10분 이내 hot_list에서 종목별 최신 레코드만 반환 (momentum_score 내림차순).

    같은 종목이 여러 사이클에 올라온 경우 가장 최근 레코드만 사용
    (GROUP BY ticker + MAX(created_at)) — 중복 Gate 처리 방지.
    """
    rows = fetch_all(
        """
        SELECT h.ticker, h.name, h.signal_type, h.volume_ratio, h.price_change_pct, h.rsi, h.reason,
               COALESCE(h.momentum_score, 0.0) AS momentum_score,
               COALESCE(h.obv_slope, 0.0) AS obv_slope,
               COALESCE(h.day_range_pos, 0.5) AS day_range_pos,
               COALESCE(h.stoch_rsi, 50.0) AS stoch_rsi,
               COALESCE(h.bb_width_ratio, 1.0) AS bb_width_ratio,
               COALESCE(h.trading_value, 0) AS trading_value,
               COALESCE(h.exec_strength, 100.0) AS exec_strength,
               COALESCE(h.rs_daily, 0.0) AS rs_daily,
               COALESCE(h.rs_5d, 0.0) AS rs_5d,
               COALESCE(h.sector, '') AS sector,
               COALESCE(h.frgn_net_buy, 0) AS frgn_net_buy,
               COALESCE(h.inst_net_buy, 0) AS inst_net_buy,
               COALESCE(h.atr_pct, 0.0) AS atr_pct,
               COALESCE(h.slot, '') AS slot
        FROM hot_list h
        INNER JOIN (
            SELECT ticker, MAX(created_at) AS latest
            FROM hot_list
            WHERE created_at >= datetime('now', '-10 minutes')
            GROUP BY ticker
        ) latest_only ON h.ticker = latest_only.ticker AND h.created_at = latest_only.latest
        ORDER BY COALESCE(h.momentum_score, 0.0) DESC, h.volume_ratio DESC
        LIMIT 10
        """
    )
    return [dict(r) for r in rows]


def _load_ticker_stats(ticker: str) -> dict | None:
    """ticker_stats 테이블에서 종목 과거 통계 조회. 없으면 None."""
    try:
        row = fetch_one(
            """
            SELECT total_trades, win_rate, avg_pnl_pct,
                   COALESCE(avg_win_pct, 0.0) AS avg_win_pct,
                   COALESCE(avg_loss_pct, 0.0) AS avg_loss_pct,
                   avg_hold_minutes, best_entry_hour,
                   best_signal_type, notes, frgn_buy_win_rate, inst_buy_win_rate
            FROM ticker_stats WHERE ticker = ?
            """,
            (ticker,),
        )
        return dict(row) if row and row["total_trades"] > 0 else None
    except Exception:
        return None


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
    atr_pct: float = 0.0,
) -> tuple[float, float, float]:
    """
    ATR 기반 트레일링 스톱 파라미터 동적 산출.

    Returns:
        (initial_stop_pct, trigger_pct, floor_pct)
        - initial_stop_pct: 진입 직후 초기 손절선 간격 (%)
        - trigger_pct: 이 수익률 도달 시 트레일링 시작 (%)
        - floor_pct: 트레일링 손절선 간격 (현재가 대비 %)

    손절 산출 원리 (ATR 기반):
        눌림·오프닝급락: ATR × 1.2 (이미 하락 중 — 타이트하게 대응)
        갭업·거래량폭발: ATR × 1.5 (추세 지속 중 — 노이즈 여유 필요)
        일반 모멘텀:     ATR × 1.5
        상한: 3.5% / 하한: 1.0% (param_tuner 오버라이드 가능)
    """
    from src.teams.research.param_tuner import get_param as _gp
    _stop_min = _gp("initial_stop_min_pct", 1.0)
    _stop_max = _gp("initial_stop_max_pct", 3.5)

    # ── 초기 손절선: ATR primary ────────────────────────────────
    if atr_pct and atr_pct > 0:
        # 전략별 ATR 배수: 눌림·급락은 이미 하락 중이므로 더 타이트하게
        _is_pullback = signal_type in ("pullback_rebound", "opening_plunge_rebound")
        atr_mult = 1.2 if _is_pullback else 1.5
        atr_stop = round(max(_stop_min, min(_stop_max, atr_pct * atr_mult)), 2)

        # Claude 제시값이 ATR보다 더 타이트하면 존중
        # (Claude가 "이건 1%에 끊어야 해"라고 하면 ATR 기준보다 우선)
        if stop_pct and stop_pct < atr_stop:
            initial_stop = max(_stop_min, stop_pct)
        else:
            initial_stop = atr_stop
    else:
        # ATR 없으면 Claude 제시값 또는 기본값
        _stop_base = _gp("initial_stop_pct", 2.0)
        initial_stop = max(_stop_min, min(_stop_max, stop_pct if stop_pct else _stop_base))

    # RSI 과열권 → 추가 타이트
    if rsi > 65:
        initial_stop = min(initial_stop, round(_stop_min + 0.3, 1))
    # 하락장 → 손실 최소화
    if market_score < -0.1:
        initial_stop = min(initial_stop, round(_stop_min + 0.2, 1))

    initial_stop = round(max(_stop_min, min(_stop_max, initial_stop)), 1)

    # ── 트레일링 시작 수익률 ─────────────────────────────────
    trigger_pct = max(2.0, target_pct * 0.5)
    if signal_type == "breakout":
        trigger_pct = max(2.5, trigger_pct)
    elif signal_type == "volume_surge" and volume_ratio > 5:
        trigger_pct = max(1.5, trigger_pct * 0.7)

    # ── 트레일링 간격 (ATR 기반) ─────────────────────────────
    if atr_pct and atr_pct > 0:
        # 실제 변동폭의 2배 = 노이즈 제거 후 추세 이탈 감지
        floor_pct = round(max(1.5, min(3.5, atr_pct * 2.0)), 1)
    elif rsi > 65 or volume_ratio > 5:
        floor_pct = 2.0
    elif volume_ratio > 3:
        floor_pct = 2.5
    else:
        floor_pct = 3.0

    if market_score < -0.1:
        floor_pct = min(floor_pct, 2.0)

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
