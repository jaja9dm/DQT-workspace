"""
engine.py — 매매팀 메인 엔진

역할:
  Hot List 종목을 대상으로 다단계 게이트를 통과한 경우에만
  Claude에 최종 매수 판단을 요청하고 KIS API로 주문을 실행한다.

게이트 구조 (순서대로, 하나라도 실패 시 진입 차단):
  Gate 0. 장 시작 오프닝 게이트 — 9:00 즉시 매수 vs 9:10 대기 판단 (Claude)
  Gate 1. 리스크 레벨 — Level 4↑이면 신규 진입 제한
  Gate 2. 글로벌 시황 — korea_market_outlook == 'negative'이면 차단
  Gate 3. 국내 시황 — market_score < -0.3이면 차단
  Gate 4. Hot List — DB에서 최신 Hot List 읽기
  Gate 5. Claude 최종 판단 — 매수 여부 + 예상 목표가·손절가

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
from datetime import date, datetime

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

## 매수 기준 (buy: true)

### 적극 매수 신호 (하나 이상이면 매수 강력 권고)
- volume_surge 신호 + 당일 등락 +1%↑ + RSI 45~70: 거래량·가격 동반 상승
- breakout 신호 + RSI 60↑ + 감성 neutral 이상: 볼린저밴드 돌파 모멘텀
- momentum 신호 + 거래량 3배↑ + 당일 등락 +0.5%↑: 복합 모멘텀

### 조건부 매수 (아래 항목 과반 충족 시)
- RSI 50~65 (상승 초입, 과열 아님)
- 당일 등락 0% 이상 (최소한 보합 이상)
- 거래량 1.5배↑
- 감성 점수 -0.2 이상 (매우 부정적이지 않으면 허용)

## 매수 보류 기준 (buy: false)
- RSI 75 초과: 단기 과열 — 고점 매수 위험
- 당일 등락 +5% 초과: 추격 매수 지양 (급등 후 조정 가능)
- 글로벌 리스크 8 이상: 매크로 불확실성 — 신규 진입 자제
- 감성 점수 -0.5 이하: 강한 부정 뉴스 존재 시

## 목표 수익률·손절 기준 산정 (종목별 동적 결정)
- target_pct: 신호 강도에 따라 동적 제시
  - volume_surge + RSI 50~65: 6~8%
  - breakout 신호: 7~10%
  - momentum 약한 신호: 4~5%
  - 글로벌 리스크 6↑ 또는 시황 낮음: 목표 축소 (-1~2%)
- stop_pct: 종목 특성에 따라 동적 결정 (시스템이 트레일링 파라미터로 활용)
  - RSI 65↑ (과열): 1.5% — 빠른 반전 대비
  - breakout 신호: 2.5% — 돌파 후 조정 허용
  - momentum/기본: 2.0%
  - 하락장 (시황 낮음): 1.5%

## 운영 방침
실매매 알고리즘입니다. 수익 가능성이 높은 경우에만 buy: true를 선택하세요.
아래 조건 중 하나라도 해당하면 보류:
- RSI 70 초과
- 당일 등락 +4% 초과 (급등 추격)
- 글로벌 리스크 7 이상
- 국내 시황 점수 -0.2 이하
- 감성 점수 -0.3 이하

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

        # 장 종료 전 신규 매수 차단 (15:20 이후 — 장마감 청산 시간대)
        _hm = now.hour * 100 + now.minute
        if _hm >= 1520:
            logger.debug(f"장마감 시간대 ({now.strftime('%H:%M')}) — 신규 매수 차단")
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
        max_pos = int(get_param("max_positions", 5.0))
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
        from src.teams.intraday_macd.engine import get_latest_macd_signal
        candidates = []
        for h in hot_list:
            ticker = h["ticker"]
            if ticker not in self._today_tickers:
                candidates.append(h)
            elif ticker in self._macd_reentry_ok:
                # 재진입 허용 종목: MACD buy_pre 신호 확인 후 진입
                macd_sig = get_latest_macd_signal(ticker, max_age_minutes=6)
                if macd_sig == "buy_pre" and not _has_open_position(ticker):
                    logger.info(f"[MACD 재진입] {ticker} — MACD buy_pre 복귀, 재매수 허용")
                    self._today_tickers.discard(ticker)   # 재진입 허용
                    self._macd_reentry_ok.discard(ticker)
                    candidates.append(h)

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
        batch = candidates[:3]   # 1회 최대 3종목
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

            # 1주당 금액 조회
            current_price = _fetch_current_price(ticker)
            if current_price <= 0:
                continue

            # 종목당 투자 한도
            max_invest = usable_cash * max_single_pct / 100

            # 분할 매수 1차 (40%)
            tranche1_amt = max_invest * _TRANCHE_RATIOS[0]
            qty = max(1, int(tranche1_amt / current_price))

            result = self._place_buy(
                ticker=ticker,
                name=item.get("name", ""),
                quantity=qty,
                current_price=current_price,
                tranche=1,
                decision=decision,
            )
            if result:
                orders.append(result)
                self._today_tickers.add(ticker)
                self._macd_reentry_ok.add(ticker)  # 이 종목은 MACD 손절 후 재진입 허용

                # 2차·3차 분할 매수 예약 (별도 스레드로 지연 실행)
                self._schedule_tranches(
                    ticker=ticker,
                    name=item.get("name", ""),
                    entry_price=current_price,
                    max_invest=max_invest,
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
                max_tokens=128,
                temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
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
            stock_lines.append(
                f"- 티커: {ticker} ({item.get('name', '')})\n"
                f"  신호: {item.get('signal_type', '')} | "
                f"등락: {item.get('price_change_pct', 0):+.1f}% | "
                f"거래량: {item.get('volume_ratio', 0):.1f}배 | "
                f"RSI: {item.get('rsi', 50):.0f}\n"
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
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
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
    """최근 10분 이내 hot_list 항목 반환."""
    rows = fetch_all(
        """
        SELECT ticker, name, signal_type, volume_ratio, price_change_pct, rsi, reason
        FROM hot_list
        WHERE created_at >= datetime('now', '-10 minutes')
        ORDER BY volume_ratio DESC
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
    # Claude가 제시한 stop_pct를 베이스로 RSI·시황 보정
    initial_stop = max(1.5, min(3.5, stop_pct))

    if rsi > 65:
        initial_stop = min(initial_stop, 1.5)   # 과열권 — 빠른 반전 대비 타이트
    elif rsi < 45:
        initial_stop = min(initial_stop + 0.5, 3.0)  # 약한 모멘텀 — 조금 여유

    if market_score < -0.1:
        initial_stop = min(initial_stop, 1.5)   # 하락장 — 손실 최소화 우선

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
