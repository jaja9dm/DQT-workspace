"""
engine.py — 매매팀 메인 엔진

역할:
  Hot List 종목을 대상으로 다단계 게이트를 통과한 경우에만
  Claude에 최종 매수 판단을 요청하고 KIS API로 주문을 실행한다.

게이트 구조 (순서대로, 하나라도 실패 시 진입 차단):
  Gate 1. 리스크 레벨 — Level 4↑이면 신규 진입 제한
  Gate 2. 글로벌 시황 — korea_market_outlook == 'negative'이면 차단
  Gate 3. 국내 시황 — market_score < -0.3이면 차단
  Gate 4. Hot List — DB에서 최신 Hot List 읽기
  Gate 5. Claude 최종 판단 — 매수 여부 + 예상 목표가·손절가

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
_KIS_BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
_KIS_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"

# 분할 매수 비율
_TRANCHE_RATIOS = [0.40, 0.35, 0.25]

# 게이트 임계값
_MARKET_SCORE_GATE = -0.3     # 국내 시황 최소 점수
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
        # ── Gate 1: 리스크 레벨 ─────────────────
        risk = get_current_risk()
        level = risk.get("risk_level", 1)
        position_limit_pct = risk.get("position_limit_pct", 100)
        max_single_pct = risk.get("max_single_trade_pct", 5.0)

        if level >= _RISK_LEVEL_GATE:
            logger.info(f"Gate 1 차단: 리스크 레벨 {level} — 신규 진입 금지")
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

        # ── Gate 4: Hot List ─────────────────────
        hot_list = _load_hot_list()
        if not hot_list:
            logger.debug("Gate 4: Hot List 비어있음 — 대기")
            return []

        # 이미 당일 매수한 종목 제외
        candidates = [h for h in hot_list if h["ticker"] not in self._today_tickers]
        if not candidates:
            return []

        # ── 가용 예수금 조회 ─────────────────────
        available_cash = _fetch_available_cash()
        if available_cash <= 0:
            logger.warning("예수금 부족 — 매수 불가")
            return []

        # 리스크 레벨에 따라 실제 사용 가능 예수금 제한
        usable_cash = available_cash * position_limit_pct / 100

        # ── Gate 5: Claude 최종 판단 ─────────────
        orders = []
        for item in candidates[:3]:   # 1회 최대 3종목
            ticker = item["ticker"]

            decision = self._ask_claude(
                item=item,
                market_score=market_score,
                global_risk_score=global_ctx.get("global_risk_score", 5),
                risk_level=level,
            )

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
    # Claude 매수 판단
    # ──────────────────────────────────────────

    def _ask_claude(
        self,
        item: dict,
        market_score: float,
        global_risk_score: int,
        risk_level: int,
    ) -> dict:
        """
        Claude에 종목 매수 여부 최종 판단 요청.

        Returns:
            {"buy": bool, "reason": str, "target_pct": float, "stop_pct": float}
        """
        ticker = item["ticker"]
        sentiment = _load_sentiment(ticker)

        prompt = f"""당신은 국내 주식 퀀트 트레이더입니다.
아래 정보를 종합하여 이 종목의 즉시 매수 여부를 판단하세요.

## 매크로 컨텍스트
- 리스크 레벨: {risk_level}/5
- 글로벌 리스크 점수: {global_risk_score}/10
- 국내 시황 점수: {market_score:+.2f} (-1.0 약세 ~ +1.0 강세)

## 종목 정보
- 티커: {ticker} ({item.get('name', '')})
- 신호 유형: {item.get('signal_type', '')}
- 당일 등락률: {item.get('price_change_pct', 0):+.1f}%
- 거래량 비율: {item.get('volume_ratio', 0):.1f}배 (평균 대비)
- RSI: {item.get('rsi', 50):.0f}
- 선정 근거: {item.get('reason', '')}

## 감성 분석 (최근 5건 평균)
- 감성 점수: {sentiment.get('avg_score', 0):+.2f} (-1.0~+1.0)
- 주요 방향: {sentiment.get('direction', 'neutral')}

## 판단 기준
- RSI 70 초과이면 과열 — 보수적으로 판단
- 거래량 급등 + 상승이 가장 강력한 신호
- 글로벌 리스크 7 이상이면 매수 자제
- 당일 이미 3% 이상 상승했으면 추격 매수 지양

## 응답 형식 (JSON만)
{{
  "buy": <true|false>,
  "reason": "<판단 근거 30자 이내>",
  "target_pct": <목표 수익률 %, 양수>,
  "stop_pct": <손절 기준 %, 양수>
}}"""

        try:
            response = _client.messages.create(
                model=settings.CLAUDE_MODEL_MAIN,
                max_tokens=256,
                temperature=settings.CLAUDE_TEMPERATURE,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            return json.loads(raw)
        except Exception as e:
            logger.error(f"Claude 매수 판단 오류 [{ticker}]: {e}")
            from src.utils.notifier import check_claude_error
            check_claude_error(e, f"매매팀 [{ticker}]")
            return {"buy": False, "reason": "Claude 오류", "target_pct": 5.0, "stop_pct": 5.0}

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
