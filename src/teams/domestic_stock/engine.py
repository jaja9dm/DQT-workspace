"""
engine.py — 국내 주식팀 메인 엔진

실행 주기: 장 중 5분마다 (09:00 ~ 15:30)
즉시 트리거 조건:
  - 개별 종목 거래량 급등 (평균 대비 5배 이상)
  - 개별 종목 가격 급등 (+5% 이상)
  - 글로벌 리스크 점수 7 이상 (위기 직전 — 스캔 강화)

수집 → Claude 슬롯 판단 → DB 저장 → 트리거 체크 순으로 실행.

슬롯 구조 (매일 3종목 상한):
  leader   — 주도섹터·주도주 (시장 대장주)
  breakout — 돌파매매 (갭업·박스 상단 돌파)
  pullback — 눌림목매매 (단기 눌림 후 반등)

타임라인:
  09:00~09:07 — 관찰 모드 (슬롯 배정 금지, 개장 변동성 안정 대기)
  09:07~11:30 — 슬롯 탐색·배정 가능
  11:30 이후  — 신규 슬롯 탐색 중단 (기존 포지션 관리만)
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime

from src.infra.database import execute, fetch_all, fetch_one
from src.teams.domestic_stock.analyzer import analyze
from src.teams.domestic_stock.collector import (
    PRICE_SURGE_PCT,
    VOLUME_SURGE_RATIO,
    StockSnapshot,
    UniverseScan,
    collect,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

_INTERVAL_SEC = 300              # 5분
_IMMEDIATE_VOL_RATIO = 5.0      # 즉시 트리거용 거래량 배율 (5배)
_IMMEDIATE_PRICE_PCT = 5.0      # 즉시 트리거용 가격 급등 (5%)

# 슬롯 타임라인
_SLOT_OPEN_HHMM   = 907   # 관찰 모드 종료 — 이 시각부터 슬롯 배정 가능
_SLOT_CUTOFF_HHMM = 1130  # 신규 슬롯 탐색 마감

# 매매팀 Gate 1~3과 동일한 임계값
_GATE_RISK_LEVEL_MAX = 4
_GATE_MARKET_SCORE_MIN = -0.3

_ALL_SLOTS = ("leader", "breakout", "pullback")


class DomesticStockEngine:
    """국내 주식팀 엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._force_rescan = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="domestic-stock-engine",
        )

    def start(self) -> None:
        logger.info("국내 주식팀 엔진 시작")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._force_rescan.set()
        self._thread.join(timeout=15)
        logger.info("국내 주식팀 엔진 종료")

    # ──────────────────────────────────────────
    # 메인 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"국내 주식팀 오류: {e}", exc_info=True)

            _hm = int(datetime.now().strftime("%H%M"))
            _wait = _INTERVAL_SEC * 2 if 1130 <= _hm < 1330 else _INTERVAL_SEC

            self._force_rescan.wait(timeout=_wait)
            if self._force_rescan.is_set() and not self._stop_event.is_set():
                logger.info("거래량 급등 감지 — 즉시 Hot List 재스캔")
            self._force_rescan.clear()

    def run_once(self) -> dict[str, dict | None]:
        """
        1회 실행: 수집 → 슬롯 판단 → DB 저장 → 트리거 체크.

        Returns:
            슬롯별 선정 결과 dict  {"leader": {...}|None, "breakout": ..., "pullback": ...}
        """
        hm = int(datetime.now().strftime("%H%M"))

        # 1. 컨텍스트 조회
        market_score    = _get_market_score()
        global_risk_score = _get_global_risk_score()

        # 2. 유니버스 스캔
        scan = collect()

        # 3. 즉시 트리거 경보
        self._check_immediate_alerts(scan)

        # 4. Gate 사전 체크 — 어차피 매매 불가 시황이면 스킵
        if _is_trading_blocked(market_score):
            logger.info("Hot List 분석 스킵 — 매매팀 게이트 차단 조건")
            return {s: None for s in _ALL_SLOTS}

        # 5. 관찰 모드 체크 (09:00~09:07)
        if hm < _SLOT_OPEN_HHMM:
            logger.info(f"관찰 모드 ({hm}) — 슬롯 배정 대기 중 (09:07 이후 시작)")
            return {s: None for s in _ALL_SLOTS}

        # 6. 활성 슬롯 건강 평가 — 11:30 이후에도 계속 실행 (오후 포지션 관리)
        _evaluate_active_slots(scan)

        # 7. 슬롯 탐색 마감 체크 (11:30 이후 — 신규 탐색만 중단)
        if hm >= _SLOT_CUTOFF_HHMM:
            logger.info("슬롯 탐색 마감 (11:30) — 기존 포지션 건강 관리만 유지")
            return {s: None for s in _ALL_SLOTS}

        # 8. 시장 주도주 갱신
        try:
            from src.infra.market_leaders import refresh as _refresh_leaders
            _refresh_leaders()
        except Exception as _le:
            logger.debug(f"주도주 갱신 실패: {_le}")

        # 9. 비어 있는 슬롯만 탐색
        slots_to_fill = _get_empty_slots()
        if not slots_to_fill:
            logger.info("모든 슬롯이 오늘 이미 배정됨 — 스킵")
            return {s: None for s in _ALL_SLOTS}

        # 10. Claude 슬롯 판단
        key_events  = _get_global_key_events()
        kospi_chg   = _get_kospi_chg_pct()
        slot_result = analyze(
            scan, market_score, global_risk_score,
            global_key_events=key_events,
            kospi_chg_pct=kospi_chg,
            slots_to_fill=slots_to_fill,
        )

        # 11. DB 저장 (슬롯별)
        saved = _save_slots(slot_result, scan)

        # 12. 섹터 로테이션 갱신
        try:
            from src.infra.sector_rotation import inject_scan_results
            inject_scan_results(scan.snapshots)
        except Exception as _e:
            logger.debug(f"섹터 로테이션 갱신 실패: {_e}")

        # 13. 뉴스 감성 분석 제출
        self._submit_ticker_sentiment(
            [v for v in slot_result.values() if v], scan
        )

        filled = [s for s, v in slot_result.items() if v]
        logger.info(f"슬롯 확정: {filled}")
        return slot_result

    # ──────────────────────────────────────────
    # 즉시 트리거
    # ──────────────────────────────────────────

    def _check_immediate_alerts(self, scan: UniverseScan) -> None:
        has_surge = False
        for snap in scan.snapshots:
            alerts = []
            if snap.volume_ratio >= _IMMEDIATE_VOL_RATIO:
                alerts.append(f"거래량 {snap.volume_ratio:.1f}배 급등")
            if snap.change_pct >= _IMMEDIATE_PRICE_PCT:
                alerts.append(f"가격 {snap.change_pct:+.1f}% 급등")

            if alerts:
                logger.warning(
                    f"[주식 경보] {snap.ticker}({snap.name}): "
                    + " / ".join(alerts)
                )
                has_surge = True

        if has_surge:
            self._force_rescan.set()

    # ──────────────────────────────────────────
    # 감성 분석 제출
    # ──────────────────────────────────────────

    def _submit_ticker_sentiment(
        self, hot_list: list[dict], scan: UniverseScan
    ) -> None:
        if not hot_list:
            return

        hot_tickers = {h["ticker"] for h in hot_list}
        snaps = {s.ticker: s for s in scan.snapshots}

        def _analyze_all():
            from src.infra.sentiment_cache import SentimentCache
            cache = SentimentCache()
            for ticker in hot_tickers:
                snap = snaps.get(ticker)
                if not snap:
                    continue
                try:
                    pass  # 뉴스 수집기 연동 시 추가
                except Exception as e:
                    logger.debug(f"감성 분석 제출 오류 [{ticker}]: {e}")

        t = threading.Thread(target=_analyze_all, daemon=True, name="stock-sentiment")
        t.start()


# ──────────────────────────────────────────────
# 슬롯 관리 헬퍼
# ──────────────────────────────────────────────

_SLOT_HEALTH_THRESHOLD = 45.0   # 이 점수 미만이면 교체 플래그
_SLOT_HEALTH_SAFE_PNL  = 2.5    # 수익이 이 % 이상이면 건강 점수 무관 유지 (트레일링 스톱에 맡김)
_SLOT_MIN_HOLD_MINUTES = 15     # 배정 후 이 시간 이내는 평가 보류


def _evaluate_active_slots(scan: UniverseScan) -> None:
    """
    5분마다 호출 — 보유 중인 슬롯의 종목을 재채점하고,
    건강 점수가 임계값 미만이면 replace_requested 플래그를 세운다.
    포지션 감시팀이 이 플래그를 보고 실제 매도를 실행한다.
    """
    today = date.today().isoformat()
    try:
        rows = fetch_all(
            """
            SELECT slot, ticker, assigned_at, health_score
            FROM slot_assignments
            WHERE trade_date = ? AND status = 'active' AND replace_requested = 0
            """,
            (today,),
        )
    except Exception:
        return

    if not rows:
        return

    snaps = {s.ticker: s for s in scan.snapshots}

    for row in rows:
        slot       = row["slot"]
        ticker     = row["ticker"]
        assigned_at_str = str(row["assigned_at"] or "")

        # 배정 직후는 평가 보류 (변동성 안정 대기)
        try:
            from datetime import datetime as _dt
            assigned_at = _dt.fromisoformat(assigned_at_str)
            held_min = (datetime.now() - assigned_at).total_seconds() / 60
            if held_min < _SLOT_MIN_HOLD_MINUTES:
                logger.debug(f"슬롯 건강 평가 보류 [{slot}/{ticker}] — 배정 후 {held_min:.0f}분 (최소 {_SLOT_MIN_HOLD_MINUTES}분 대기)")
                continue
        except Exception:
            pass

        snap = snaps.get(ticker)
        if snap is None:
            # 스캔 유니버스에 없는 종목 (상장폐지·서킷브레이커 등) → 즉시 교체 요청
            score = 0.0
            reason = "스캔 유니버스 이탈 — 유동성 상실 의심"
        else:
            score, reason = _score_slot_health(slot, ticker, snap)

        # DB에 건강 점수 갱신
        try:
            execute(
                "UPDATE slot_assignments SET health_score = ?, updated_at = CURRENT_TIMESTAMP WHERE slot = ? AND trade_date = ?",
                (score, slot, today),
            )
        except Exception:
            pass

        if score < _SLOT_HEALTH_THRESHOLD:
            logger.warning(
                f"[슬롯 건강 경고] {slot} / {ticker} — 점수 {score:.0f}점 "
                f"(임계값 {_SLOT_HEALTH_THRESHOLD:.0f}점) | {reason}"
            )
            try:
                execute(
                    """
                    UPDATE slot_assignments
                    SET replace_requested = 1,
                        replace_reason    = ?,
                        updated_at        = CURRENT_TIMESTAMP
                    WHERE slot = ? AND trade_date = ?
                    """,
                    (reason, slot, today),
                )
            except Exception:
                pass
        else:
            logger.debug(f"슬롯 건강 OK [{slot}/{ticker}] — {score:.0f}점")


def _score_slot_health(
    slot: str, ticker: str, snap: "StockSnapshot"
) -> tuple[float, str]:
    """
    슬롯 역할별 건강 점수 산출 (0~100).
    Returns (score, 가장 심각한 감점 이유).

    leader  — 주도주 역할 유지 여부 (거래대금 순위 + 수급 + 섹터)
    breakout— 돌파 모멘텀 지속 여부 (OBV + 갭 유지 + 거래량)
    pullback— 반등 지속 여부 (장중 등락 + OBV + 추가 하락 없음)
    """
    score = 100.0
    worst_reason = ""

    def _deduct(pts: float, reason: str) -> None:
        nonlocal score, worst_reason
        score -= pts
        if not worst_reason or pts >= 20:
            worst_reason = reason

    if slot == "leader":
        # ① 거래대금 순위에서 이탈
        try:
            from src.infra.market_leaders import get_all_top_tickers
            if ticker not in get_all_top_tickers():
                _deduct(30.0, "거래대금 TOP 이탈 — 시장 관심도 급감")
        except Exception:
            pass

        # ② OBV 하락 전환 (매도세 우위)
        if getattr(snap, "obv_slope", 0.0) < 0:
            _deduct(20.0, "OBV 하락 전환 — 세력 이탈 신호")

        # ③ 외인+기관 동시 이탈
        frgn = getattr(snap, "frgn_net_buy", 0)
        inst = getattr(snap, "inst_net_buy", 0)
        if frgn < 0 and inst < 0:
            _deduct(20.0, "외인+기관 동시 순매도 — 수급 이탈")
        elif frgn < 0:
            _deduct(8.0, "외인 순매도 전환")

        # ④ 주도 섹터에서 이탈
        try:
            from src.infra.sector_rotation import get_hot_sectors, get_sector
            hot = set(get_hot_sectors(3))
            sector = get_sector(ticker)
            if hot and sector and sector not in hot:
                _deduct(15.0, f"주도섹터 이탈 ({sector})")
        except Exception:
            pass

        # ⑤ RSI 극과열
        if snap.rsi > 85:
            _deduct(15.0, f"RSI 극과열 {snap.rsi:.0f}")

    elif slot == "breakout":
        # ① OBV 하락 (모멘텀 소진)
        if getattr(snap, "obv_slope", 0.0) < 0:
            _deduct(30.0, "OBV 하락 — 돌파 모멘텀 소진")

        # ② 갭이 메워짐 (등락률 0% 이하로 복귀)
        if snap.change_pct <= 0:
            _deduct(30.0, f"갭 메워짐 (등락 {snap.change_pct:+.1f}%) — 돌파 실패")

        # ③ 거래량 급등 소진
        if snap.volume_ratio < 1.5:
            _deduct(20.0, f"거래량 정상화 ({snap.volume_ratio:.1f}배) — 관심도 소멸")

        # ④ RSI 극과열
        if snap.rsi > 85:
            _deduct(20.0, f"RSI 극과열 {snap.rsi:.0f}")

    elif slot == "pullback":
        intra = getattr(snap, "intraday_chg_pct", 0.0)

        # ① 장중 다시 하락 전환 (반등 실패)
        if intra < -1.0:
            _deduct(40.0, f"반등 실패 — 장중 재하락 {intra:+.1f}%")
        elif intra < 0:
            _deduct(20.0, f"반등 둔화 — 장중 {intra:+.1f}%")

        # ② OBV 하락 (매도세 우위)
        if getattr(snap, "obv_slope", 0.0) < 0:
            _deduct(25.0, "OBV 하락 — 반등 매수세 부재")

        # ③ 추가 급락 (눌림목 범위 이탈 — 하락 가속)
        if snap.change_pct < -8.0:
            _deduct(40.0, f"추가 급락 ({snap.change_pct:+.1f}%) — 눌림목 아닌 추세 하락")

        # ④ RSI 과열 (눌림목인데 RSI가 오히려 높음 — 잘못된 신호)
        if snap.rsi > 75:
            _deduct(15.0, f"RSI {snap.rsi:.0f} — 눌림목 조건 위배")

    score = max(0.0, score)
    if not worst_reason:
        worst_reason = "양호"
    return score, worst_reason


def _get_empty_slots() -> list[str]:
    """오늘 slot_assignments에서 아직 비어 있는 슬롯 목록 반환."""
    today = date.today().isoformat()
    try:
        rows = fetch_all(
            "SELECT slot FROM slot_assignments WHERE trade_date = ? AND status = 'active'",
            (today,),
        )
        filled = {r["slot"] for r in rows}
        return [s for s in _ALL_SLOTS if s not in filled]
    except Exception:
        return list(_ALL_SLOTS)


def _save_slots(
    slot_result: dict[str, dict | None],
    scan: UniverseScan,
) -> list[dict]:
    """슬롯별 결과를 hot_list + slot_assignments 테이블에 저장."""
    snaps = {s.ticker: s for s in scan.snapshots}
    today = date.today().isoformat()
    saved = []

    for slot, item in slot_result.items():
        if not item:
            continue

        ticker = item.get("ticker", "")
        snap = snaps.get(ticker)
        if not snap:
            continue

        # hot_list 저장
        execute(
            """
            INSERT INTO hot_list
                (ticker, name, signal_type, volume_ratio,
                 price_change_pct, rsi, sector, reason,
                 momentum_score, obv_slope, day_range_pos,
                 stoch_rsi, bb_width_ratio, trading_value, exec_strength,
                 rs_daily, rs_5d, frgn_net_buy, inst_net_buy, atr_pct, slot)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                snap.name,
                item.get("signal_type", "unknown"),
                snap.volume_ratio,
                snap.change_pct,
                snap.rsi,
                getattr(snap, "sector", ""),
                item.get("reason", ""),
                getattr(snap, "momentum_score", 0.0),
                getattr(snap, "obv_slope", 0.0),
                getattr(snap, "day_range_pos", 0.5),
                getattr(snap, "stoch_rsi", 50.0),
                getattr(snap, "bb_width_ratio", 1.0),
                getattr(snap, "trading_value", 0),
                getattr(snap, "exec_strength", 100.0),
                getattr(snap, "rs_daily", 0.0),
                getattr(snap, "rs_5d", 0.0),
                getattr(snap, "frgn_net_buy", 0),
                getattr(snap, "inst_net_buy", 0),
                getattr(snap, "atr_pct", 0.0),
                slot,
            ),
        )

        # slot_assignments UPSERT
        execute(
            """
            INSERT INTO slot_assignments (slot, ticker, name, signal_type, reason, trade_date, status)
            VALUES (?, ?, ?, ?, ?, ?, 'active')
            ON CONFLICT(slot) DO UPDATE SET
                ticker      = excluded.ticker,
                name        = excluded.name,
                signal_type = excluded.signal_type,
                reason      = excluded.reason,
                trade_date  = excluded.trade_date,
                status      = 'active',
                updated_at  = CURRENT_TIMESTAMP
            """,
            (
                slot,
                ticker,
                snap.name,
                item.get("signal_type", "unknown"),
                item.get("reason", ""),
                today,
            ),
        )

        saved.append({
            "slot":         slot,
            "ticker":       ticker,
            "name":         snap.name,
            "signal_type":  item.get("signal_type"),
            "change_pct":   snap.change_pct,
            "rsi":          snap.rsi,
            "reason":       item.get("reason", ""),
        })

    return saved


def release_slot(slot: str) -> None:
    """
    포지션 청산 후 슬롯을 비워 재탐색 가능하게 만든다.
    position_monitor/trading engine에서 호출.
    """
    today = date.today().isoformat()
    try:
        execute(
            """
            UPDATE slot_assignments
            SET status = 'empty', updated_at = CURRENT_TIMESTAMP
            WHERE slot = ? AND trade_date = ?
            """,
            (slot, today),
        )
        logger.info(f"슬롯 해제: {slot}")
    except Exception as e:
        logger.warning(f"슬롯 해제 실패 [{slot}]: {e}")


def get_slot_for_ticker(ticker: str) -> str | None:
    """ticker의 오늘 슬롯 반환 (없으면 None)."""
    today = date.today().isoformat()
    try:
        row = fetch_one(
            "SELECT slot FROM slot_assignments WHERE ticker = ? AND trade_date = ? AND status = 'active'",
            (ticker, today),
        )
        return row["slot"] if row else None
    except Exception:
        return None


# ──────────────────────────────────────────────
# DB 헬퍼
# ──────────────────────────────────────────────

def _is_trading_blocked(market_score: float) -> bool:
    try:
        row = fetch_one(
            "SELECT risk_level FROM risk_status ORDER BY created_at DESC LIMIT 1"
        )
        if row and int(row["risk_level"]) >= _GATE_RISK_LEVEL_MAX:
            logger.debug(f"Gate 1 차단: 리스크 레벨 {row['risk_level']}")
            return True
    except Exception:
        pass

    try:
        row = fetch_one(
            "SELECT korea_market_outlook FROM global_condition ORDER BY created_at DESC LIMIT 1"
        )
        if row and row["korea_market_outlook"] == "negative":
            logger.debug("Gate 2 차단: 글로벌 시황 negative")
            return True
    except Exception:
        pass

    if market_score < _GATE_MARKET_SCORE_MIN:
        logger.debug(f"Gate 3 차단: 국내 시황 점수 {market_score:.2f}")
        return True

    return False


def _get_market_score() -> float:
    try:
        row = fetch_one(
            "SELECT market_score FROM market_condition ORDER BY created_at DESC LIMIT 1"
        )
        return float(row["market_score"]) if row else 0.0
    except Exception:
        return 0.0


def _get_global_risk_score() -> int:
    try:
        row = fetch_one(
            "SELECT global_risk_score FROM global_condition ORDER BY created_at DESC LIMIT 1"
        )
        return int(row["global_risk_score"]) if row else 5
    except Exception:
        return 5


def _get_kospi_chg_pct() -> float:
    try:
        row = fetch_one(
            "SELECT summary FROM market_condition ORDER BY created_at DESC LIMIT 1"
        )
        if row and row.get("summary"):
            summary = json.loads(row["summary"])
            return float(summary.get("kospi", 0.0))
    except Exception:
        pass
    return 0.0


def _get_global_key_events() -> list[str]:
    import json as _json
    try:
        row = fetch_one(
            "SELECT key_events FROM global_condition ORDER BY created_at DESC LIMIT 1"
        )
        if row and row["key_events"]:
            return _json.loads(row["key_events"])
        return []
    except Exception:
        return []


def get_latest_hot_list(limit: int = 10) -> list[dict]:
    """가장 최근 hot_list 조회 (매매팀이 읽는 공개 API)."""
    rows = fetch_all(
        """
        SELECT ticker, name, signal_type, volume_ratio, price_change_pct,
               rsi, reason, slot, created_at
        FROM hot_list
        ORDER BY created_at DESC LIMIT ?
        """,
        (limit,),
    )
    return [dict(r) for r in rows]
