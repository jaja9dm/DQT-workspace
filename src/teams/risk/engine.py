"""
engine.py — 위기 관리팀 메인 엔진

역할:
  DB의 시황 정보 + KIS 포트폴리오 손익을 종합해
  리스크 레벨(1~5)을 산출하고 risk_status 테이블에 저장한다.

  리스크 레벨에 따라 매매팀·포지션 감시팀의 행동이 바뀐다:
    Level 1 (정상)  — 전략 전체 가동, 손절 -5%
    Level 2 (주의)  — 신규 진입 70%로 제한, 손절 -3%
    Level 3 (경계)  — 신규 진입 40%로 제한
    Level 4 (위험)  — 신규 진입 20%로 제한, 손절 -1%
    Level 5 (극위험) — 신규 진입 금지, 전량 청산 신호

실행 주기: 15분마다
즉시 트리거: 글로벌 리스크 점수 ≥8, KOSPI -2% 이상, 포트폴리오 손익 -5% 이상
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, RequestPriority
from src.utils.logger import get_logger
from src.utils.notifier import notify_risk

logger = get_logger(__name__)

_INTERVAL_SEC = 900          # 15분
_KIS_BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"

# 즉시 트리거 임계값
_GLOBAL_RISK_EMERGENCY = 8
_KOSPI_DROP_EMERGENCY = -2.0      # KOSPI -2%
_PORTFOLIO_LOSS_EMERGENCY = -5.0  # 포트폴리오 -5%


# ── 리스크 레벨 정의 ──────────────────────────────────────────

# (position_limit_pct, max_single_trade_pct, stop_loss_tighten, description)
# max_single_trade_pct: 1회 종목당 투입 한도 (가용 예수금 대비 %)
_LEVEL_SPEC: dict[int, tuple[int, float, int, str]] = {
    1: (100, 33.0, 0, "정상 — 전략 전체 가동"),      # 3종목 × 33% = 99% → 예수금 전액 투입
    2: (80,  24.0, 0, "주의 — 신규 진입 80% 제한"),  # 3종목 × 24% × 80% ≈ 58%
    3: (60,  18.0, 0, "경계 — 신규 진입 60% 제한"),  # 3종목 × 18% × 60% ≈ 32%
    4: (30,   8.0, 1, "위험 — 신규 진입 30% 제한 / 손절 강화"),
    5: (0,    0.0, 1, "극위험 — 신규 진입 금지 / 전량 청산"),
}


_emergency_trigger: threading.Event = threading.Event()


def trigger_emergency() -> None:
    """외부(글로벌/국내 시황팀)에서 긴급 경보 발생 시 위기 엔진 즉시 재평가 요청."""
    _emergency_trigger.set()


class RiskEngine:
    """위기 관리팀 엔진 — 독립 스레드로 실행."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="risk-engine",
        )
        self._last_global_risk: int = 0
        self._last_kospi_change: float = 0.0
        self._last_level: int = 1

    def start(self) -> None:
        logger.info("위기 관리팀 엔진 시작")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=10)
        logger.info("위기 관리팀 엔진 종료")

    # ──────────────────────────────────────────
    # 메인 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"위기 관리팀 오류: {e}", exc_info=True)

            # 15분 대기 중 긴급 트리거 수신 시 즉시 재평가
            deadline = time.monotonic() + _INTERVAL_SEC
            while not self._stop_event.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                fired = _emergency_trigger.wait(timeout=min(remaining, 5.0))
                if fired:
                    _emergency_trigger.clear()
                    logger.info("긴급 경보 수신 — 위기 레벨 즉시 재평가")
                    break

    def run_once(self) -> dict:
        """
        1회 실행: 데이터 수집 → 리스크 점수 산출 → 레벨 결정 → DB 저장.

        Returns:
            저장된 risk_status 딕셔너리
        """
        # 1. DB에서 시황 데이터 읽기
        ctx = _load_context()

        # 2. KIS 포트폴리오 손익 조회
        portfolio = _fetch_portfolio()

        # 3. 리스크 점수 산출 (0~100)
        score, alerts = _calc_risk_score(ctx, portfolio)

        # 4. 레벨 결정
        level = _score_to_level(score)

        # 5. 즉시 트리거 강제 상향
        level, alerts = self._check_emergency_upgrade(level, alerts, ctx, portfolio)

        # 6. DB 저장
        row = _save_to_db(level, score, alerts)

        logger.info(
            f"리스크 평가 완료 — 점수={score} | 레벨={level} | "
            f"{_LEVEL_SPEC[level][3]}"
        )

        # 7. 레벨 변경 시 Telegram 알림
        if level != self._last_level:
            notify_risk(level, alerts)
            self._last_level = level

        return row

    # ──────────────────────────────────────────
    # 긴급 레벨 강제 상향
    # ──────────────────────────────────────────

    def _check_emergency_upgrade(
        self,
        level: int,
        alerts: list[str],
        ctx: dict,
        portfolio: dict,
    ) -> tuple[int, list[str]]:
        """긴급 조건 발생 시 레벨을 즉시 4 이상으로 상향."""
        forced = level

        global_risk = ctx.get("global_risk_score", 0)
        kospi_change = ctx.get("kospi_change", 0.0)
        pnl_pct = portfolio.get("total_pnl_pct", 0.0)

        if global_risk >= _GLOBAL_RISK_EMERGENCY:
            if forced < 4:
                forced = 4
                alerts.append(f"글로벌 리스크 {global_risk}/10 — 레벨 4 강제 상향")

        if kospi_change <= _KOSPI_DROP_EMERGENCY:
            if forced < 4:
                forced = 4
                alerts.append(f"KOSPI {kospi_change:+.2f}% — 레벨 4 강제 상향")

        if pnl_pct <= _PORTFOLIO_LOSS_EMERGENCY:
            if forced < 5:
                forced = 5
                alerts.append(f"포트폴리오 손익 {pnl_pct:+.2f}% — 레벨 5 강제 상향")

        if forced != level:
            logger.warning(f"[위기 경보] 리스크 레벨 {level} → {forced} 긴급 상향")
            # 즉시 트리거 추적
            self._last_global_risk = global_risk
            self._last_kospi_change = kospi_change

        return forced, alerts


# ──────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────

def _load_context() -> dict:
    """DB에서 최신 시황·Hot List 데이터 로드."""
    ctx: dict = {
        "global_risk_score": 5,
        "vix": 0.0,
        "korea_market_outlook": "neutral",
        "market_score": 0.0,
        "kospi_change": 0.0,
        "foreign_net_buy_bn": 0.0,
        "hot_list_count_1h": 0,
    }

    # 글로벌 시황
    row = fetch_one(
        "SELECT global_risk_score, vix, korea_market_outlook FROM global_condition ORDER BY created_at DESC LIMIT 1"
    )
    if row:
        ctx["global_risk_score"] = int(row["global_risk_score"])
        ctx["vix"] = float(row["vix"] or 0)
        ctx["korea_market_outlook"] = row["korea_market_outlook"]

    # 국내 시황
    row = fetch_one(
        "SELECT market_score, foreign_net_buy_bn, summary FROM market_condition ORDER BY created_at DESC LIMIT 1"
    )
    if row:
        ctx["market_score"] = float(row["market_score"] or 0)
        ctx["foreign_net_buy_bn"] = float(row["foreign_net_buy_bn"] or 0)
        # summary JSON에서 kospi 등락률 추출
        try:
            summary = json.loads(row["summary"] or "{}")
            ctx["kospi_change"] = float(summary.get("kospi", 0))
        except Exception:
            pass

    # 최근 1시간 Hot List 건수
    rows = fetch_all(
        "SELECT COUNT(*) as cnt FROM hot_list WHERE created_at >= datetime('now', '-1 hour')"
    )
    if rows:
        ctx["hot_list_count_1h"] = int(rows[0]["cnt"])

    return ctx


def _fetch_portfolio() -> dict:
    """
    KIS API로 현재 보유 포지션 손익 조회.

    Returns:
        {"total_eval_amt": float, "total_pnl_amt": float, "total_pnl_pct": float, "positions": list}
    """
    _empty = {"total_eval_amt": 0.0, "total_pnl_amt": 0.0, "total_pnl_pct": 0.0, "positions": []}
    gw = KISGateway()

    try:
        account_no = settings.KIS_ACCOUNT_NO
        acnt_no, acnt_prdt_cd = (account_no.split("-") + ["01"])[:2]

        # 모의/실거래 tr_id 분기
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

        output1 = resp.get("output1", [])  # 종목별
        output2 = resp.get("output2", [{}])  # 합계

        summary = output2[0] if output2 else {}
        total_eval = float(summary.get("tot_evlu_amt", 0) or 0)
        total_pnl = float(summary.get("evlu_pfls_smtl_amt", 0) or 0)
        purchase_amt = float(summary.get("pchs_amt_smtl_amt", 0) or total_eval)
        total_pnl_pct = (total_pnl / purchase_amt * 100) if purchase_amt > 0 else 0.0

        positions = []
        for item in output1:
            qty = int(item.get("hldg_qty", 0) or 0)
            if qty == 0:
                continue
            positions.append({
                "ticker": item.get("pdno", ""),
                "name": item.get("prdt_name", ""),
                "quantity": qty,
                "avg_price": float(item.get("pchs_avg_pric", 0) or 0),
                "current_price": float(item.get("prpr", 0) or 0),
                "pnl_pct": float(item.get("evlu_pfls_rt", 0) or 0),
            })

        return {
            "total_eval_amt": total_eval,
            "total_pnl_amt": total_pnl,
            "total_pnl_pct": round(total_pnl_pct, 3),
            "positions": positions,
        }

    except Exception as e:
        logger.warning(f"KIS 잔고 조회 실패: {e}")
        return _empty


# ──────────────────────────────────────────────
# 리스크 점수 산출
# ──────────────────────────────────────────────

def _calc_risk_score(ctx: dict, portfolio: dict) -> tuple[int, list[str]]:
    """
    0~100점 리스크 점수 산출.

    구성:
      글로벌 리스크 (0~40pt): global_risk_score * 4
      국내 시황    (0~20pt): market_score 역산
      VIX          (0~15pt): VIX 수준
      포트폴리오   (0~15pt): 현재 손익
      Hot List 과열 (0~10pt): 단기 Hot List 급증

    Returns:
        (score 0~100, alerts 리스트)
    """
    alerts: list[str] = []
    score = 0

    # 1. 글로벌 리스크 (0~40)
    global_risk = ctx.get("global_risk_score", 5)
    global_pts = global_risk * 4
    score += global_pts
    if global_risk >= 7:
        alerts.append(f"글로벌 리스크 {global_risk}/10 — 고위험")

    # 2. 국내 시황 (0~20) — market_score -1(약세)~+1(강세) → 약세일수록 점수 높음
    market_score = ctx.get("market_score", 0.0)
    # -1.0 → 20pt, 0.0 → 10pt, +1.0 → 0pt
    market_pts = int((1.0 - market_score) * 10)
    market_pts = max(0, min(20, market_pts))
    score += market_pts

    outlook = ctx.get("korea_market_outlook", "neutral")
    if outlook == "negative":
        alerts.append("국내 시황 부정적")

    # 3. VIX (0~15)
    vix = ctx.get("vix", 0.0)
    if vix >= 30:
        vix_pts = 15
        alerts.append(f"VIX {vix:.1f} — 극도 공포")
    elif vix >= 25:
        vix_pts = 10
        alerts.append(f"VIX {vix:.1f} — 공포 경보")
    elif vix >= 18:
        vix_pts = 5
    else:
        vix_pts = 0
    score += vix_pts

    # 4. 포트폴리오 손익 (0~15)
    pnl_pct = portfolio.get("total_pnl_pct", 0.0)
    if pnl_pct <= -5.0:
        pnl_pts = 15
        alerts.append(f"포트폴리오 손익 {pnl_pct:+.2f}% — 손실 경보")
    elif pnl_pct <= -3.0:
        pnl_pts = 10
        alerts.append(f"포트폴리오 손익 {pnl_pct:+.2f}% — 주의")
    elif pnl_pct <= -1.0:
        pnl_pts = 5
    else:
        pnl_pts = 0
    score += pnl_pts

    # 5. Hot List 과열 (0~10): 1시간 내 Hot List ≥ 5종목 → 변동성 과열
    hot_cnt = ctx.get("hot_list_count_1h", 0)
    if hot_cnt >= 10:
        hot_pts = 10
        alerts.append(f"Hot List {hot_cnt}종목/1시간 — 시장 과열")
    elif hot_cnt >= 5:
        hot_pts = 5
    else:
        hot_pts = 0
    score += hot_pts

    score = max(0, min(100, score))
    return score, alerts


def _score_to_level(score: int) -> int:
    """점수(0~100) → 리스크 레벨(1~5)."""
    if score <= 20:
        return 1
    elif score <= 40:
        return 2
    elif score <= 60:
        return 3
    elif score <= 80:
        return 4
    else:
        return 5


# ──────────────────────────────────────────────
# DB 저장
# ──────────────────────────────────────────────

def _save_to_db(level: int, score: int, alerts: list[str]) -> dict:
    """risk_status 테이블에 저장."""
    spec = _LEVEL_SPEC[level]
    position_limit_pct, max_single_trade_pct, stop_loss_tighten, description = spec

    execute(
        """
        INSERT INTO risk_status
            (risk_level, risk_score, position_limit_pct,
             max_single_trade_pct, stop_loss_tighten,
             active_alerts, recommended_action)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            level,
            score,
            position_limit_pct,
            max_single_trade_pct,
            stop_loss_tighten,
            json.dumps(alerts, ensure_ascii=False),
            description,
        ),
    )

    return {
        "risk_level": level,
        "risk_score": score,
        "position_limit_pct": position_limit_pct,
        "max_single_trade_pct": max_single_trade_pct,
        "stop_loss_tighten": stop_loss_tighten,
        "active_alerts": alerts,
        "recommended_action": description,
    }


# ──────────────────────────────────────────────
# 공개 API (다른 팀이 읽는 인터페이스)
# ──────────────────────────────────────────────

def get_current_risk() -> dict:
    """
    현재 리스크 상태 반환 (매매팀·포지션 감시팀이 사용).

    Returns:
        risk_status 최신 행 딕셔너리.
        데이터 없으면 Level 1 기본값.
    """
    row = fetch_one(
        "SELECT * FROM risk_status ORDER BY created_at DESC LIMIT 1"
    )
    if row:
        d = dict(row)
        d["active_alerts"] = json.loads(d.get("active_alerts") or "[]")
        return d

    # 데이터 없을 때 기본값 (시스템 시작 직후)
    spec = _LEVEL_SPEC[1]
    return {
        "risk_level": 1,
        "risk_score": 0,
        "position_limit_pct": spec[0],
        "max_single_trade_pct": spec[1],
        "stop_loss_tighten": spec[2],
        "active_alerts": [],
        "recommended_action": spec[3],
    }


def get_stop_loss_pct() -> float:
    """
    현재 리스크 레벨에 맞는 손절률 반환.
    포지션 감시팀이 손절 기준으로 사용.
    """
    risk = get_current_risk()
    level = risk.get("risk_level", 1)
    tighten = bool(risk.get("stop_loss_tighten", 0))

    if level >= 4 or tighten:
        return settings.STOP_LOSS_LEVEL4_PCT
    elif level == 2:
        return settings.STOP_LOSS_LEVEL2_PCT
    else:
        return settings.STOP_LOSS_DEFAULT_PCT
