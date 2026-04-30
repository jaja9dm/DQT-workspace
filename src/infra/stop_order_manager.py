"""
stop_order_manager.py — KIS 거래소 사전 손절 주문 관리

역할:
  매수 직후 KIS에 지정가 매도 주문을 미리 제출해둔다.
  시스템이 다운되더라도 거래소 서버에서 자동 체결되는 안전망.
  트레일링 스톱 손절선이 올라갈 때마다 기존 주문을 취소하고 새 가격으로 재제출.
  우리 시스템이 직접 매도할 때는 미리 제출한 주문을 먼저 취소해 이중 매도 방지.

주의:
  지정가(ORD_DVSN=00) 주문이므로 stop_price 이상에서만 체결됨.
  갭 하락으로 가격이 stop_price 아래로 뛰어넘어 열리는 경우 미체결 상태로 남을 수 있음.
  이 경우 position_monitor의 90초 폴링이 백업으로 동작한다.

KIS 취소 API:
  endpoint : /uapi/domestic-stock/v1/trading/order-rvsecncl
  TR_ID    : VTTC0803U (모의) / TTTC0803U (실거래)
  필수 필드 : ORGN_ODNO (원주문번호), KRX_FWDG_ORD_ORGNO, QTY_ALL_ORD_YN="Y"
"""

from __future__ import annotations

from src.config.settings import settings
from src.infra.database import execute, fetch_one
from src.infra.kis_gateway import KISGateway, RequestPriority
from src.utils.logger import get_logger

logger = get_logger(__name__)

_KIS_ORDER_PATH  = "/uapi/domestic-stock/v1/trading/order-cash"
_KIS_CANCEL_PATH = "/uapi/domestic-stock/v1/trading/order-rvsecncl"


# ──────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────

def _tick_unit(price: float) -> int:
    """KIS 호가 단위 반환."""
    if price < 1_000:    return 1
    if price < 5_000:    return 5
    if price < 10_000:   return 10
    if price < 50_000:   return 50
    if price < 100_000:  return 100
    if price < 500_000:  return 500
    return 1_000


def _floor_to_tick(price: float) -> int:
    """가격을 호가 단위 아래로 내림 (손절가는 보수적으로 내림)."""
    unit = _tick_unit(price)
    return int(price // unit) * unit


def place_stop_order(ticker: str, quantity: int, stop_price: float) -> bool:
    """
    KIS에 지정가 손절 매도 주문 제출.

    Args:
        ticker    : 종목코드
        quantity  : 매도 수량 (전량)
        stop_price: 손절 지정가 (원)

    Returns:
        True  → 주문 정상 제출 및 DB 저장
        False → 실패 (로그 기록됨)
    """
    # 이미 존재하는 주문이 있으면 먼저 취소 — 실패 시 이중 매도 방지를 위해 중단
    existing = _get_stop_order(ticker)
    if existing:
        logger.info(f"[Stop Order] {ticker} 기존 주문 취소 후 재제출")
        cancel_result = _cancel_on_kis(existing)
        if cancel_result == "error":
            logger.error(f"[Stop Order] {ticker} 기존 주문 취소 실패 — 이중 매도 방지를 위해 신규 제출 중단")
            return False
        # "ok" 또는 "filled" 모두 이전 주문은 소멸됨 → DB에서 제거 후 재제출
        _delete_stop_order(ticker)

    gw = KISGateway()
    tr_id = "VTTC0801U" if settings.KIS_MODE == "paper" else "TTTC0801U"
    acnt_no, acnt_prdt_cd = (settings.KIS_ACCOUNT_NO.split("-") + ["01"])[:2]

    try:
        resp = gw.request(
            method="POST",
            path=_KIS_ORDER_PATH,
            body={
                "CANO": acnt_no,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": ticker,
                "ORD_DVSN": "00",               # 지정가
                "ORD_QTY": str(quantity),
                "ORD_UNPR": str(_floor_to_tick(stop_price)),
                "ALGO_NO": "",
            },
            tr_id=tr_id,
            priority=RequestPriority.TRADING,
        )
        output   = resp.get("output", {})
        order_no = output.get("ODNO", "")
        krx_orgno = output.get("KRX_FWDG_ORD_ORGNO", "")

        if not order_no:
            logger.warning(f"[Stop Order] {ticker} 주문번호 미수신 — 응답: {output}")
            return False

        _save_stop_order(ticker, order_no, krx_orgno, stop_price, quantity)
        logger.info(
            f"[Stop Order] {ticker} 지정가 손절 주문 제출 완료 "
            f"| 손절가 {stop_price:,.0f}원 | {quantity}주 | 주문번호 {order_no}"
        )
        return True

    except Exception as e:
        logger.error(f"[Stop Order] {ticker} 주문 제출 실패: {e}")
        return False


def cancel_stop_order(ticker: str) -> str:
    """
    기존 손절 주문 취소.
    포지션 감시팀이 직접 매도하기 직전에 호출해 이중 매도 방지.

    Returns:
        "ok"      → 취소 성공 또는 기존 주문 없음
        "filled"  → 손절 주문이 이미 KIS에서 자동 체결됨 → 포지션 이미 없음
        "error"   → 취소 실패 (주문 살아있을 수 있음)
    """
    existing = _get_stop_order(ticker)
    if not existing:
        return "ok"  # 취소할 주문 없음 — 정상

    result = _cancel_on_kis(existing)
    if result in ("ok", "filled"):
        _delete_stop_order(ticker)
        if result == "ok":
            logger.info(f"[Stop Order] {ticker} 손절 주문 취소 완료")
        # "filled" 로그는 _cancel_on_kis 내부에서 이미 기록
    else:
        logger.warning(f"[Stop Order] {ticker} 손절 주문 취소 실패 — 수동 확인 필요")
    return result


def update_stop_order(ticker: str, quantity: int, new_stop_price: float) -> bool:
    """
    손절 주문 가격 업데이트 (취소 + 재제출).
    트레일링 스톱 손절선이 올라갈 때 호출.

    Args:
        ticker        : 종목코드
        quantity      : 현재 보유 수량
        new_stop_price: 새 손절 지정가 (원)
    """
    logger.info(
        f"[Stop Order] {ticker} 손절선 업데이트 → {new_stop_price:,.0f}원 ({quantity}주)"
    )
    return place_stop_order(ticker, quantity, new_stop_price)


# ──────────────────────────────────────────────
# KIS API 헬퍼
# ──────────────────────────────────────────────

def _cancel_on_kis(order: dict) -> str:
    """
    KIS 취소 API 호출.

    Returns:
        "ok"       → 취소 성공
        "filled"   → 이미 체결됨 ("정정/취소할 수량이 없습니다")
        "error"    → 그 외 오류
    """
    gw = KISGateway()
    tr_id = "VTTC0803U" if settings.KIS_MODE == "paper" else "TTTC0803U"
    acnt_no, acnt_prdt_cd = (settings.KIS_ACCOUNT_NO.split("-") + ["01"])[:2]

    try:
        gw.request(
            method="POST",
            path=_KIS_CANCEL_PATH,
            body={
                "CANO": acnt_no,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "KRX_FWDG_ORD_ORGNO": order.get("krx_orgno", ""),
                "ORGN_ODNO": order["order_no"],
                "ORD_DVSN": "00",         # 원주문과 동일 (지정가)
                "RVSE_CNCL_DVSN_CD": "02",  # 02 = 취소
                "ORD_QTY": str(order["quantity"]),
                "ORD_UNPR": "0",
                "QTY_ALL_ORD_YN": "Y",
                "ALGO_NO": "",
            },
            tr_id=tr_id,
            priority=RequestPriority.TRADING,
        )
        return "ok"
    except Exception as e:
        err_str = str(e)
        # "정정/취소할 수량이 없습니다" = 손절 주문이 이미 KIS에서 자동 체결됨
        if "정정/취소할 수량" in err_str or "취소할 수량" in err_str:
            logger.info(
                f"[Stop Order] {order['ticker']} 주문번호 {order['order_no']}: "
                f"이미 체결됨 (자동 손절 확정)"
            )
            return "filled"
        logger.error(
            f"[Stop Order] KIS 취소 실패 [{order['ticker']}] "
            f"주문번호 {order['order_no']}: {e}"
        )
        return "error"


# ──────────────────────────────────────────────
# DB 헬퍼
# ──────────────────────────────────────────────

def _get_stop_order(ticker: str) -> dict | None:
    try:
        row = fetch_one("SELECT * FROM stop_orders WHERE ticker = ?", (ticker,))
        return dict(row) if row else None
    except Exception:
        return None


def get_stop_order_price(ticker: str) -> float | None:
    """
    등록된 손절 주문의 지정가 반환.
    stop 주문이 자동 체결됐을 때 체결 추정가로 사용.
    """
    row = _get_stop_order(ticker)
    return float(row["stop_price"]) if row else None


def _save_stop_order(
    ticker: str, order_no: str, krx_orgno: str, stop_price: float, quantity: int
) -> None:
    execute(
        """
        INSERT INTO stop_orders (ticker, order_no, krx_orgno, stop_price, quantity, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(ticker) DO UPDATE SET
            order_no   = excluded.order_no,
            krx_orgno  = excluded.krx_orgno,
            stop_price = excluded.stop_price,
            quantity   = excluded.quantity,
            updated_at = CURRENT_TIMESTAMP
        """,
        (ticker, order_no, krx_orgno, stop_price, quantity),
    )


def _delete_stop_order(ticker: str) -> None:
    try:
        execute("DELETE FROM stop_orders WHERE ticker = ?", (ticker,))
    except Exception:
        pass
