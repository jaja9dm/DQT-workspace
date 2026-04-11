"""
kis_gateway.py
KIS(한국투자증권) API 게이트웨이 — 싱글턴 서비스.

모든 팀은 KIS API를 직접 호출하지 않고 반드시 이 게이트웨이를 경유한다.
- 토큰 관리: Access Token 발급·갱신·만료 감지 (만료 30분 전 자동 갱신)
- Rate Limit 관리: 초당·분당 호출 한도 준수 (큐 방식)
- 우선순위: 포지션 감시 > 매매팀 > 수집팀
- 모의/실전 전환: KIS_MODE 환경변수 1개로 전환
"""

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
from queue import PriorityQueue

import requests

from src.config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Priority(IntEnum):
    """요청 우선순위 — 낮을수록 먼저 처리."""
    POSITION_MONITOR = 1  # 포지션 감시 (가장 우선)
    TRADING = 2           # 매매팀
    DATA_COLLECTION = 3   # 수집팀


@dataclass(order=True)
class _Request:
    """우선순위 큐에 넣을 요청 래퍼."""
    priority: int
    seq: int = field(compare=True)           # 동일 우선순위 내 FIFO
    method: str = field(compare=False)
    path: str = field(compare=False)
    tr_id: str = field(compare=False)
    params: dict = field(compare=False, default_factory=dict)
    body: dict = field(compare=False, default_factory=dict)
    result: dict | None = field(compare=False, default=None)
    event: threading.Event = field(compare=False, default_factory=threading.Event)
    error: Exception | None = field(compare=False, default=None)


class KISGateway:
    """
    KIS API 게이트웨이 싱글턴.

    Usage:
        gw = KISGateway()
        price = gw.get_price("005930")
        gw.place_order("005930", "buy", quantity=10, price=70000)
    """

    _instance: "KISGateway | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "KISGateway":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._base_url = settings.KIS_BASE_URL
        self._app_key = settings.KIS_APP_KEY
        self._app_secret = settings.KIS_APP_SECRET
        self._account_no = settings.KIS_ACCOUNT_NO
        self._mode = settings.KIS_MODE

        self._access_token: str = ""
        self._token_expires_at: datetime = datetime.min
        self._token_lock = threading.Lock()

        # 우선순위 요청 큐
        self._queue: PriorityQueue[_Request] = PriorityQueue()
        self._seq = 0
        self._seq_lock = threading.Lock()

        # Rate Limit: KIS API — 초당 20건, 분당 200건 (여유 있게 절반 사용)
        self._rate_per_sec = 10
        self._last_call_times: list[float] = []
        self._rate_lock = threading.Lock()

        # 워커 스레드 시작
        self._worker_thread = threading.Thread(
            target=self._worker, daemon=True, name="kis-gateway-worker"
        )
        self._worker_thread.start()

        # 토큰 갱신 스레드 시작
        self._token_refresh_thread = threading.Thread(
            target=self._token_refresh_loop, daemon=True, name="kis-token-refresh"
        )
        self._token_refresh_thread.start()

        logger.info(f"KIS 게이트웨이 시작 — 모드: {self._mode.upper()}")

    # ──────────────────────────────────────────
    # 공개 API
    # ──────────────────────────────────────────

    def get_price(self, ticker: str, priority: Priority = Priority.DATA_COLLECTION) -> dict:
        """
        현재가 조회.

        Returns:
            {
                "ticker": "005930",
                "price": 70000,
                "change_pct": 1.23,
                "volume": 12345678,
            }
        """
        return self._request(
            method="GET",
            path="/uapi/domestic-stock/v1/quotations/inquire-price",
            tr_id="FHKST01010100",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            priority=priority,
        )

    def get_balance(self, priority: Priority = Priority.TRADING) -> dict:
        """잔고 및 보유 포지션 조회."""
        acc, prod = self._account_no.split("-")
        return self._request(
            method="GET",
            path="/uapi/domestic-stock/v1/trading/inquire-balance",
            tr_id="TTTC8434R" if self._mode == "live" else "VTTC8434R",
            params={
                "CANO": acc,
                "ACNT_PRDT_CD": prod,
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
            priority=priority,
        )

    def place_order(
        self,
        ticker: str,
        side: str,           # "buy" | "sell"
        quantity: int,
        price: int = 0,      # 0 이면 시장가
        priority: Priority = Priority.TRADING,
    ) -> dict:
        """
        주문 실행.

        Args:
            ticker:   종목 코드 ex) "005930"
            side:     "buy" | "sell"
            quantity: 주문 수량
            price:    지정가 (원). 0 이면 시장가 주문.
        """
        order_type = "00" if price == 0 else "01"  # 00=시장가, 01=지정가
        if self._mode == "live":
            tr_id = "TTTC0802U" if side == "buy" else "TTTC0801U"
        else:
            tr_id = "VTTC0802U" if side == "buy" else "VTTC0801U"

        acc, prod = self._account_no.split("-")
        body = {
            "CANO": acc,
            "ACNT_PRDT_CD": prod,
            "PDNO": ticker,
            "ORD_DVSN": order_type,
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price),
        }
        logger.info(
            f"주문 요청 [{self._mode.upper()}] {side.upper()} {ticker} "
            f"수량={quantity} 가격={'시장가' if price == 0 else f'{price:,}원'}"
        )
        return self._request(
            method="POST",
            path="/uapi/domestic-stock/v1/trading/order-cash",
            tr_id=tr_id,
            body=body,
            priority=priority,
        )

    def cancel_order(
        self,
        order_no: str,
        ticker: str,
        quantity: int,
        priority: Priority = Priority.TRADING,
    ) -> dict:
        """주문 취소."""
        if self._mode == "live":
            tr_id = "TTTC0803U"
        else:
            tr_id = "VTTC0803U"

        acc, prod = self._account_no.split("-")
        body = {
            "CANO": acc,
            "ACNT_PRDT_CD": prod,
            "KRX_FWDG_ORD_ORGNO": "",
            "ORGN_ODNO": order_no,
            "ORD_DVSN": "02",
            "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0",
            "PDNO": ticker,
        }
        return self._request(
            method="POST",
            path="/uapi/domestic-stock/v1/trading/order-rvsecncl",
            tr_id=tr_id,
            body=body,
            priority=priority,
        )

    # ──────────────────────────────────────────
    # 내부 구현
    # ──────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        tr_id: str,
        params: dict | None = None,
        body: dict | None = None,
        priority: Priority = Priority.DATA_COLLECTION,
    ) -> dict:
        """요청을 큐에 넣고 결과가 올 때까지 블로킹."""
        with self._seq_lock:
            self._seq += 1
            seq = self._seq

        req = _Request(
            priority=int(priority),
            seq=seq,
            method=method,
            path=path,
            tr_id=tr_id,
            params=params or {},
            body=body or {},
        )
        self._queue.put(req)
        req.event.wait(timeout=30)

        if req.error:
            raise req.error
        return req.result or {}

    def _worker(self) -> None:
        """큐에서 요청을 꺼내 순서대로 실행하는 워커 스레드."""
        while True:
            req = self._queue.get()
            try:
                self._rate_limit_wait()
                token = self._get_token()
                result = self._call_api(req, token)
                req.result = result
            except Exception as e:
                logger.error(f"KIS API 오류: {e}")
                req.error = e
            finally:
                req.event.set()
                self._queue.task_done()

    def _rate_limit_wait(self) -> None:
        """초당 호출 한도 준수. 필요 시 슬립."""
        with self._rate_lock:
            now = time.monotonic()
            self._last_call_times = [t for t in self._last_call_times if now - t < 1.0]
            if len(self._last_call_times) >= self._rate_per_sec:
                wait = 1.0 - (now - self._last_call_times[0])
                if wait > 0:
                    time.sleep(wait)
            self._last_call_times.append(time.monotonic())

    def _call_api(self, req: _Request, token: str) -> dict:
        """실제 HTTP 요청 실행. 실패 시 최대 3회 재시도."""
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self._app_key,
            "appsecret": self._app_secret,
            "tr_id": req.tr_id,
            "custtype": "P",
        }
        url = self._base_url + req.path

        for attempt in range(3):
            try:
                if req.method == "GET":
                    resp = requests.get(url, headers=headers, params=req.params, timeout=10)
                else:
                    resp = requests.post(url, headers=headers, json=req.body, timeout=10)

                resp.raise_for_status()
                data = resp.json()

                if data.get("rt_cd") != "0":
                    raise RuntimeError(f"KIS API 오류 응답: {data.get('msg1')} (rt_cd={data.get('rt_cd')})")

                return data

            except requests.exceptions.RequestException as e:
                if attempt < 2:
                    logger.warning(f"KIS API 재시도 {attempt + 1}/3: {e}")
                    time.sleep(1 * (attempt + 1))
                else:
                    raise RuntimeError(f"KIS API 3회 실패: {e}") from e

        return {}  # unreachable

    def _get_token(self) -> str:
        """Access Token 반환. 만료 30분 전이면 자동 갱신."""
        with self._token_lock:
            if datetime.now() < self._token_expires_at - timedelta(minutes=30):
                return self._access_token
            return self._issue_token()

    def _issue_token(self) -> str:
        """Access Token 신규 발급."""
        url = self._base_url + "/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": self._app_key,
            "appsecret": self._app_secret,
        }
        resp = requests.post(url, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 86400))
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        logger.info(f"KIS 토큰 갱신 완료 — 만료: {self._token_expires_at.strftime('%H:%M:%S')}")
        return self._access_token

    def _token_refresh_loop(self) -> None:
        """만료 30분 전 자동 토큰 갱신 루프."""
        while True:
            time.sleep(60)  # 1분마다 체크
            try:
                with self._token_lock:
                    remaining = (self._token_expires_at - datetime.now()).total_seconds()
                    if 0 < remaining < 1800:  # 30분 미만 남으면 갱신
                        self._issue_token()
            except Exception as e:
                logger.error(f"토큰 자동 갱신 실패: {e}")
