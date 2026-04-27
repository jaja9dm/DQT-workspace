"""
kis_gateway.py
KIS(한국투자증권) API 게이트웨이 — 싱글턴 서비스.

모든 팀은 KIS API를 직접 호출하지 않고 반드시 이 게이트웨이를 경유한다.
- 토큰 관리: Access Token 발급·갱신·만료 감지 (만료 30분 전 자동 갱신)
- Rate Limit 관리: 초당·분당 호출 한도 준수 (큐 방식)
- 우선순위: 포지션 감시 > 매매팀 > 수집팀
- 모의/실전 전환: KIS_MODE 환경변수 1개로 전환
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
from pathlib import Path
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
    BACKGROUND = 4        # 분봉 조회 등 백그라운드 (rate limit 여유 있을 때만)


# 하위 호환 alias
RequestPriority = Priority


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
        self._token_expires_at: datetime = datetime(2000, 1, 1)  # 항상 만료 상태로 초기화
        self._token_lock = threading.Lock()
        # 토큰 파일 캐시 경로 — 재시작 시 재발급 방지 (KIS 하루 1회 발급 제한)
        self._token_cache_path = Path(settings.DB_PATH).parent.parent / ".kis_token_cache.json"

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

    def request(
        self,
        method: str,
        path: str,
        tr_id: str,
        params: dict | None = None,
        body: dict | None = None,
        priority: Priority = Priority.DATA_COLLECTION,
    ) -> dict:
        """공개 요청 메서드 — 커스텀 API 경로 직접 호출."""
        return self._request(
            method=method,
            path=path,
            tr_id=tr_id,
            params=params,
            body=body,
            priority=priority,
        )

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

    def get_minute_candles(
        self,
        ticker: str,
        priority: Priority = Priority.DATA_COLLECTION,
    ) -> list[dict]:
        """
        1분봉 데이터 조회 (최근 30봉).

        KIS FHKST03010200 — 주식 당일 분봉 조회.
        반환 리스트는 최신순(내림차순)이므로 시간순 사용 시 reversed() 필요.

        Returns:
            [{"time": "HHmmss", "open": .., "high": .., "low": .., "close": .., "volume": ..}, ...]
        """
        from datetime import datetime as _dt
        try:
            resp = self._request(
                method="GET",
                path="/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
                tr_id="FHKST03010200",
                params={
                    "FID_ETC_CLS_CODE": "",
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": ticker,
                    "FID_INPUT_HOUR_1": _dt.now().strftime("%H%M%S"),
                    "FID_PW_DATA_INCU_YN": "N",
                },
                priority=priority,
            )
            candles: list[dict] = []
            for item in resp.get("output2", []):
                try:
                    candles.append({
                        "time":   item.get("stck_cntg_hour", ""),
                        "open":   float(item.get("stck_oprc", 0) or 0),
                        "high":   float(item.get("stck_hgpr", 0) or 0),
                        "low":    float(item.get("stck_lwpr", 0) or 0),
                        "close":  float(item.get("stck_prpr", 0) or 0),
                        "volume": int(item.get("cntg_vol", 0) or 0),
                    })
                except (ValueError, TypeError):
                    continue
            return candles  # 최신순 반환
        except Exception as e:
            logger.debug(f"분봉 조회 실패 [{ticker}]: {e}")
            return []

    def get_trading_value_ranking(
        self,
        market: str = "J",   # J=KOSPI, Q=KOSDAQ
        top_n: int = 20,
        priority: Priority = Priority.BACKGROUND,
    ) -> list[dict]:
        """
        거래대금 순위 조회 (KIS FHPST01730000).

        Args:
            market: "J"=KOSPI, "Q"=KOSDAQ
            top_n:  상위 N종목

        Returns:
            [{"ticker", "name", "price", "change_pct", "trading_value",
              "volume", "frgn_net_buy", "inst_net_buy"}, ...]
        """
        try:
            resp = self._request(
                method="GET",
                path="/uapi/domestic-stock/v1/ranking/trading-value",
                tr_id="FHPST01730000",
                params={
                    "fid_cond_mrkt_div_code": market,
                    "fid_cond_scr_div_code":  "20173",
                    "fid_input_iscd":          "0000",
                    "fid_div_cls_code":        "0",
                    "fid_blng_cls_code":       "0",
                    "fid_trgt_cls_code":       "111111111",
                    "fid_trgt_exls_cls_code":  "0000000000",
                    "fid_input_price_1":       "",
                    "fid_input_price_2":       "",
                    "fid_vol_cnt":             "",
                    "fid_input_date_1":        "",
                },
                priority=priority,
            )
            result = []
            for item in resp.get("output", [])[:top_n]:
                try:
                    result.append({
                        "ticker":       str(item.get("stck_shrn_iscd", "")).zfill(6),
                        "name":         str(item.get("hts_kor_isnm", "")),
                        "price":        float(item.get("stck_prpr", 0) or 0),
                        "change_pct":   float(item.get("prdy_ctrt", 0) or 0),
                        "trading_value": int(item.get("acml_tr_pbmn", 0) or 0),
                        "volume":       int(item.get("acml_vol", 0) or 0),
                        "frgn_net_buy": int(item.get("frgn_ntby_qty", 0) or 0),
                        "inst_net_buy": int(item.get("orgn_ntby_qty", 0) or 0),
                    })
                except (ValueError, TypeError):
                    continue
            return result
        except Exception as e:
            logger.debug(f"거래대금 순위 조회 실패 ({market}): {e}")
            return []

    def get_orderbook(
        self,
        ticker: str,
        priority: Priority = Priority.TRADING,
    ) -> dict:
        """
        호가창 조회 — 매수/매도 10단계 잔량.

        Returns:
            {
                "bid_qty": int,   # 상위 5단계 매수호가 잔량 합
                "ask_qty": int,   # 상위 5단계 매도호가 잔량 합
                "imbalance": float,  # bid_qty / ask_qty (1.0=균형, 1.5+=매수우위)
            }
        실패 시: {"bid_qty": 0, "ask_qty": 0, "imbalance": 1.0}
        """
        _default = {"bid_qty": 0, "ask_qty": 0, "imbalance": 1.0}
        try:
            resp = self._request(
                method="GET",
                path="/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                tr_id="FHKST01010200",
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": ticker,
                },
                priority=priority,
            )
            output2 = resp.get("output2", {})
            if not output2:
                return _default
            # 상위 5단계 매수/매도 잔량 합산
            bid = sum(
                int(output2.get(f"bidp_rsqn{i}", 0) or 0) for i in range(1, 6)
            )
            ask = sum(
                int(output2.get(f"askp_rsqn{i}", 0) or 0) for i in range(1, 6)
            )
            imbalance = round(bid / ask, 3) if ask > 0 else 1.0
            return {"bid_qty": bid, "ask_qty": ask, "imbalance": imbalance}
        except Exception as e:
            logger.debug(f"호가창 조회 실패 [{ticker}]: {e}")
            return _default

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
        signaled = req.event.wait(timeout=30)

        if not signaled:
            raise TimeoutError(f"KIS API 요청 타임아웃 (30s): {req.path}")
        if req.error:
            raise req.error
        return req.result or {}

    def _worker(self) -> None:
        """큐에서 요청을 꺼내 순서대로 실행하는 워커 스레드."""
        while True:
            req = self._queue.get()
            if req is None:  # 종료 sentinel
                self._queue.task_done()
                break
            try:
                self._rate_limit_wait()
                token = self._get_token()
                result = self._call_api(req, token)
                req.result = result
            except Exception as e:
                msg = str(e)
                if "500" in msg or "장외시간" in msg:
                    logger.debug(f"KIS API 서버 오류 (장외시간): {e}")
                else:
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

        max_retries = 3
        for attempt in range(max_retries):
            try:
                if req.method == "GET":
                    resp = requests.get(url, headers=headers, params=req.params, timeout=10)
                else:
                    resp = requests.post(url, headers=headers, json=req.body, timeout=10)

                # 500 서버 오류 — 장외시간 등으로 재시도 의미 없음 → 즉시 실패
                if resp.status_code == 500:
                    raise RuntimeError(f"KIS API 서버 오류 (500) — 장외시간 가능성")

                resp.raise_for_status()
                data = resp.json()

                if data.get("rt_cd") != "0":
                    raise RuntimeError(f"KIS API 오류 응답: {data.get('msg1')} (rt_cd={data.get('rt_cd')})")

                return data

            except RuntimeError:
                raise  # 500 오류 및 rt_cd 오류는 즉시 전파
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"KIS API 재시도 {attempt + 1}/{max_retries}: {e}")
                    time.sleep(1 * (attempt + 1))
                else:
                    raise RuntimeError(f"KIS API {max_retries}회 실패: {e}") from e

        return {}  # unreachable

    def _get_token(self) -> str:
        """Access Token 반환. 캐시 파일 확인 → 만료 30분 전이면 갱신."""
        with self._token_lock:
            # 메모리 토큰이 유효하면 바로 반환
            if datetime.now() < self._token_expires_at - timedelta(minutes=30):
                return self._access_token
            # 파일 캐시에서 복원 시도 (재시작 시 재발급 방지)
            if self._load_token_cache():
                if datetime.now() < self._token_expires_at - timedelta(minutes=30):
                    logger.info("KIS 토큰 파일 캐시 복원 — 재발급 생략")
                    return self._access_token
            return self._issue_token()

    def _load_token_cache(self) -> bool:
        """파일 캐시에서 토큰 복원. 성공 시 True."""
        try:
            if not self._token_cache_path.exists():
                return False
            data = json.loads(self._token_cache_path.read_text())
            expires_at = datetime.fromisoformat(data["expires_at"])
            if datetime.now() >= expires_at - timedelta(minutes=30):
                return False  # 만료 임박 — 재발급 필요
            self._access_token = data["access_token"]
            self._token_expires_at = expires_at
            return True
        except Exception:
            return False

    def _issue_token(self) -> str:
        """Access Token 신규 발급 후 파일에도 캐시."""
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
        if expires_in <= 0:
            expires_in = 86400
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)

        # 파일 캐시에 저장
        try:
            self._token_cache_path.write_text(json.dumps({
                "access_token": self._access_token,
                "expires_at": self._token_expires_at.isoformat(),
            }))
        except Exception as e:
            logger.warning(f"토큰 캐시 파일 저장 실패: {e}")

        logger.info(
            f"KIS 토큰 발급 완료 — 만료: {self._token_expires_at.strftime('%Y-%m-%d %H:%M:%S')}"
        )
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
