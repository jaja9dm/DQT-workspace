"""
kis_websocket.py — KIS 실시간 WebSocket 클라이언트

역할:
  KIS OpenAPI WebSocket을 통해 보유 종목의 실시간 체결가를 구독한다.
  tick이 들어올 때마다 등록된 콜백을 호출 — 폴링 갭(90초) 없이 즉시 반응.
  연결 끊김 시 5초 후 자동 재연결, 재연결 후 기존 구독 복원.

사용법:
    ws = KISWebSocket()
    ws.subscribe("005930", lambda ticker, price: print(ticker, price))
    ws.unsubscribe("005930")

WebSocket 구독 프로토콜:
  1. REST POST /oauth2/Approval → 인가키 발급
  2. ws://ops.koreainvestment.com:21000 연결
  3. TR_ID=H0STCNT0 구독 메시지 전송 (종목별)
  4. 수신 메시지 파싱 → "0|H0STCNT0|건수|데이터^..." 형식
     데이터 필드[0]=종목코드, 필드[2]=현재가

주의:
  H0STCNT0 구독은 장 중(09:00~15:30)에만 체결 데이터를 수신한다.
  장외 시간에는 연결은 유지되나 메시지가 오지 않음 — 폴링이 계속 백업 역할.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Callable

import requests

from src.config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_WS_URL = "ws://ops.koreainvestment.com:21000"
_APPROVAL_PATH = "/oauth2/Approval"
_RECONNECT_DELAY = 5   # 재연결 대기 (초)
_PING_INTERVAL = 60    # WebSocket 핑 주기 (초)


class KISWebSocket:
    """
    KIS 실시간 WebSocket 클라이언트 — 싱글턴.

    thread-safe. subscribe/unsubscribe는 어느 스레드에서 호출해도 안전.
    """

    _instance: "KISWebSocket | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "KISWebSocket":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._approval_key: str = ""
        self._ws = None                          # websocket.WebSocketApp 인스턴스
        self._connected: bool = False
        self._stop_event = threading.Event()

        # 구독 테이블: {ticker: [callback, ...]}
        self._subscriptions: dict[str, list[Callable[[str, float], None]]] = {}
        self._sub_lock = threading.Lock()

        # 매도 처리 중 종목 (중복 매도 방지)
        self._selling: set[str] = set()

        # WebSocket 연결·수신 루프 시작
        t = threading.Thread(target=self._run_loop, daemon=True, name="kis-ws")
        t.start()
        logger.info("KIS WebSocket 클라이언트 초기화")

    # ──────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────

    def subscribe(self, ticker: str, callback: Callable[[str, float], None]) -> None:
        """실시간 체결가 구독 등록. 연결 중이면 즉시 구독 메시지 전송."""
        with self._sub_lock:
            if ticker not in self._subscriptions:
                self._subscriptions[ticker] = []
                if self._connected:
                    self._send_subscribe(ticker)
            if callback not in self._subscriptions[ticker]:
                self._subscriptions[ticker].append(callback)
        logger.debug(f"[WS] 구독 등록: {ticker}")

    def unsubscribe(self, ticker: str) -> None:
        """구독 해제. 연결 중이면 즉시 해제 메시지 전송."""
        with self._sub_lock:
            if ticker in self._subscriptions:
                del self._subscriptions[ticker]
                if self._connected:
                    self._send_unsubscribe(ticker)
        logger.debug(f"[WS] 구독 해제: {ticker}")

    def mark_selling(self, ticker: str) -> bool:
        """
        매도 처리 시작 표시.
        이미 처리 중이면 False 반환 — 중복 매도 방지.
        """
        with self._sub_lock:
            if ticker in self._selling:
                return False
            self._selling.add(ticker)
            return True

    def clear_selling(self, ticker: str) -> None:
        """매도 처리 완료 후 표시 해제."""
        with self._sub_lock:
            self._selling.discard(ticker)

    def stop(self) -> None:
        """WebSocket 연결 종료 (시스템 종료 시)."""
        self._stop_event.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        # 싱글톤 초기화 — 다음 기동 시 새 인스턴스 생성
        with KISWebSocket._lock:
            KISWebSocket._instance = None

    # ──────────────────────────────────────────
    # 연결 루프
    # ──────────────────────────────────────────

    def _run_loop(self) -> None:
        """자동 재연결 루프 — 연결 끊기면 _RECONNECT_DELAY 후 재시도."""
        while not self._stop_event.is_set():
            try:
                self._connect_and_run()
            except Exception as e:
                logger.warning(f"[WS] 연결 오류: {e} — {_RECONNECT_DELAY}초 후 재연결")
            self._connected = False
            if not self._stop_event.is_set():
                time.sleep(_RECONNECT_DELAY)

    def _connect_and_run(self) -> None:
        """인가키 발급 → WebSocket 연결 → 수신 루프 (블로킹)."""
        try:
            import websocket
        except ImportError:
            raise ImportError("websocket-client 미설치 — 'pip install websocket-client' 실행 필요")

        self._approval_key = self._get_approval_key()
        if not self._approval_key:
            raise ConnectionError("KIS WebSocket 인가키 발급 실패")

        ws = websocket.WebSocketApp(
            _WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws = ws
        ws.run_forever(ping_interval=_PING_INTERVAL, ping_timeout=10)

    # ──────────────────────────────────────────
    # WebSocket 이벤트 핸들러
    # ──────────────────────────────────────────

    def _on_open(self, ws) -> None:
        logger.info("[WS] KIS WebSocket 연결됨")
        self._connected = True
        # 기존 구독 복원 (재연결 시)
        with self._sub_lock:
            for ticker in list(self._subscriptions.keys()):
                self._send_subscribe(ticker)

    def _on_message(self, ws, message: str) -> None:
        try:
            if message.startswith("0|") or message.startswith("1|"):
                self._parse_realtime(message)
            else:
                # JSON 메시지 (구독 확인, PINGPONG 등)
                data = json.loads(message)
                tr_id = data.get("header", {}).get("tr_id", "")
                if tr_id == "PINGPONG":
                    ws.send(message)  # PONG 응답
        except Exception as e:
            logger.debug(f"[WS] 메시지 파싱 오류: {e}")

    def _on_error(self, ws, error) -> None:
        logger.warning(f"[WS] WebSocket 오류: {error}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        logger.info(f"[WS] 연결 종료 — 코드: {close_status_code}")
        self._connected = False

    # ──────────────────────────────────────────
    # 데이터 파싱
    # ──────────────────────────────────────────

    def _parse_realtime(self, message: str) -> None:
        """
        실시간 체결 데이터 파싱.

        메시지 형식: "0|H0STCNT0|건수|필드0^필드1^필드2^..."
        H0STCNT0 주요 필드:
          [0] 종목코드  [1] 체결시간  [2] 현재가  [3] 전일대비부호  ...
        """
        parts = message.split("|")
        if len(parts) < 4:
            return

        tr_id = parts[1]
        if tr_id != "H0STCNT0":
            return

        try:
            count = int(parts[2])
        except ValueError:
            count = 1

        fields = parts[3].split("^")
        # 건수가 여러 개면 필드가 반복됨
        fields_per_item = len(fields) // count if count > 0 else len(fields)

        for i in range(count):
            offset = i * fields_per_item
            if offset + 2 >= len(fields):
                break

            ticker = fields[offset]
            try:
                price = float(fields[offset + 2])
            except (ValueError, IndexError):
                continue

            if price <= 0:
                continue

            with self._sub_lock:
                callbacks = list(self._subscriptions.get(ticker, []))

            for cb in callbacks:
                try:
                    cb(ticker, price)
                except Exception as e:
                    logger.error(f"[WS] 콜백 오류 [{ticker}]: {e}")

    # ──────────────────────────────────────────
    # 구독 메시지 전송
    # ──────────────────────────────────────────

    def _send_subscribe(self, ticker: str) -> None:
        self._send_tr_msg(ticker, tr_type="1")

    def _send_unsubscribe(self, ticker: str) -> None:
        self._send_tr_msg(ticker, tr_type="2")

    def _send_tr_msg(self, ticker: str, tr_type: str) -> None:
        if not self._ws:
            return
        msg = json.dumps({
            "header": {
                "approval_key": self._approval_key,
                "custtype": "P",
                "tr_type": tr_type,
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": "H0STCNT0",
                    "tr_key": ticker,
                }
            },
        })
        try:
            self._ws.send(msg)
        except Exception as e:
            logger.warning(f"[WS] 메시지 전송 실패 [{ticker}]: {e}")

    # ──────────────────────────────────────────
    # KIS 인가키 발급
    # ──────────────────────────────────────────

    def _get_approval_key(self) -> str:
        """WebSocket 접속 인가키 발급 (REST API)."""
        try:
            resp = requests.post(
                f"{settings.KIS_BASE_URL}{_APPROVAL_PATH}",
                headers={"content-type": "application/json"},
                json={
                    "grant_type": "client_credentials",
                    "appkey": settings.KIS_APP_KEY,
                    "secretkey": settings.KIS_APP_SECRET,
                },
                timeout=10,
            )
            resp.raise_for_status()
            key = resp.json().get("approval_key", "")
            if key:
                logger.info("[WS] 인가키 발급 완료")
            return key
        except Exception as e:
            logger.error(f"[WS] 인가키 발급 실패: {e}")
            return ""
