"""
collector.py — 국내 주식팀 데이터 수집 모듈

수집 대상:
  - KIS API: 유니버스 종목 실시간 현재가·거래량·전일 대비
  - FinanceDataReader: 60일 OHLCV (기술지표 계산 원본)
  - pandas-ta: RSI(14), MACD(12/26/9), 볼린저밴드(20/2), 거래량 비율

스캔 대상: UniverseManager에서 확정된 당일 ~450종목만.
KIS API는 반드시 KISGateway 경유.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import FinanceDataReader as fdr
import pandas as pd

from src.infra.kis_gateway import KISGateway, RequestPriority
from src.infra.universe import UniverseManager
from src.utils.logger import get_logger

logger = get_logger(__name__)

# KIS API 경로
_KIS_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"

# 급등 감지 임계값
VOLUME_SURGE_RATIO = 3.0     # 평균 대비 3배 이상
PRICE_SURGE_PCT = 3.0        # 3% 이상 급등
RSI_OVERBOUGHT = 70.0
RSI_OVERSOLD = 30.0

# 기술지표 파라미터
_RSI_PERIOD = 14
_MACD_FAST = 12
_MACD_SLOW = 26
_MACD_SIGNAL = 9
_BB_PERIOD = 20
_BB_STD = 2.0
_MA_VOL_PERIOD = 20          # 거래량 이동평균 (평균 거래량 산출)


# ── 데이터 클래스 ─────────────────────────────────────────────

@dataclass
class StockSnapshot:
    """종목 단일 스냅샷 — KIS 실시간 + FDR 기술지표."""
    ticker: str = ""
    name: str = ""

    # 실시간 (KIS)
    current_price: float = 0.0
    change_pct: float = 0.0        # 전일 대비 등락률 (%)
    volume: int = 0                # 당일 누적 거래량
    volume_ratio: float = 0.0     # 현재 거래량 / 20일 평균 거래량

    # 기술지표 (FDR + pandas-ta)
    rsi: float = 50.0
    macd: float = 0.0
    macd_signal: float = 0.0
    macd_hist: float = 0.0
    bb_upper: float = 0.0
    bb_mid: float = 0.0
    bb_lower: float = 0.0
    bb_position: float = 0.5      # (price - lower) / (upper - lower), 0~1

    # 60일 이동평균
    ma5: float = 0.0
    ma20: float = 0.0
    ma60: float = 0.0

    # 신호 플래그
    is_volume_surge: bool = False
    is_price_surge: bool = False
    is_breakout: bool = False      # 볼린저밴드 상단 돌파
    above_ma20: bool = False

    # MACD 일봉 상태
    macd_hist_prev: float = 0.0   # 직전 봉 히스토그램 (Pre-Cross 감지용)
    daily_macd_ok: bool = False   # 일봉 MACD 강세 여부 (True → 매매 허용)

    error: str = ""               # 수집 오류 메시지


@dataclass
class UniverseScan:
    """전체 유니버스 스캔 결과."""
    timestamp: str = ""
    total_scanned: int = 0
    snapshots: list[StockSnapshot] = field(default_factory=list)

    # 사전 필터링 후보군 (애널라이저가 판단할 종목)
    candidates: list[StockSnapshot] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ── KIS 현재가 조회 ───────────────────────────────────────────

def _fetch_price_from_kis(ticker: str) -> tuple[float, float, int]:
    """
    KIS API로 현재가·등락률·거래량 조회.

    Returns:
        (current_price, change_pct, volume)
    """
    gw = KISGateway()
    try:
        resp = gw.request(
            method="GET",
            path=_KIS_PRICE_PATH,
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
            },
            tr_id="FHKST01010100",
            priority=RequestPriority.DATA_COLLECTION,
        )
        output = resp.get("output", {})
        price = float(output.get("stck_prpr", 0) or 0)
        change_pct = float(output.get("prdy_ctrt", 0) or 0)
        volume = int(output.get("acml_vol", 0) or 0)
        return price, change_pct, volume
    except Exception as e:
        logger.debug(f"KIS 현재가 실패 [{ticker}]: {e}")
        return 0.0, 0.0, 0


# ── FDR 기술지표 계산 ─────────────────────────────────────────

def _compute_indicators(ticker: str, current_price: float, current_volume: int) -> dict:
    """
    FinanceDataReader + pandas-ta로 기술지표 계산.

    Returns:
        기술지표 딕셔너리 (실패 시 기본값)
    """
    _default = {
        "rsi": 50.0, "macd": 0.0, "macd_signal": 0.0, "macd_hist": 0.0,
        "macd_hist_prev": 0.0,
        "bb_upper": 0.0, "bb_mid": 0.0, "bb_lower": 0.0, "bb_position": 0.5,
        "ma5": 0.0, "ma20": 0.0, "ma60": 0.0,
        "volume_ratio": 0.0, "is_breakout": False, "above_ma20": False,
    }

    try:
        end = datetime.now().date()
        start = end - timedelta(days=120)  # 60일 지표 계산용 여유분
        df = fdr.DataReader(ticker, start, end)
        if df.empty or len(df) < 20:
            return _default

        close = df["Close"].astype(float)
        volume_series = df["Volume"].astype(float)

        # 이동평균
        ma5 = float(close.rolling(5).mean().iloc[-1]) if len(close) >= 5 else 0.0
        ma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else 0.0
        ma60 = float(close.rolling(60).mean().iloc[-1]) if len(close) >= 60 else 0.0

        # 거래량 비율
        avg_vol = float(volume_series.rolling(_MA_VOL_PERIOD).mean().iloc[-1]) if len(volume_series) >= _MA_VOL_PERIOD else 0.0
        volume_ratio = round(current_volume / avg_vol, 2) if avg_vol > 0 else 0.0

        # pandas-ta 사용 시도 (설치 안 돼 있으면 수동 계산)
        try:
            import pandas_ta as ta
            df_ta = pd.DataFrame({"close": close, "high": df["High"].astype(float), "low": df["Low"].astype(float)})

            # RSI
            rsi_series = ta.rsi(df_ta["close"], length=_RSI_PERIOD)
            rsi = float(rsi_series.iloc[-1]) if rsi_series is not None and not rsi_series.empty else 50.0

            # MACD
            macd_df = ta.macd(df_ta["close"], fast=_MACD_FAST, slow=_MACD_SLOW, signal=_MACD_SIGNAL)
            if macd_df is not None and not macd_df.empty:
                macd = float(macd_df.iloc[-1, 0])           # MACD
                macd_signal = float(macd_df.iloc[-1, 2])    # Signal
                macd_hist = float(macd_df.iloc[-1, 1])      # Histogram
                macd_hist_prev = float(macd_df.iloc[-2, 1]) if len(macd_df) >= 2 else macd_hist
            else:
                macd = macd_signal = macd_hist = macd_hist_prev = 0.0

            # 볼린저밴드
            bb_df = ta.bbands(df_ta["close"], length=_BB_PERIOD, std=_BB_STD)
            if bb_df is not None and not bb_df.empty:
                bb_upper = float(bb_df.iloc[-1, 0])
                bb_mid = float(bb_df.iloc[-1, 1])
                bb_lower = float(bb_df.iloc[-1, 2])
            else:
                bb_upper = bb_mid = bb_lower = 0.0

        except ImportError:
            # pandas-ta 미설치 — 수동 계산
            rsi = _calc_rsi_manual(close)
            macd, macd_signal, macd_hist, macd_hist_prev = _calc_macd_manual(close)
            bb_upper, bb_mid, bb_lower = _calc_bb_manual(close)

        # BB 위치 (0~1)
        bb_range = bb_upper - bb_lower
        ref = current_price if current_price > 0 else float(close.iloc[-1])
        bb_position = round((ref - bb_lower) / bb_range, 3) if bb_range > 0 else 0.5
        bb_position = max(0.0, min(1.0, bb_position))

        return {
            "rsi": round(rsi, 2) if not _isnan(rsi) else 50.0,
            "macd": round(macd, 4) if not _isnan(macd) else 0.0,
            "macd_signal": round(macd_signal, 4) if not _isnan(macd_signal) else 0.0,
            "macd_hist": round(macd_hist, 4) if not _isnan(macd_hist) else 0.0,
            "macd_hist_prev": round(macd_hist_prev, 4) if not _isnan(macd_hist_prev) else 0.0,
            "bb_upper": round(bb_upper, 2) if not _isnan(bb_upper) else 0.0,
            "bb_mid": round(bb_mid, 2) if not _isnan(bb_mid) else 0.0,
            "bb_lower": round(bb_lower, 2) if not _isnan(bb_lower) else 0.0,
            "bb_position": bb_position,
            "ma5": round(ma5, 2) if not _isnan(ma5) else 0.0,
            "ma20": round(ma20, 2) if not _isnan(ma20) else 0.0,
            "ma60": round(ma60, 2) if not _isnan(ma60) else 0.0,
            "volume_ratio": volume_ratio,
            "is_breakout": bool(ref > bb_upper > 0),
            "above_ma20": bool(ref > ma20 > 0),
        }

    except Exception as e:
        logger.debug(f"기술지표 계산 실패 [{ticker}]: {e}")
        return _default


# ── 수동 지표 계산 (pandas-ta 폴백) ──────────────────────────

def _calc_rsi_manual(close: pd.Series) -> float:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(_RSI_PERIOD).mean()
    loss = (-delta.clip(upper=0)).rolling(_RSI_PERIOD).mean()
    rs = gain / loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return float(val) if not _isnan(val) else 50.0


def _calc_macd_manual(close: pd.Series) -> tuple[float, float, float, float]:
    ema_fast = close.ewm(span=_MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=_MACD_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=_MACD_SIGNAL, adjust=False).mean()
    hist = macd_line - signal_line
    hist_prev = float(hist.iloc[-2]) if len(hist) >= 2 else float(hist.iloc[-1])
    return float(macd_line.iloc[-1]), float(signal_line.iloc[-1]), float(hist.iloc[-1]), hist_prev


def _calc_bb_manual(close: pd.Series) -> tuple[float, float, float]:
    mid = close.rolling(_BB_PERIOD).mean()
    std = close.rolling(_BB_PERIOD).std()
    upper = mid + _BB_STD * std
    lower = mid - _BB_STD * std
    return float(upper.iloc[-1]), float(mid.iloc[-1]), float(lower.iloc[-1])


def _isnan(val: float) -> bool:
    import math
    try:
        return math.isnan(val)
    except (TypeError, ValueError):
        return True


# ── 단일 종목 스냅샷 ──────────────────────────────────────────

def _scan_ticker(ticker: str, name: str) -> StockSnapshot:
    """단일 종목 전체 수집 (KIS 현재가 + FDR 기술지표)."""
    snap = StockSnapshot(ticker=ticker, name=name)

    # 1. KIS 현재가
    price, change_pct, volume = _fetch_price_from_kis(ticker)
    snap.current_price = price
    snap.change_pct = change_pct
    snap.volume = volume

    # 2. 기술지표 (FDR + pandas-ta)
    ind = _compute_indicators(ticker, price, volume)
    snap.rsi = ind["rsi"]
    snap.macd = ind["macd"]
    snap.macd_signal = ind["macd_signal"]
    snap.macd_hist = ind["macd_hist"]
    snap.macd_hist_prev = ind["macd_hist_prev"]
    snap.bb_upper = ind["bb_upper"]
    snap.bb_mid = ind["bb_mid"]
    snap.bb_lower = ind["bb_lower"]
    snap.bb_position = ind["bb_position"]
    snap.ma5 = ind["ma5"]
    snap.ma20 = ind["ma20"]
    snap.ma60 = ind["ma60"]
    snap.volume_ratio = ind["volume_ratio"]
    snap.is_breakout = ind["is_breakout"]
    snap.above_ma20 = ind["above_ma20"]

    # 일봉 MACD 강세 여부 (is_daily_macd_bullish 유틸 사용)
    from src.utils.macd import is_daily_macd_bullish
    snap.daily_macd_ok = is_daily_macd_bullish(
        macd_val=snap.macd,
        signal_val=snap.macd_signal,
        hist_val=snap.macd_hist,
        prev_hist_val=snap.macd_hist_prev,
    )

    # 3. 신호 플래그
    snap.is_volume_surge = snap.volume_ratio >= VOLUME_SURGE_RATIO
    snap.is_price_surge = snap.change_pct >= PRICE_SURGE_PCT

    return snap


# ── 통합 수집 ─────────────────────────────────────────────────

def collect(max_workers: int = 10) -> UniverseScan:
    """
    유니버스 전체 스캔.

    KIS API 레이트 리밋(10 req/s) 안에서 순차 처리.
    max_workers 설정은 향후 병렬화 시 사용.

    Returns:
        UniverseScan 인스턴스
    """
    um = UniverseManager()
    tickers = um.get_today()

    scan = UniverseScan(
        timestamp=datetime.now().isoformat(timespec="seconds"),
        total_scanned=len(tickers),
    )

    if not tickers:
        logger.warning("유니버스가 비어 있음 — 스캔 건너뜀")
        return scan

    logger.info(f"종목 스캔 시작 — {len(tickers)}종목")

    # 유니버스에서 이름 가져오기
    from src.infra.database import fetch_all
    from datetime import date
    rows = fetch_all(
        "SELECT ticker, name FROM universe WHERE active_date = ?",
        (str(date.today()),),
    )
    name_map = {r["ticker"]: r["name"] for r in rows}

    for ticker in tickers:
        snap = _scan_ticker(ticker, name_map.get(ticker, ""))
        scan.snapshots.append(snap)

    # 후보 필터: 신호가 하나라도 있는 종목
    scan.candidates = [
        s for s in scan.snapshots
        if s.is_volume_surge or s.is_price_surge or s.is_breakout
    ]

    logger.info(
        f"스캔 완료 — {len(scan.snapshots)}종목 / "
        f"후보 {len(scan.candidates)}종목 "
        f"(거래량급증 {sum(1 for s in scan.snapshots if s.is_volume_surge)}개, "
        f"가격급등 {sum(1 for s in scan.snapshots if s.is_price_surge)}개, "
        f"BB돌파 {sum(1 for s in scan.snapshots if s.is_breakout)}개)"
    )
    return scan
