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

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import FinanceDataReader as fdr
import pandas as pd

from src.infra.kis_gateway import KISGateway, RequestPriority
from src.infra.universe import UniverseManager
from src.utils.logger import get_logger
from src.utils.retry import retry_call

logger = get_logger(__name__)

# ── FDR 당일 캐시 ─────────────────────────────────────────────
# FDR OHLCV는 일봉 데이터라 장 중 변하지 않음.
# 매 5분 사이클마다 재요청하는 대신 날짜 단위로 캐시.
# → 첫 사이클에만 네트워크 요청, 이후 사이클은 메모리에서 즉시 반환.
_fdr_cache: dict[str, tuple[str, pd.DataFrame]] = {}  # ticker → (date_str, df)
_fdr_cache_lock = threading.Lock()
_FDR_MIN_ATR_PCT = 1.5   # ATR(%) 최소 기준 — 이하면 수수료 후 수익 불가 종목으로 간주

# 사이클 ID: 현재 시각을 5분 단위로 내림한 문자열 (재시작 후 이어받기 기준)
def _cycle_id() -> str:
    now = datetime.now()
    minute_floor = (now.minute // 5) * 5
    return now.strftime(f"%Y%m%d%H{minute_floor:02d}")

# KIS API 경로
_KIS_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"

# 급등 감지 임계값
VOLUME_SURGE_RATIO = 3.0     # 평균 대비 3배 이상
PRICE_SURGE_PCT = 3.0        # 3% 이상 급등
RSI_OVERBOUGHT = 70.0
RSI_OVERSOLD = 30.0

# 잡주 필터: 당일 거래대금 최소 기준 (원)
# 30억 미만 → 유동성 부족 (3종목 집중 투자 — 체결 슬리피지 최소화)
MIN_TRADING_VALUE = 3_000_000_000   # 30억원

# 체결강도 기준값 (100 = 매수/매도 균형)
EXEC_STRENGTH_BASELINE = 100.0

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

    # 거래대금 (잡주 필터용)
    trading_value: int = 0        # 당일 누적 거래대금 (원) — 10억 미만 = 잡주

    # 수급 (외인·기관 순매수 — KIS 현재가 응답에서 추출, 없으면 0)
    frgn_net_buy: int = 0         # 외국인 순매수량 (주) — 양수=매수우위, 음수=매도우위
    inst_net_buy: int = 0         # 기관 순매수량 (주) — orgn_ntby_qty 필드, 없으면 0

    # 추가 보조지표
    obv_slope: float = 0.0        # OBV 5봉 기울기 — 양수=매수세 유입, 음수=매도세
    bb_width: float = 0.0         # 볼린저밴드 폭 (upper-lower)/mid — 확대=변동성 폭발
    bb_width_ratio: float = 1.0   # 현재 BB폭 / 20봉 평균 BB폭 — 1.3↑=스퀴즈 돌파
    stoch_rsi: float = 50.0       # Stochastic RSI(14,14) — 80↑=단기 과매수, 20↓=과매도
    momentum_score: float = 0.0   # 종합 모멘텀 점수 (0~130) — Gate 4.2 순위 기준
    at_new_high: bool = False      # 120일 신고가 돌파 여부 — 저항 없는 상승 구간

    # 당일 가격 범위
    day_high: float = 0.0         # 당일 고가 (KIS stck_hgpr)
    day_low: float = 0.0          # 당일 저가 (KIS stck_lwpr)
    day_open: float = 0.0         # 당일 시가 (KIS stck_oprc)
    day_range_pos: float = 0.5    # (현재가-저가)/(고가-저가) — 0=저가권, 1=고가권

    # 갭업 돌파 플래그
    is_gap_up: bool = False        # 전일 종가 대비 +8% 이상 갭업 (돌파매매 대상)
    gap_up_pct: float = 0.0        # 갭업 비율 (≈ change_pct, 당일 등락률 활용)

    # 장중 위치 (눌림목 반등 감지용)
    intraday_chg_pct: float = 0.0  # (현재가 - 시가) / 시가 × 100 — 양수=시가 대비 상승, 음수=하락

    # 체결강도 (KIS tntm_vol_tnrt, 없으면 vol_ratio+등락률 근사)
    # 100 기준 — 130↑ 강한 매수세(FOMO), 80↓ 매도세 우위
    exec_strength: float = 100.0

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

def _fetch_price_from_kis(ticker: str) -> tuple[float, float, int, int, int, int, float, float]:
    """
    KIS API로 현재가·등락률·거래량·거래대금·외인순매수·기관순매수·당일고가·당일저가 조회.

    Returns:
        (current_price, change_pct, volume, trading_value, frgn_net_buy, inst_net_buy, day_high, day_low)
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
        trading_value = int(output.get("acml_tr_pbmn", 0) or 0)
        frgn_net_buy = int(output.get("frgn_ntby_qty", 0) or 0)
        inst_net_buy = int(output.get("orgn_ntby_qty", 0) or 0)
        day_high = float(output.get("stck_hgpr", 0) or 0)
        day_low  = float(output.get("stck_lwpr", 0) or 0)
        day_open = float(output.get("stck_oprc", 0) or 0)
        # 체결강도: KIS tntm_vol_tnrt 필드 직접 사용, 없으면 등락률+거래량으로 근사
        _raw_es = float(output.get("tntm_vol_tnrt", 0) or 0)
        if _raw_es > 10.0:
            exec_strength = _raw_es
        else:
            # 등락률 방향 + 거래량 비율로 근사:
            # 양봉+거래량 급등 → 매수세, 음봉 → 매도세
            avg_vol_approx = float(output.get("lstn_stcn_avrg_vol", 0) or 0)
            vol_ratio_approx = (volume / avg_vol_approx) if avg_vol_approx > 0 else 1.0
            if change_pct >= 0:
                exec_strength = 100.0 + min(60.0, (vol_ratio_approx - 1.0) * 25.0)
            else:
                exec_strength = 100.0 - min(40.0, (vol_ratio_approx - 1.0) * 20.0)
            exec_strength = max(30.0, min(200.0, exec_strength))
        return price, change_pct, volume, trading_value, frgn_net_buy, inst_net_buy, day_high, day_low, day_open, exec_strength
    except Exception as e:
        logger.debug(f"KIS 현재가 실패 [{ticker}]: {e}")
        return 0.0, 0.0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 100.0


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
        today_str = datetime.now().strftime("%Y%m%d")
        # 당일 캐시 확인 (FDR 일봉 데이터는 장 중 변경 없음 — 재요청 불필요)
        with _fdr_cache_lock:
            cached = _fdr_cache.get(ticker)
        if cached and cached[0] == today_str:
            df = cached[1]
        else:
            end = datetime.now().date()
            start = end - timedelta(days=120)
            df = retry_call(fdr.DataReader, ticker, start, end, max_attempts=3, base_delay=2.0)
            with _fdr_cache_lock:
                _fdr_cache[ticker] = (today_str, df)

        if df.empty or len(df) < 20:
            return _default

        # ATR 최소 기준 필터: 일봉 기준 평균 변동폭 < 1.5% 종목은 수수료 후 수익 불가
        try:
            tr_pct = ((df["High"] - df["Low"]) / df["Close"]).tail(14).mean() * 100
            if tr_pct < _FDR_MIN_ATR_PCT:
                _default["_low_atr"] = True  # 호출측에서 스킵 여부 판단
                return _default
        except Exception:
            pass

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

        # ── 추가 보조지표 계산 ──────────────────────────────────

        # OBV (On-Balance Volume): 거래량이 매수세인지 매도세인지 확인
        # 가격 상승 시 거래량 누적(+), 하락 시 차감(-) → 5봉 기울기로 방향 판단
        obv_slope = 0.0
        try:
            vol_s = df["Volume"].astype(float)
            close_s = df["Close"].astype(float)
            direction = close_s.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
            obv = (direction * vol_s).cumsum()
            if len(obv) >= 5:
                obv_slope = float(obv.iloc[-1] - obv.iloc[-5]) / (float(vol_s.iloc[-5:].mean()) + 1e-9)
        except Exception:
            obv_slope = 0.0

        # BB Width: 볼린저밴드 폭 — 스퀴즈 후 폭발 패턴 감지
        # bb_width_ratio > 1.2: 현재 폭이 최근 20봉 평균보다 20% 이상 확대 → 변동성 폭발
        bb_width = 0.0
        bb_width_ratio = 1.0
        try:
            ma20_s = close.rolling(_BB_PERIOD).mean()
            std20_s = close.rolling(_BB_PERIOD).std()
            bb_up_s = ma20_s + _BB_STD * std20_s
            bb_lo_s = ma20_s - _BB_STD * std20_s
            width_s = (bb_up_s - bb_lo_s) / ma20_s.replace(0, float("nan"))
            if len(width_s.dropna()) >= 20:
                bb_width = float(width_s.iloc[-1]) if not _isnan(float(width_s.iloc[-1])) else 0.0
                avg_width = float(width_s.dropna().iloc[-20:].mean())
                bb_width_ratio = round(bb_width / avg_width, 3) if avg_width > 0 else 1.0
        except Exception:
            bb_width = 0.0
            bb_width_ratio = 1.0

        # Stochastic RSI (14,14): RSI의 스토캐스틱 — 더 민감한 단기 과매수/과매도
        stoch_rsi = 50.0
        try:
            delta = close.diff()
            gain  = delta.clip(lower=0).rolling(_RSI_PERIOD).mean()
            loss  = (-delta.clip(upper=0)).rolling(_RSI_PERIOD).mean()
            rs    = gain / loss.replace(0, float("nan"))
            rsi_s = 100 - (100 / (1 + rs))
            rsi_14 = rsi_s.dropna()
            if len(rsi_14) >= _RSI_PERIOD:
                rsi_window = rsi_14.rolling(_RSI_PERIOD)
                rsi_min = rsi_window.min()
                rsi_max = rsi_window.max()
                rsi_range = rsi_max - rsi_min
                stoch = (rsi_14 - rsi_min) / rsi_range.replace(0, float("nan")) * 100
                stoch_rsi_val = float(stoch.iloc[-1])
                stoch_rsi = round(stoch_rsi_val, 1) if not _isnan(stoch_rsi_val) else 50.0
        except Exception:
            stoch_rsi = 50.0

        # 종합 모멘텀 점수 (0~130): Gate 4.2에서 종목 우선순위 결정에 사용
        # 거래량(35%) + MACD(25%) + BB폭(20%) + OBV(20%) + 120일신고가(+10)
        try:
            vol_score   = min(volume_ratio / 5.0, 1.0) * 35
            macd_score  = (25 if macd_hist > 0 and macd_hist > macd_hist_prev else
                           15 if macd_hist > macd_hist_prev else 0)
            bbw_score   = min((bb_width_ratio - 1.0) / 0.5, 1.0) * 20 if bb_width_ratio > 1.0 else 0.0
            obv_score   = 20 if obv_slope > 0 else 0.0
            momentum_score = round(vol_score + macd_score + bbw_score + obv_score, 1)
        except Exception:
            momentum_score = 0.0

        # 120일 신고가 돌파 여부 (저항선 없는 상승 — 강한 모멘텀 신호)
        at_new_high = False
        try:
            high_120d = float(df["High"].iloc[:-1].tail(120).max()) if len(df) >= 2 else 0.0
            if high_120d > 0 and ref >= high_120d * 0.99:
                at_new_high = True
                momentum_score = round(momentum_score + 10.0, 1)
        except Exception:
            pass

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
            # 추가 지표
            "obv_slope": round(obv_slope, 4),
            "bb_width": round(bb_width, 4),
            "bb_width_ratio": bb_width_ratio,
            "stoch_rsi": stoch_rsi,
            "momentum_score": momentum_score,
            "at_new_high": at_new_high,
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
    price, change_pct, volume, trading_value, frgn_net_buy, inst_net_buy, day_high, day_low, day_open, exec_strength = _fetch_price_from_kis(ticker)
    snap.current_price = price
    snap.change_pct = change_pct
    snap.volume = volume
    snap.trading_value = trading_value
    snap.frgn_net_buy = frgn_net_buy
    snap.inst_net_buy = inst_net_buy
    snap.day_high = day_high
    snap.day_low  = day_low
    snap.day_open = day_open
    snap.exec_strength = exec_strength
    # 당일 가격 범위 내 현재 위치 (0=저가권, 1=고가권)
    day_range = day_high - day_low
    snap.day_range_pos = round((price - day_low) / day_range, 3) if day_range > 0 else 0.5
    # 갭업 돌파 플래그: +8% 이상 갭업이면 돌파매매 대상
    snap.gap_up_pct = change_pct
    snap.is_gap_up  = change_pct >= 8.0
    # 장중 위치: 시가 대비 현재가 위치 (눌림목 반등 감지)
    snap.intraday_chg_pct = round((price - day_open) / day_open * 100, 2) if day_open > 0 else 0.0

    # 2. 기술지표 (FDR + pandas-ta)
    ind = _compute_indicators(ticker, price, volume)
    # ATR 최소 기준 미달 종목: 수수료 후 수익 불가 → 스냅샷 오류 처리
    if ind.get("_low_atr"):
        snap.error = f"ATR < {_FDR_MIN_ATR_PCT}% (수수료 후 수익 불가)"
        return snap
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
    snap.obv_slope      = ind.get("obv_slope", 0.0)
    snap.bb_width       = ind.get("bb_width", 0.0)
    snap.bb_width_ratio = ind.get("bb_width_ratio", 1.0)
    snap.stoch_rsi      = ind.get("stoch_rsi", 50.0)
    snap.momentum_score = ind.get("momentum_score", 0.0)
    snap.at_new_high    = ind.get("at_new_high", False)

    # 거래대금 가중치: 단타 유동성 가산점 (같은 기술 점수면 거래대금 높은 종목 우선)
    # 이수페타시스(1.1조)와 나노캠텍(53억)이 동점 되는 문제 해결
    if trading_value >= 200_000_000_000:    # 2000억↑
        snap.momentum_score = min(130.0, snap.momentum_score + 25.0)
    elif trading_value >= 50_000_000_000:   # 500억↑
        snap.momentum_score = min(130.0, snap.momentum_score + 15.0)
    elif trading_value >= 10_000_000_000:   # 100억↑
        snap.momentum_score = min(130.0, snap.momentum_score + 5.0)

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


# ── 재시도 포함 단일 종목 스캔 ──────────────────────────────

def _scan_ticker_safe(ticker: str, name: str, max_attempts: int = 3) -> StockSnapshot:
    """
    재시도 포함 단일 종목 스캔.
    네트워크 오류 시 지수 백오프로 최대 max_attempts회 시도.
    최종 실패 시 error 필드를 채운 빈 스냅샷 반환 (예외 전파 안 함).
    """
    delay = 2.0
    for attempt in range(1, max_attempts + 1):
        try:
            return _scan_ticker(ticker, name)
        except Exception as e:
            if attempt == max_attempts:
                logger.warning(f"종목 스캔 최종 실패 [{ticker}]: {e}")
                snap = StockSnapshot(ticker=ticker, name=name)
                snap.error = str(e)[:200]
                return snap
            logger.warning(
                f"종목 스캔 재시도 [{ticker}] ({attempt}/{max_attempts}), "
                f"{delay:.1f}초 후: {e}"
            )
            time.sleep(delay)
            delay = min(delay * 2, 15.0)
    return StockSnapshot(ticker=ticker, name=name)


# ── 통합 수집 (체크포인트 + 재시도) ──────────────────────────

def collect(max_workers: int = 10) -> UniverseScan:
    """
    유니버스 전체 스캔.

    체크포인트 기반 중단 재개:
      - 5분 단위 cycle_id로 현재 사이클을 식별
      - 이미 완료된 종목은 DB fetch_checkpoint에서 확인해 건너뜀
      - 각 종목 완료 후 즉시 DB에 기록 → 재시작 시 이어받기 가능

    KIS API 레이트 리밋(10 req/s) 안에서 순차 처리.

    Returns:
        UniverseScan 인스턴스
    """
    from datetime import date as _date
    from src.infra.database import execute as db_exec, fetch_all

    cycle = _cycle_id()
    um = UniverseManager()
    tickers = um.get_today()

    scan = UniverseScan(
        timestamp=datetime.now().isoformat(timespec="seconds"),
        total_scanned=len(tickers),
    )

    if not tickers:
        logger.warning("유니버스가 비어 있음 — 스캔 건너뜀")
        return scan

    # 이번 사이클에서 이미 완료된 종목 확인 (중단 후 재시작 시 이어받기)
    done_rows = fetch_all(
        "SELECT item_key FROM fetch_checkpoint "
        "WHERE cycle_id = ? AND scan_type = 'domestic_stock' AND status = 'done'",
        (cycle,),
    )
    done_set = {r["item_key"] for r in done_rows}

    # 이름 맵
    rows = fetch_all(
        "SELECT ticker, name FROM universe WHERE active_date = ?",
        (str(_date.today()),),
    )
    name_map = {r["ticker"]: r["name"] for r in rows}

    remaining = [t for t in tickers if t not in done_set]
    if done_set:
        logger.info(
            f"[체크포인트 복원] 사이클 {cycle} — "
            f"완료 {len(done_set)}개 건너뜀, 남은 {len(remaining)}개 재개"
        )
    logger.info(f"종목 스캔 시작 — {len(remaining)}종목")

    for ticker in remaining:
        snap = _scan_ticker_safe(ticker, name_map.get(ticker, ""))
        scan.snapshots.append(snap)

        # 체크포인트 기록 (종목 완료마다 즉시 저장)
        status = "error" if snap.error else "done"
        try:
            db_exec(
                """
                INSERT OR REPLACE INTO fetch_checkpoint
                    (cycle_id, scan_type, item_key, status, error_msg, fetched_at)
                VALUES (?, 'domestic_stock', ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (cycle, ticker, status, snap.error[:200] if snap.error else None),
            )
        except Exception as e:
            logger.debug(f"체크포인트 저장 실패 [{ticker}]: {e}")

    # 후보 필터 1: 신호 기반 후보 분류
    # ① 모멘텀 후보: 거래량급증+가격급등+BB돌파 중 2개↑, 또는 거래량 5배↑ 단독
    # ② 눌림목 반등 후보: 오늘 하락 + OBV양수 + 적정 거래량 (기존 필터에서 전부 탈락하던 문제 수정)
    # ③ 오프닝급락 반등 후보: 시가 대비 -3%↓ 급락 후 저점 반등 중 (장초반)
    _now_hm_c = int(datetime.now().strftime("%H%M"))

    def _signal_count(s: StockSnapshot) -> int:
        return int(s.is_volume_surge) + int(s.is_price_surge) + int(s.is_breakout)

    def _is_pullback_candidate(s: StockSnapshot) -> bool:
        return (
            -5.0 <= s.change_pct <= -0.3   # 소폭~중폭 하락
            and s.volume_ratio >= 1.5       # 평균 이상 거래량
            and s.obv_slope > 0             # OBV 매수세 유입
            and s.rsi <= 75                 # 과열 아님
        )

    def _is_op_plunge_candidate(s: StockSnapshot) -> bool:
        return (
            _now_hm_c <= 1030               # 장초반 10:30 이전
            and s.intraday_chg_pct <= -3.0  # 시가 대비 -3%↓ 급락
            and s.day_range_pos >= 0.15     # 저점에서 반등 시작
        )

    raw_candidates = [
        s for s in scan.snapshots
        if not s.error and (
            _signal_count(s) >= 2
            or (s.is_volume_surge and s.volume_ratio >= 5.0)
            or _is_pullback_candidate(s)
            or _is_op_plunge_candidate(s)
        )
    ]
    skipped = len([s for s in scan.snapshots if not s.error]) - len(raw_candidates)
    if skipped:
        logger.info(f"신호 필터: {skipped}종목 제외 (복합 조건·눌림·오프닝 모두 미충족)")

    # 후보 필터 2: 잡주 제외 — 당일 거래대금 기준
    # 장중에는 최종 거래대금이 낮을 수 있으므로:
    #   - 거래대금이 집계됐고 MIN_TRADING_VALUE 미만이면 제외
    #   - 거래대금이 0이면 (조회 실패 등) 거래량 급증 여부로 대체 판단
    filtered_out = 0
    scan.candidates = []
    for s in raw_candidates:
        if s.trading_value == 0:
            # 거래대금 조회 실패 → 거래량 급증인 경우만 포함 (최소 유동성 보장)
            if s.is_volume_surge or s.volume > 100_000:
                scan.candidates.append(s)
            else:
                filtered_out += 1
        elif s.trading_value < MIN_TRADING_VALUE:
            filtered_out += 1  # 잡주 제외
        else:
            scan.candidates.append(s)

    if filtered_out:
        logger.info(f"잡주 필터: {filtered_out}종목 제외 (거래대금 30억 미만)")

    logger.info(
        f"스캔 완료 — {len(scan.snapshots)}종목 / "
        f"후보 {len(scan.candidates)}종목 "
        f"(거래량급증 {sum(1 for s in scan.snapshots if s.is_volume_surge)}개, "
        f"가격급등 {sum(1 for s in scan.snapshots if s.is_price_surge)}개, "
        f"BB돌파 {sum(1 for s in scan.snapshots if s.is_breakout)}개)"
    )

    # 오래된 체크포인트 정리 (오늘 이전 것 삭제, DB 비대화 방지)
    try:
        today_prefix = datetime.now().strftime("%Y%m%d")
        db_exec(
            "DELETE FROM fetch_checkpoint WHERE cycle_id < ? AND scan_type = 'domestic_stock'",
            (today_prefix,),
        )
    except Exception:
        pass

    return scan
