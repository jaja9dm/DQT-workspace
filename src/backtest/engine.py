"""
backtest/engine.py — DQT 전략 백테스트 엔진

시뮬레이션 규칙:
  - 신호: 당일 종가 기준 지표 계산 (end-of-day signal)
  - 진입: 신호 다음날 시가 (look-ahead bias 방지)
  - 청산: 진입 후 당일 고/저가로 손절·목표 히트 체크
           미체결 시 최대 N일 후 시가 청산 (time-cut)
  - Claude Gate 5: Gate 4.2 점수 ≥ 72 → 매수 (룰 대체)
  - 비용: 왕복 0.4% (수수료 + 거래세 + 슬리피지)
  - 최대 동시 보유: 3종목 / 종목당 균등 배분

사용 예:
    from src.backtest.engine import Backtester
    bt = Backtester(start="2024-01-01", end="2024-12-31")
    result = bt.run()
    result.print_summary()
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── 백테스트 기본 파라미터 ────────────────────────────────────
_COMMISSION_RT  = 0.004   # 왕복 수수료+거래세+슬리피지 0.4%
_STOP_PCT       = 0.0175  # 평균 손절선 1.75%
_TARGET_PCT     = 0.055   # 평균 목표 수익 5.5%
_TRAILING_TRIGGER = 0.03  # 트레일링 시작 수익률 3%
_TRAILING_FLOOR   = 0.025 # 트레일링 간격 2.5%
_MAX_HOLD_DAYS  = 5       # 최대 보유일 (이후 시가 청산)
_MAX_POSITIONS  = 3       # 최대 동시 보유 종목
_MIN_VOL_RATIO  = 2.0     # 최소 거래량 비율
_RSI_MAX        = 82.0    # RSI 하드 상한
_RSI_MIN        = 35.0    # RSI 하드 하한
_SCORE_ENTER    = 50.0    # 진입 최소 점수
_SCORE_FULL     = 72.0    # 풀사이즈 최소 점수

# 기술지표 파라미터
_RSI_P   = 14
_MACD_F  = 12
_MACD_S  = 26
_MACD_SIG = 9
_BB_P    = 20
_BB_STD  = 2.0
_VOL_MA  = 20


# ── 데이터 클래스 ────────────────────────────────────────────

@dataclass
class Trade:
    ticker:       str
    entry_date:   date
    exit_date:    date
    entry_price:  float
    exit_price:   float
    size_mult:    float       # 0.75 or 1.0
    exit_reason:  str         # target | stop | trailing | timecut
    score:        float
    pnl_pct:      float = 0.0
    pnl_amt:      float = 0.0

    def __post_init__(self):
        gross = (self.exit_price - self.entry_price) / self.entry_price
        self.pnl_pct = round((gross - _COMMISSION_RT) * 100, 3)


@dataclass
class BacktestResult:
    trades:          list[Trade]        = field(default_factory=list)
    equity_curve:    list[float]        = field(default_factory=list)
    initial_capital: float              = 1_000_000.0
    start:           str                = ""
    end:             str                = ""
    universe_size:   int                = 0

    # ── 파생 지표 ────────────────────────────────────────────

    @property
    def total_trades(self) -> int:
        return len(self.trades)

    @property
    def wins(self) -> list[Trade]:
        return [t for t in self.trades if t.pnl_pct > 0]

    @property
    def losses(self) -> list[Trade]:
        return [t for t in self.trades if t.pnl_pct <= 0]

    @property
    def win_rate(self) -> float:
        return len(self.wins) / self.total_trades if self.total_trades else 0.0

    @property
    def avg_win(self) -> float:
        return sum(t.pnl_pct for t in self.wins) / len(self.wins) if self.wins else 0.0

    @property
    def avg_loss(self) -> float:
        return sum(t.pnl_pct for t in self.losses) / len(self.losses) if self.losses else 0.0

    @property
    def profit_factor(self) -> float:
        gross_win  = sum(t.pnl_pct for t in self.wins)
        gross_loss = abs(sum(t.pnl_pct for t in self.losses))
        return round(gross_win / gross_loss, 2) if gross_loss > 0 else 0.0

    @property
    def total_return_pct(self) -> float:
        if not self.equity_curve:
            return 0.0
        return round((self.equity_curve[-1] / self.initial_capital - 1) * 100, 2)

    @property
    def max_drawdown_pct(self) -> float:
        if not self.equity_curve:
            return 0.0
        peak = self.equity_curve[0]
        mdd = 0.0
        for v in self.equity_curve:
            if v > peak:
                peak = v
            dd = (peak - v) / peak
            if dd > mdd:
                mdd = dd
        return round(mdd * 100, 2)

    @property
    def sharpe_ratio(self) -> float:
        if len(self.equity_curve) < 2:
            return 0.0
        returns = pd.Series(self.equity_curve).pct_change().dropna()
        if returns.std() == 0:
            return 0.0
        return round((returns.mean() / returns.std()) * (252 ** 0.5), 2)

    def print_summary(self) -> None:
        print("\n" + "=" * 55)
        print(f"  DQT 백테스트 결과  {self.start} ~ {self.end}")
        print("=" * 55)
        print(f"  유니버스:       {self.universe_size}종목")
        print(f"  초기 자본:      {self.initial_capital:,.0f}원")
        final = self.equity_curve[-1] if self.equity_curve else self.initial_capital
        print(f"  최종 자본:      {final:,.0f}원")
        print(f"  총 수익률:      {self.total_return_pct:+.2f}%")
        print(f"  최대 낙폭(MDD): -{self.max_drawdown_pct:.2f}%")
        print(f"  샤프 지수:      {self.sharpe_ratio:.2f}")
        print("-" * 55)
        print(f"  총 거래:        {self.total_trades}건")
        print(f"  승률:           {self.win_rate*100:.1f}%")
        print(f"  평균 수익:      {self.avg_win:+.2f}%")
        print(f"  평균 손실:      {self.avg_loss:+.2f}%")
        print(f"  손익비(PF):     {self.profit_factor:.2f}")
        # 청산 사유별
        reasons = {}
        for t in self.trades:
            reasons[t.exit_reason] = reasons.get(t.exit_reason, 0) + 1
        print(f"  청산 사유:      " + " | ".join(f"{k} {v}건" for k, v in sorted(reasons.items())))
        # 최고/최악 거래
        if self.trades:
            best  = max(self.trades, key=lambda t: t.pnl_pct)
            worst = min(self.trades, key=lambda t: t.pnl_pct)
            print(f"  최고 거래:      {best.ticker} {best.pnl_pct:+.2f}% ({best.entry_date})")
            print(f"  최악 거래:      {worst.ticker} {worst.pnl_pct:+.2f}% ({worst.entry_date})")
        print("=" * 55 + "\n")


# ── 지표 계산 ────────────────────────────────────────────────

def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """일봉 OHLCV DataFrame에 기술지표 컬럼 추가."""
    close  = df["Close"].astype(float)
    high   = df["High"].astype(float)
    low    = df["Low"].astype(float)
    volume = df["Volume"].astype(float)

    # RSI
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(_RSI_P).mean()
    loss  = (-delta.clip(upper=0)).rolling(_RSI_P).mean()
    rs    = gain / loss.replace(0, float("nan"))
    df["rsi"] = 100 - (100 / (1 + rs))

    # MACD
    ema_fast   = close.ewm(span=_MACD_F, adjust=False).mean()
    ema_slow   = close.ewm(span=_MACD_S, adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    sig_line   = macd_line.ewm(span=_MACD_SIG, adjust=False).mean()
    df["macd_hist"]      = macd_line - sig_line
    df["macd_hist_prev"] = df["macd_hist"].shift(1)

    # 볼린저밴드
    ma20       = close.rolling(_BB_P).mean()
    std20      = close.rolling(_BB_P).std()
    bb_upper   = ma20 + _BB_STD * std20
    bb_lower   = ma20 - _BB_STD * std20
    bb_width   = (bb_upper - bb_lower) / ma20
    df["bb_width_ratio"] = bb_width / bb_width.rolling(_BB_P).mean()
    df["bb_position"]    = (close - bb_lower) / (bb_upper - bb_lower + 1e-9)

    # OBV 기울기 (5봉)
    direction  = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv        = (volume * direction).cumsum()
    df["obv_slope"] = obv.diff(5) / (volume.rolling(5).mean() + 1e-9)

    # 거래량 비율
    df["vol_ratio"] = volume / volume.rolling(_VOL_MA).mean()

    # 이동평균
    df["ma20"] = ma20
    df["ma60"] = close.rolling(60).mean()

    # StochRSI
    rsi14      = df["rsi"]
    rsi_min14  = rsi14.rolling(14).min()
    rsi_max14  = rsi14.rolling(14).max()
    df["stoch_rsi"] = (rsi14 - rsi_min14) / (rsi_max14 - rsi_min14 + 1e-9) * 100

    # 모멘텀 점수 (간이 — 실시간과 동일 공식)
    df["momentum_score"] = (
        df["vol_ratio"].clip(0, 5) / 5 * 40 +
        df["obv_slope"].clip(-1, 1).apply(lambda x: max(0, x)) * 30 +
        df["rsi"].apply(lambda r: max(0, 20 - abs(r - 55))) +
        df["bb_width_ratio"].clip(0, 2).apply(lambda b: min(10, (b - 1) * 10))
    ).clip(0, 130)

    return df


def _load_market_data(start: str, end: str) -> dict[str, pd.DataFrame]:
    """
    시황 Gate용 지수 데이터 로드.

    Returns:
        {
            "kospi":  KOSPI일봉 DataFrame (Close, Change%),
            "vix":    VIX 일봉,
            "sp500":  S&P500 일봉,
        }
    """
    import FinanceDataReader as fdr

    result: dict[str, pd.DataFrame] = {}
    try:
        df = fdr.DataReader("KS11", start, end)
        df["chg_pct"] = df["Close"].pct_change() * 100
        df["ret_5d"]  = df["Close"].pct_change(5) * 100
        df["ret_20d"] = df["Close"].pct_change(20) * 100
        result["kospi"] = df
    except Exception as e:
        logger.warning(f"KOSPI 로드 실패: {e}")

    try:
        df = fdr.DataReader("VIX", start, end)
        result["vix"] = df
    except Exception as e:
        logger.warning(f"VIX 로드 실패: {e}")

    try:
        df = fdr.DataReader("SP500", start, end)
        df["ret_3d"] = df["Close"].pct_change(3) * 100
        result["sp500"] = df
    except Exception as e:
        logger.warning(f"SP500 로드 실패: {e}")

    return result


def _check_market_gates(
    market_data: dict[str, pd.DataFrame],
    today,
) -> tuple[bool, str]:
    """
    시황 Gate 1~3.5 백테스트 근사.

    실제 시스템 Gate와의 대응:
      Gate 1  — VIX ≥ 30 → 위기 (risk_level 4 이상 근사)
      Gate 2  — S&P500 3일 수익률 < -3% → 글로벌 급락
      Gate 3  — KOSPI 20일 수익률 기반 market_score < -0.3
      Gate 3.5— KOSPI 당일 등락률 < -0.5%

    Returns:
        (blocked: bool, reason: str)
    """
    # Gate 1: VIX 기반 리스크 레벨 — 진짜 공포 구간만 차단
    vix_df = market_data.get("vix")
    if vix_df is not None and today in vix_df.index:
        vix = float(vix_df.loc[today, "Close"])
        if vix >= 30.0:
            return True, f"Gate 1: VIX {vix:.1f} ≥ 30 (위기)"

    # Gate 2: S&P500 3일 급락 — 글로벌 패닉만 차단
    sp500_df = market_data.get("sp500")
    if sp500_df is not None and today in sp500_df.index:
        ret_3d = float(sp500_df.loc[today, "ret_3d"])
        if ret_3d < -5.0:
            return True, f"Gate 2: S&P500 3일 {ret_3d:.1f}% 급락"

    # Gate 3: 극심한 약세장만 차단 (KOSPI 20일 -8% 이하)
    kospi_df = market_data.get("kospi")
    if kospi_df is not None and today in kospi_df.index:
        row = kospi_df.loc[today]
        ret_20d = float(row.get("ret_20d", 0) or 0)
        if ret_20d < -8.0:
            return True, f"Gate 3: KOSPI 20일 {ret_20d:.1f}% (극심한 약세장)"

        # Gate 3.5: 강한 급락일만 차단 — 살짝 빠지는 날은 개별종목 기회
        chg_pct = float(row.get("chg_pct", 0) or 0)
        if chg_pct < -1.5:
            return True, f"Gate 3.5: KOSPI 당일 {chg_pct:.2f}% 급락"

    return False, ""


def _gate_score(row: pd.Series, price_chg: float) -> tuple[bool, float, float]:
    """
    Gate 4.2 룰베이스 점수 계산 (Claude 대체).

    Returns:
        (blocked, score, size_mult)
    """
    vol     = float(row.get("vol_ratio", 0))
    rsi     = float(row.get("rsi", 50))
    obv     = float(row.get("obv_slope", 0))
    srsi    = float(row.get("stoch_rsi", 50))
    bb_r    = float(row.get("bb_width_ratio", 1))
    ms      = float(row.get("momentum_score", 0))

    # Hard fails
    if rsi > _RSI_MAX or rsi < _RSI_MIN:
        return True, 0.0, 0.0
    if vol < 1.5:
        return True, 0.0, 0.0
    if price_chg <= 0:
        return True, 0.0, 0.0
    if obv < 0 and rsi > 70:
        return True, 0.0, 0.0
    if srsi > 88:
        return True, 0.0, 0.0

    # 점수화
    score = 0.0
    # 거래량 (0~30)
    score += 30 if vol >= 5 else 22 if vol >= 3 else 16 if vol >= 2 else 10 if vol >= 1.5 else 5
    # RSI (0~20)
    score += 20 if rsi < 55 else 16 if rsi < 65 else 11 if rsi < 72 else 6 if rsi <= 82 else 2
    # OBV (0~20)
    score += 20 if obv > 0.5 else 15 if obv > 0.1 else 10 if obv > 0 else 5 if obv > -0.1 else 0
    # StochRSI (0~15)
    score += 15 if srsi < 30 else 12 if srsi < 50 else 9 if srsi < 65 else 6 if srsi < 75 else 3 if srsi < 85 else 0
    # 모멘텀+BB (0~15)
    score += min(8.0, ms / 130 * 8)
    score += min(7.0, (bb_r - 1.0) * 14)

    score = min(100.0, score)

    if score < _SCORE_ENTER:
        return True, score, 0.0
    size_mult = 1.0 if score >= _SCORE_FULL else 0.75
    return False, score, size_mult


# ── 메인 엔진 ────────────────────────────────────────────────

class Backtester:
    """
    DQT 전략 일봉 백테스터.

    Args:
        start:      시작일 "YYYY-MM-DD"
        end:        종료일 "YYYY-MM-DD"
        tickers:    종목 코드 리스트 (없으면 기본 유니버스 사용)
        capital:    초기 자본 (원)
        stop_pct:   손절선 (0.0175 = 1.75%)
        target_pct: 목표 수익 (0.055 = 5.5%)
        verbose:    진행상황 출력 여부
    """

    def __init__(
        self,
        start: str = "2024-01-01",
        end:   str = "2024-12-31",
        tickers: list[str] | None = None,
        capital: float = 1_000_000.0,
        stop_pct: float = _STOP_PCT,
        target_pct: float = _TARGET_PCT,
        use_market_gates: bool = True,
        verbose: bool = True,
    ) -> None:
        self.start             = start
        self.end               = end
        self.tickers           = tickers or _default_universe()
        self.capital           = capital
        self.stop_pct          = stop_pct
        self.target_pct        = target_pct
        self.use_market_gates  = use_market_gates
        self.verbose           = verbose

    def run(self) -> BacktestResult:
        import FinanceDataReader as fdr

        result = BacktestResult(
            initial_capital=self.capital,
            start=self.start,
            end=self.end,
            universe_size=len(self.tickers),
        )

        # 1. 시황 데이터 로드 (Gate 1~3.5용)
        market_data: dict[str, pd.DataFrame] = {}
        if self.use_market_gates:
            if self.verbose:
                print("시황 데이터 로드 중... (KOSPI / VIX / S&P500)")
            market_data = _load_market_data(self.start, self.end)
            loaded = [k for k, v in market_data.items() if v is not None and not v.empty]
            if self.verbose:
                print(f"시황 로드 완료: {', '.join(loaded)}")

        # 2. 종목 데이터 로드 (전체 유니버스)
        all_data: dict[str, pd.DataFrame] = {}
        if self.verbose:
            print(f"종목 데이터 로드 중... ({len(self.tickers)}종목)")
        _lock = threading.Lock()

        def _load(ticker: str) -> None:
            try:
                df = fdr.DataReader(
                    ticker,
                    self.start,
                    self.end,
                )
                if df is None or len(df) < 30:
                    return
                df = _compute_indicators(df.copy())
                with _lock:
                    all_data[ticker] = df
            except Exception:
                pass

        threads = [threading.Thread(target=_load, args=(t,)) for t in self.tickers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if self.verbose:
            print(f"로드 완료: {len(all_data)}종목 / 실패: {len(self.tickers)-len(all_data)}종목")

        # 2. 거래일 목록 추출
        if not all_data:
            return result
        sample_df = next(iter(all_data.values()))
        trading_days = list(sample_df.index)

        # 3. 시뮬레이션
        cash         = self.capital
        positions: dict[str, dict] = {}   # ticker → {entry_price, stop, target, highest, size_mult, entry_date, entry_capital}
        equity_curve = [cash]
        all_trades: list[Trade] = []

        for i, today in enumerate(trading_days[:-1]):  # 마지막 날은 진입만 하고 내일이 없음
            tomorrow = trading_days[i + 1] if i + 1 < len(trading_days) else None
            if tomorrow is None:
                break

            # ── 기존 포지션 청산 체크 (오늘 고/저로 히트 확인) ──────
            closed_tickers = []
            for ticker, pos in positions.items():
                df_t = all_data.get(ticker)
                if df_t is None or today not in df_t.index:
                    continue
                row_today = df_t.loc[today]
                day_high  = float(row_today.get("High", row_today.get("high", pos["entry_price"])))
                day_low   = float(row_today.get("Low",  row_today.get("low",  pos["entry_price"])))
                day_close = float(row_today.get("Close", row_today.get("close", pos["entry_price"])))

                entry = pos["entry_price"]
                stop  = pos["stop"]
                target = pos["target"]
                highest = pos["highest"]
                held_days = (today.date() if hasattr(today, "date") else today) - (
                    pos["entry_date"].date() if hasattr(pos["entry_date"], "date") else pos["entry_date"]
                )
                held_n = held_days.days if hasattr(held_days, "days") else 0

                # 최고가 갱신
                if day_high > highest:
                    pos["highest"] = day_high
                    highest = day_high
                    # 트레일링 스톱 갱신
                    if (highest - entry) / entry >= _TRAILING_TRIGGER:
                        new_floor = highest * (1 - _TRAILING_FLOOR)
                        if new_floor > pos["stop"]:
                            pos["stop"] = new_floor
                            stop = new_floor

                exit_price  = None
                exit_reason = None

                # 손절 히트
                if day_low <= stop:
                    exit_price  = stop
                    exit_reason = "stop" if (stop - entry) / entry < 0 else "trailing"
                # 목표 히트
                elif day_high >= target:
                    exit_price  = target
                    exit_reason = "target"
                # 타임컷
                elif held_n >= _MAX_HOLD_DAYS:
                    exit_price  = day_close
                    exit_reason = "timecut"

                if exit_price is not None:
                    pnl_gross = (exit_price - entry) / entry - _COMMISSION_RT
                    trade_capital = pos["entry_capital"]
                    pnl_amt = trade_capital * pnl_gross
                    cash += trade_capital + pnl_amt

                    trade = Trade(
                        ticker=ticker,
                        entry_date=pos["entry_date"].date() if hasattr(pos["entry_date"], "date") else pos["entry_date"],
                        exit_date=today.date() if hasattr(today, "date") else today,
                        entry_price=entry,
                        exit_price=exit_price,
                        size_mult=pos["size_mult"],
                        exit_reason=exit_reason,
                        score=pos["score"],
                        pnl_amt=round(pnl_amt, 0),
                    )
                    all_trades.append(trade)
                    closed_tickers.append(ticker)

            for t in closed_tickers:
                del positions[t]

            # ── 시황 Gate 1~3.5 체크 ─────────────────────────────
            if self.use_market_gates and market_data:
                gate_blocked, gate_reason = _check_market_gates(market_data, today)
                if gate_blocked:
                    equity_curve.append(cash + sum(
                        p["entry_capital"] for p in positions.values()
                    ))
                    continue  # 당일 신규 진입 전면 차단

            # ── 신규 진입 스캔 ────────────────────────────────────
            if len(positions) >= _MAX_POSITIONS:
                equity_curve.append(cash + sum(
                    p["entry_capital"] for p in positions.values()
                ))
                continue

            candidates = []
            for ticker, df_t in all_data.items():
                if ticker in positions:
                    continue
                if today not in df_t.index:
                    continue
                row = df_t.loc[today]

                price_today = float(row.get("Close", 0) or 0)
                if price_today <= 0:
                    continue

                # 전일 종가 대비 등락률 계산
                idx_today = df_t.index.get_loc(today)
                if idx_today == 0:
                    continue
                prev_close = float(df_t.iloc[idx_today - 1].get("Close", price_today))
                price_chg  = (price_today - prev_close) / prev_close * 100 if prev_close > 0 else 0.0

                blocked, score, size_mult = _gate_score(row, price_chg)
                if blocked:
                    continue

                candidates.append({
                    "ticker":     ticker,
                    "score":      score,
                    "size_mult":  size_mult,
                    "ms":         float(row.get("momentum_score", 0)),
                    "price_today": price_today,
                })

            # 모멘텀 점수 내림차순 정렬, 빈 슬롯만큼 선택
            candidates.sort(key=lambda x: x["ms"], reverse=True)
            slots = _MAX_POSITIONS - len(positions)
            for cand in candidates[:slots]:
                ticker = cand["ticker"]
                df_t   = all_data[ticker]
                if tomorrow not in df_t.index:
                    continue

                # 다음날 시가로 진입
                entry_price = float(df_t.loc[tomorrow].get("Open", df_t.loc[tomorrow].get("open", 0)) or 0)
                if entry_price <= 0:
                    continue

                # 종목당 투자 금액
                slot_capital = (cash / (_MAX_POSITIONS - len(positions))) * cand["size_mult"]
                if slot_capital > cash:
                    slot_capital = cash
                if slot_capital <= 0:
                    continue

                cash -= slot_capital

                positions[ticker] = {
                    "entry_price":   entry_price,
                    "stop":          entry_price * (1 - self.stop_pct),
                    "target":        entry_price * (1 + self.target_pct),
                    "highest":       entry_price,
                    "size_mult":     cand["size_mult"],
                    "score":         cand["score"],
                    "entry_date":    tomorrow,
                    "entry_capital": slot_capital,
                }

            # 당일 자산 = 현금 + 미실현 포지션 평가액
            unrealized = 0.0
            for ticker, pos in positions.items():
                df_t = all_data.get(ticker)
                if df_t is not None and today in df_t.index:
                    cur = float(df_t.loc[today].get("Close", pos["entry_price"]))
                    unrealized += pos["entry_capital"] * (1 + (cur - pos["entry_price"]) / pos["entry_price"])
                else:
                    unrealized += pos["entry_capital"]
            equity_curve.append(cash + unrealized)

        result.trades       = all_trades
        result.equity_curve = equity_curve
        return result


# ── 기본 유니버스 ────────────────────────────────────────────

def _default_universe() -> list[str]:
    """
    KOSPI 대형주 + KOSDAQ 핵심 종목 50개.
    실제 사용 시 UniverseManager 연동 권장.
    """
    return [
        # KOSPI 대형주
        "005930", "000660", "035420", "005380", "000270",
        "068270", "051910", "035720", "003550", "028260",
        "105560", "096770", "032830", "018260", "009150",
        "015760", "034730", "017670", "066570", "010130",
        "207940", "006400", "000810", "011200", "012330",
        "033780", "010950", "086790", "316140", "009540",
        # KOSDAQ 핵심
        "247540", "196170", "091990", "086520", "263750",
        "145020", "357780", "112040", "041510", "036930",
        "123320", "293490", "003230", "214150", "039030",
        "065350", "950130", "241560", "140410", "093320",
    ]
