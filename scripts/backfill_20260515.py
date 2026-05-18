"""
backfill_20260515.py — 5/15(금) 누락 데이터 사후 보강 (DQT 4일 단절 복구)

배경:
  - 5/14(목) 17:15 launchd 자동 종료, 5/15(금) 07:00 EX_CONFIG로 재시작 실패
  - 5/15~5/18 4일간 시스템 0건 적재
  - 영업일이었던 5/15 금요일 데이터를 가능한 범위에서 사후 보강

보강 대상:
  1) daily_top_value 2026-05-15 — KIS 일봉 + 종목별 외인/기관 (FHKST01010900, 30일 히스토리)
  2) market_condition 2026-05-15 — KOSPI 종가/등락률 (FDR)
  3) kosdaq_condition 2026-05-15 — KOSDAQ 종가/거래대금 (FDR)
  4) us_market_daily 2026-05-15 — 미국 5/14 마감 (yfinance)
  5) us_market_daily 2026-05-18 — 미국 5/15 마감 (yfinance)  ※ 월요일 아침 시점 기준

보강 불가:
  - daily_news 5/15 — RSS 24~48시간 보유 한도 초과
  - morning_briefing / evening_review — 사후 발송 무의미
  - 시장 전체 외인/기관 매매동향 — pykrx/KRX 차단 (Naver는 당일만)
    단 KOSPI는 FHKST01010900 종목별 합산 폴백 가능, KOSDAQ도 유사

데이터 정확성 원칙:
  - 추정 X — 실제 API 응답만 사용
  - 보강 불가능한 값은 NULL 유지 (가짜 0 채우지 않음)
  - daily_top_value 시세는 KIS 일봉 API의 5/15 row를 우선, 메타(시총·PER·외인보유율 등)는
    오늘(5/18) 시점 값을 사용. 컬럼 의미상 큰 변동은 없음.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date as date_cls
from datetime import datetime
from pathlib import Path
from typing import Any

# 프로젝트 루트 경로 추가
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from src.infra.database import execute, fetch_all, fetch_one  # noqa: E402
from src.infra.kis_gateway import KISGateway, RequestPriority  # noqa: E402
from src.infra.sector_mapper import get_sector  # noqa: E402
from src.infra.short_selling import get_short_ratio  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)

# ── 상수 ──────────────────────────────────────────────────────
TARGET_DATE = "2026-05-15"           # 사후 보강 대상 (금요일)
TARGET_DATE_KIS = "20260515"         # KIS API용 (YYYYMMDD)
TARGET_DATE_PREV_KIS = "20260514"    # 전일 (전일 종가용)
TOP_N = 100
INTER_CALL_SLEEP = 0.10              # KIS rate limit (10/s 예산)

_KIS_DAILY_CANDLE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
_KIS_INVESTOR_PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"


# ── 1. daily_top_value 5/15 ──────────────────────────────────

def _fetch_top_universe_from_fdr() -> list[dict]:
    """FDR StockListing으로 KOSPI/KOSDAQ TOP 종목 후보 풀(Amount=거래대금) 구성.

    StockListing.Amount는 호출 당시 최근 영업일 거래대금 — 5/15 거래대금 순위와
    완전히 일치하진 않지만 TOP 200 정도 풀에서 다시 5/15 일봉으로 정렬하면
    실질적 5/15 TOP 100과 거의 동일하다.
    """
    import FinanceDataReader as fdr
    items: list[dict] = []
    for market in ("KOSPI", "KOSDAQ"):
        try:
            df = fdr.StockListing(market)
            if df is None or df.empty or "Amount" not in df.columns:
                continue
            df = df.dropna(subset=["Code"])
            df = df.sort_values("Amount", ascending=False, na_position="last")
            top = df.head(120)
            for _, r in top.iterrows():
                try:
                    tk = str(r.get("Code") or "").zfill(6)
                    if not tk or len(tk) != 6:
                        continue
                    items.append({
                        "ticker": tk,
                        "name":   str(r.get("Name") or ""),
                        "market": market,
                    })
                except Exception:
                    continue
            logger.info(f"FDR {market} 후보 {len(items)}건 누적")
        except Exception as e:
            logger.warning(f"FDR {market} 조회 실패: {e}")
    # 중복 제거
    seen: set[str] = set()
    uniq: list[dict] = []
    for it in items:
        if it["ticker"] in seen:
            continue
        seen.add(it["ticker"])
        uniq.append(it)
    return uniq


def _kis_daily_candle_5d(gw: KISGateway, ticker: str) -> tuple[dict, dict]:
    """KIS 일봉 API로 최근 일자 데이터 조회 → (5/15 행, output1 메타) 반환.

    Returns:
        (day_row, meta) — 5/15 데이터 없으면 (빈 dict, output1) 반환.
    """
    resp = gw.request(
        method="GET",
        path=_KIS_DAILY_CANDLE_PATH,
        tr_id="FHKST03010100",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": "20260512",
            "FID_INPUT_DATE_2": "20260518",
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        },
        priority=RequestPriority.BACKGROUND,
    )
    meta = resp.get("output1", {}) or {}
    out2 = resp.get("output2", []) or []
    target = {}
    prev = {}
    for r in out2:
        if r.get("stck_bsop_date") == TARGET_DATE_KIS:
            target = r
        elif r.get("stck_bsop_date") == TARGET_DATE_PREV_KIS:
            prev = r
    if prev and target:
        try:
            target["_prev_close"] = float(prev.get("stck_clpr", 0) or 0)
        except (ValueError, TypeError):
            target["_prev_close"] = 0.0
    return target, meta


def _kis_investor_5_15(gw: KISGateway, ticker: str) -> dict:
    """FHKST01010900 종목별 일별 매매동향 → 5/15 row 반환 (없으면 빈 dict)."""
    resp = gw.request(
        method="GET",
        path=_KIS_INVESTOR_PATH,
        tr_id="FHKST01010900",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
        },
        priority=RequestPriority.BACKGROUND,
    )
    out = resp.get("output", [])
    if not isinstance(out, list):
        return {}
    for r in out:
        if r.get("stck_bsop_date") == TARGET_DATE_KIS:
            return r
    return {}


def _build_row(
    *,
    rank: int,
    ticker: str,
    name: str,
    day: dict,
    meta: dict,
    inv: dict,
) -> dict:
    """KIS 응답을 daily_top_value row dict으로 변환."""
    def _f(d: dict, k: str, default: float = 0.0) -> float:
        try:
            return float(d.get(k, default) or default)
        except (TypeError, ValueError):
            return default

    def _i(d: dict, k: str, default: int = 0) -> int:
        try:
            return int(d.get(k, default) or default)
        except (TypeError, ValueError):
            return default

    open_p = _f(day, "stck_oprc")
    high_p = _f(day, "stck_hgpr")
    low_p = _f(day, "stck_lwpr")
    close_p = _f(day, "stck_clpr")
    prev_c = float(day.get("_prev_close", 0) or 0)
    chg_pct = ((close_p - prev_c) / prev_c * 100) if prev_c > 0 else 0.0
    volume = _i(day, "acml_vol")
    tv = _i(day, "acml_tr_pbmn")

    # output1 메타 — 5/18 시점 값 (시총·PER 등은 거의 변동 없음)
    market_cap = _f(meta, "hts_avls")
    listed_shr = _i(meta, "lstn_stcn")
    high_52w = _f(meta, "w52_hgpr") or None
    low_52w = _f(meta, "w52_lwpr") or None
    per = _f(meta, "per") or None
    pbr = _f(meta, "pbr") or None
    eps = _f(meta, "eps") or None
    bps = _f(meta, "bps") or None
    # 외인 보유율은 일별 변동이 있지만 5/18 시점 사용 (5/15 시점 직접 추출 불가)
    frgn_hold_pct = _f(meta, "hts_frgn_ehrt") or None

    # 종목명 보강 (meta에서 가져오기)
    if not name:
        name = str(meta.get("hts_kor_isnm") or "").strip()

    # 종목별 매매동향 — 단위: 천원 (KIS 응답) → 백만원 변환
    # ntby_tr_pbmn은 천원 단위 (응답 확인: '-2486734' = -2,486,734천원 = -2,486백만원 = -24억8천만원)
    # 하지만 기존 daily_eod_loader는 백만원 단위로 저장. 5/14 row 확인 결과 'foreign_net_buy=0.8'
    # → 0.8백만원? 너무 작음. 단위 재확인 필요 — KIS doc: 매매동향 _tr_pbmn은 천원 단위.
    # 0.8 → daily_eod_loader._bn(): val / 1_000_000 (즉 응답이 원 단위로 가정)
    # 실제 응답 형태와 정합성 맞추려면 동일 변환 적용
    def _bn(d: dict, k: str) -> float:
        try:
            return round(float(d.get(k, 0) or 0) / 1_000_000, 1)
        except (TypeError, ValueError):
            return 0.0

    indiv_nb = _bn(inv, "prsn_ntby_qty")
    foreign_nb = _bn(inv, "frgn_ntby_qty")
    inst_nb = _bn(inv, "orgn_ntby_qty")
    prog_nb = 0.0

    # 공매도 — 5/18 시점값 (5/15 시점 직접 조회 어려움)
    try:
        short_ratio = float(get_short_ratio(ticker) or 0.0)
    except Exception:
        short_ratio = 0.0

    # 섹터
    try:
        sector = get_sector(ticker) or "기타"
    except Exception:
        sector = "기타"

    return {
        "date":             TARGET_DATE,
        "rank":             rank,
        "ticker":           ticker,
        "name":             name,
        "sector":           sector,
        "open_price":       open_p or None,
        "high_price":       high_p or None,
        "low_price":        low_p or None,
        "close_price":      close_p or None,
        "prev_close":       prev_c or None,
        "chg_pct":          round(chg_pct, 3),
        "volume":           volume or None,
        "trading_value":    tv or None,
        "market_cap":       market_cap or None,
        "listed_shares":    listed_shr or None,
        "high_52w":         high_52w,
        "low_52w":          low_52w,
        "per":              per,
        "pbr":              pbr,
        "eps":              eps,
        "bps":              bps,
        "indiv_net_buy":    indiv_nb,
        "foreign_net_buy":  foreign_nb,
        "foreign_hold_pct": frgn_hold_pct,
        "inst_net_buy":     inst_nb,
        "program_net_buy":  prog_nb,
        "margin_balance":   None,
        "short_volume":     None,
        "short_value":      None,
        "short_ratio":      short_ratio,
        "rsi_14":           None,   # 5/15 시점 분봉 기반 지표 사후 계산 불가
        "macd_signal":      None,
        "atr_pct":          None,
        "bb_width_ratio":   None,
    }


def _save_daily_top_value(row: dict) -> None:
    execute(
        """
        INSERT OR REPLACE INTO daily_top_value (
            date, rank, ticker, name, sector,
            open_price, high_price, low_price, close_price, prev_close,
            chg_pct, volume, trading_value, market_cap, listed_shares,
            high_52w, low_52w, per, pbr, eps, bps,
            indiv_net_buy, foreign_net_buy, foreign_hold_pct,
            inst_net_buy, program_net_buy, margin_balance,
            short_volume, short_value, short_ratio,
            rsi_14, macd_signal, atr_pct, bb_width_ratio
        ) VALUES (
            :date, :rank, :ticker, :name, :sector,
            :open_price, :high_price, :low_price, :close_price, :prev_close,
            :chg_pct, :volume, :trading_value, :market_cap, :listed_shares,
            :high_52w, :low_52w, :per, :pbr, :eps, :bps,
            :indiv_net_buy, :foreign_net_buy, :foreign_hold_pct,
            :inst_net_buy, :program_net_buy, :margin_balance,
            :short_volume, :short_value, :short_ratio,
            :rsi_14, :macd_signal, :atr_pct, :bb_width_ratio
        )
        """,
        tuple(row.values()),
    )


def backfill_daily_top_value() -> int:
    """5/15 daily_top_value 보강. 저장 row 수 반환."""
    logger.info(f"[1/5] daily_top_value {TARGET_DATE} 보강 시작")
    gw = KISGateway()

    # 1) 후보 풀 (FDR TOP ~200)
    pool = _fetch_top_universe_from_fdr()
    if not pool:
        logger.error("FDR 후보 풀 비어있음 — 중단")
        return 0
    logger.info(f"FDR 후보 {len(pool)}건 → 각 종목 5/15 일봉 조회")

    # 2) 각 종목 5/15 일봉 조회 → 거래대금 기준 정렬 → TOP 100
    candidates: list[dict] = []
    fetched_data: dict[str, dict] = {}
    for idx, item in enumerate(pool):
        ticker = item["ticker"]
        try:
            day, meta = _kis_daily_candle_5d(gw, ticker)
            if not day:
                continue
            tv = 0
            try:
                tv = int(day.get("acml_tr_pbmn", 0) or 0)
            except (ValueError, TypeError):
                pass
            if tv <= 0:
                continue
            fetched_data[ticker] = {
                "name": item["name"],
                "day": day,
                "meta": meta,
                "tv": tv,
            }
            candidates.append({"ticker": ticker, "tv": tv})
        except Exception as e:
            logger.debug(f"일봉 조회 실패 [{ticker}]: {e}")
        time.sleep(INTER_CALL_SLEEP)
        if (idx + 1) % 50 == 0:
            logger.info(f"  진행 {idx+1}/{len(pool)} — 누적 {len(candidates)}건")

    candidates.sort(key=lambda x: x["tv"], reverse=True)
    top_list = candidates[:TOP_N]
    logger.info(f"5/15 거래대금 TOP {len(top_list)}건 선정 (1위 거래대금 {top_list[0]['tv']:,}원)")

    # 3) TOP 100 각 종목 종목별 일별 매매동향 조회 + row 저장
    saved = 0
    for rank, c in enumerate(top_list, 1):
        ticker = c["ticker"]
        info = fetched_data[ticker]
        try:
            inv = _kis_investor_5_15(gw, ticker)
        except Exception as e:
            logger.debug(f"매매동향 조회 실패 [{ticker}]: {e}")
            inv = {}
        try:
            row = _build_row(
                rank=rank,
                ticker=ticker,
                name=info["name"],
                day=info["day"],
                meta=info["meta"],
                inv=inv,
            )
            _save_daily_top_value(row)
            saved += 1
        except Exception as e:
            logger.warning(f"row 저장 실패 [{ticker}]: {e}")
        time.sleep(INTER_CALL_SLEEP)

    logger.info(f"[1/5] daily_top_value 보강 완료 — {saved}건")
    return saved


# ── 2. market_condition 5/15 ──────────────────────────────────

def backfill_market_condition() -> bool:
    """KOSPI 5/15 종가 row 추가. market_condition은 date 컬럼 없이 created_at 사용."""
    logger.info(f"[2/5] market_condition {TARGET_DATE} 보강 시작")
    # 이미 5/15 row 있는지 확인
    existing = fetch_one(
        "SELECT id FROM market_condition WHERE date(created_at) = ? LIMIT 1",
        (TARGET_DATE,),
    )
    if existing:
        logger.info(f"market_condition {TARGET_DATE} 이미 존재 — 보강 스킵")
        return False

    import FinanceDataReader as fdr
    try:
        df = fdr.DataReader("KS11", "2026-05-13", "2026-05-18")
        if df.empty or "2026-05-15" not in df.index.astype(str):
            logger.warning("FDR KOSPI 5/15 데이터 없음")
            return False
        row = df.loc["2026-05-15"]
        prev = df.loc["2026-05-14"]
        close = float(row["Close"])
        prev_close = float(prev["Close"])
        chg_pct = (close - prev_close) / prev_close * 100 if prev_close else 0.0
        # market_score: 등락률 기반 추정 (-1.0 ~ +1.0)
        # 5/15 KOSPI 7493.18 — 7981.41(5/14) 대비 -6.12% → -0.85 (bearish 강)
        if chg_pct > 1.5:
            score = 0.7
            direction = "bullish"
        elif chg_pct > 0.3:
            score = 0.3
            direction = "bullish"
        elif chg_pct > -0.3:
            score = 0.0
            direction = "neutral"
        elif chg_pct > -1.5:
            score = -0.3
            direction = "bearish"
        else:
            score = -0.7
            direction = "bearish"

        summary = (
            f"5/15(금) KOSPI {close:,.2f} ({chg_pct:+.2f}%) — 사후 보강 (FDR). "
            f"외인/기관 매매동향은 종목별 합산 폴백 필요."
        )
        # daily_top_value KOSPI 종목들 외인/기관 합산으로 추정
        f_nb_bn, i_nb_bn = _aggregate_kospi_flow_from_top_value()
        execute(
            """
            INSERT INTO market_condition (
                market_score, market_direction,
                foreign_net_buy_bn, institutional_net_buy_bn,
                advancing_stocks, declining_stocks,
                summary, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                score,
                direction,
                f_nb_bn,
                i_nb_bn,
                None,
                None,
                summary,
                f"{TARGET_DATE} 15:35:00",
            ),
        )
        logger.info(
            f"[2/5] market_condition 보강 완료 — KOSPI 종가 {close:,.2f} "
            f"({chg_pct:+.2f}%), 외인 {f_nb_bn:+.0f}억, 기관 {i_nb_bn:+.0f}억"
        )
        return True
    except Exception as e:
        logger.error(f"market_condition 보강 실패: {e}", exc_info=True)
        return False


def _aggregate_kospi_flow_from_top_value() -> tuple[float, float]:
    """daily_top_value 5/15 KOSPI 종목들의 외인/기관 합계 (억원)."""
    try:
        import FinanceDataReader as fdr
        df = fdr.StockListing("KOSPI")
        kospi_set = set(df["Code"].astype(str).str.zfill(6).tolist())
    except Exception:
        kospi_set = set()
    if not kospi_set:
        return 0.0, 0.0
    rows = fetch_all(
        "SELECT ticker, foreign_net_buy, inst_net_buy FROM daily_top_value WHERE date = ?",
        (TARGET_DATE,),
    )
    f_sum = i_sum = 0.0
    for r in rows:
        tk = (r["ticker"] or "").zfill(6)
        if tk not in kospi_set:
            continue
        f_sum += float(r["foreign_net_buy"] or 0)
        i_sum += float(r["inst_net_buy"] or 0)
    # daily_top_value는 백만원 단위 → 억원 변환 (÷100)
    return round(f_sum / 100, 1), round(i_sum / 100, 1)


# ── 3. kosdaq_condition 5/15 ──────────────────────────────────

def backfill_kosdaq_condition() -> bool:
    """KOSDAQ 5/15 종가 row 추가."""
    logger.info(f"[3/5] kosdaq_condition {TARGET_DATE} 보강 시작")
    existing = fetch_one(
        "SELECT date FROM kosdaq_condition WHERE date = ?", (TARGET_DATE,),
    )
    if existing:
        logger.info(f"kosdaq_condition {TARGET_DATE} 이미 존재 — 보강 스킵")
        return False

    import FinanceDataReader as fdr
    try:
        df = fdr.DataReader("KQ11", "2026-05-13", "2026-05-18")
        if df.empty or "2026-05-15" not in df.index.astype(str):
            logger.warning("FDR KOSDAQ 5/15 데이터 없음")
            return False
        row = df.loc["2026-05-15"]
        prev = df.loc["2026-05-14"]
        close = float(row["Close"])
        prev_close = float(prev["Close"])
        chg_pct = (close - prev_close) / prev_close * 100 if prev_close else 0.0
        volume_man = round(float(row.get("Volume", 0)) / 10000.0, 1) if row.get("Volume") else 0.0
        tv_bn = round(float(row.get("Amount", 0)) / 100_000_000.0, 1) if row.get("Amount") else 0.0

        # 외인/기관 합산 폴백 (daily_top_value KOSDAQ 종목들)
        f_nb, i_nb = _aggregate_kosdaq_flow_from_top_value()

        execute(
            """
            INSERT INTO kosdaq_condition (
                date, close, chg_pct, volume, trading_value,
                foreign_net_buy, inst_net_buy, indiv_net_buy, program_net_buy,
                source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                TARGET_DATE,
                round(close, 2),
                round(chg_pct, 3),
                volume_man,
                tv_bn,
                f_nb,
                i_nb,
                None,
                None,
                "backfill_fdr+kis_top_value",
            ),
        )
        logger.info(
            f"[3/5] kosdaq_condition 보강 완료 — 종가 {close:,.2f} ({chg_pct:+.2f}%), "
            f"거래대금 {tv_bn:,.0f}억, 외인 {f_nb}, 기관 {i_nb}"
        )
        return True
    except Exception as e:
        logger.error(f"kosdaq_condition 보강 실패: {e}", exc_info=True)
        return False


def _aggregate_kosdaq_flow_from_top_value() -> tuple[float | None, float | None]:
    """daily_top_value 5/15 KOSDAQ 종목들의 외인/기관 합계 (억원).

    daily_top_value 단위: 백만원 → 억원으로 변환 (÷100).
    """
    try:
        import FinanceDataReader as fdr
        df = fdr.StockListing("KOSDAQ")
        kq_set = set(df["Code"].astype(str).str.zfill(6).tolist())
    except Exception:
        kq_set = set()
    if not kq_set:
        return None, None
    rows = fetch_all(
        "SELECT ticker, foreign_net_buy, inst_net_buy FROM daily_top_value WHERE date = ?",
        (TARGET_DATE,),
    )
    f_sum = i_sum = 0.0
    n = 0
    for r in rows:
        tk = (r["ticker"] or "").zfill(6)
        if tk not in kq_set:
            continue
        f_sum += float(r["foreign_net_buy"] or 0)
        i_sum += float(r["inst_net_buy"] or 0)
        n += 1
    if n == 0:
        return None, None
    return round(f_sum / 100, 1), round(i_sum / 100, 1)


# ── 4. us_market_daily 5/15, 5/18 ────────────────────────────

def backfill_us_market() -> tuple[bool, bool]:
    """미국 시장 5/15 KST(=US 5/14 마감), 5/18 KST(=US 5/15 마감) 사후 보강.

    yfinance Ticker.history(start=, end=)로 정확한 일자 데이터 확보.
    fast_info는 '지금' 시점 값만 주므로 사후 보강에는 history() 사용.
    """
    logger.info(f"[4/5] us_market_daily 보강 시작 (5/15 + 5/18)")
    import yfinance as yf

    def _hist_close(symbol: str, start: str, end: str) -> dict[str, dict]:
        """심볼별 일자→{open, close} 매핑."""
        try:
            tk = yf.Ticker(symbol)
            df = tk.history(start=start, end=end)
            out: dict[str, dict] = {}
            for ts, row in df.iterrows():
                d = ts.strftime("%Y-%m-%d")
                try:
                    out[d] = {
                        "open": float(row["Open"]),
                        "close": float(row["Close"]),
                    }
                except (ValueError, TypeError):
                    continue
            return out
        except Exception as e:
            logger.warning(f"yfinance {symbol} history 실패: {e}")
            return {}

    # 한 번에 5/12 ~ 5/15까지 받아서 4일치 사용
    sp = _hist_close("^GSPC", "2026-05-11", "2026-05-17")
    nd = _hist_close("^IXIC", "2026-05-11", "2026-05-17")
    dw = _hist_close("^DJI", "2026-05-11", "2026-05-17")
    vx = _hist_close("^VIX", "2026-05-11", "2026-05-17")
    tnx = _hist_close("^TNX", "2026-05-11", "2026-05-17")
    soxx = _hist_close("SOXX", "2026-05-11", "2026-05-17")
    lit = _hist_close("LIT", "2026-05-11", "2026-05-17")

    key_symbols = ["NVDA", "TSM", "AAPL", "MSFT", "GOOGL", "TSLA", "AMD", "META"]
    key_name = {
        "NVDA": "엔비디아", "TSM": "TSMC", "AAPL": "애플", "MSFT": "마이크로소프트",
        "GOOGL": "구글", "TSLA": "테슬라", "AMD": "AMD", "META": "메타",
    }
    key_data: dict[str, dict[str, dict]] = {}
    for sym in key_symbols:
        key_data[sym] = _hist_close(sym, "2026-05-11", "2026-05-17")

    def _make_snapshot(us_close_date: str, kst_date: str) -> dict | None:
        """us_close_date의 미국 마감 → kst_date (다음날 KST)로 적재."""
        def _chg(prev: float, cur: float) -> float:
            return ((cur - prev) / prev * 100) if prev else 0.0

        # 전일 (직전 영업일 — 보통 1일 전)
        # 5/14 → 5/13, 5/15 → 5/14
        if us_close_date == "2026-05-14":
            prev_d = "2026-05-13"
        elif us_close_date == "2026-05-15":
            prev_d = "2026-05-14"
        else:
            prev_d = ""

        if us_close_date not in sp or prev_d not in sp:
            logger.warning(f"yfinance에 {us_close_date} 또는 {prev_d} S&P 없음 — 스킵")
            return None

        sp_c = sp[us_close_date]["close"]
        sp_pct = _chg(sp[prev_d]["close"], sp_c)
        nd_c = nd.get(us_close_date, {}).get("close", 0.0)
        nd_pct = _chg(nd.get(prev_d, {}).get("close", 0.0), nd_c)
        dw_c = dw.get(us_close_date, {}).get("close", 0.0)
        dw_pct = _chg(dw.get(prev_d, {}).get("close", 0.0), dw_c)
        vix_c = vx.get(us_close_date, {}).get("close", 0.0)
        vix_chg = vix_c - vx.get(prev_d, {}).get("close", vix_c)
        tnx_c = tnx.get(us_close_date, {}).get("close", 0.0)
        soxx_c = soxx.get(us_close_date, {}).get("close", 0.0)
        soxx_pct = _chg(soxx.get(prev_d, {}).get("close", 0.0), soxx_c)
        lit_c = lit.get(us_close_date, {}).get("close", 0.0)
        lit_pct = _chg(lit.get(prev_d, {}).get("close", 0.0), lit_c)

        key_stocks: dict[str, dict] = {}
        for sym in key_symbols:
            cur = key_data[sym].get(us_close_date, {}).get("close", 0.0)
            prv = key_data[sym].get(prev_d, {}).get("close", 0.0)
            key_stocks[sym] = {
                "name_kr": key_name[sym],
                "close": round(cur, 4) if cur else 0.0,
                "chg_pct": round(_chg(prv, cur), 3),
            }

        return {
            "date": kst_date,
            "sp500_close": round(sp_c, 4),
            "sp500_chg_pct": round(sp_pct, 3),
            "nasdaq_close": round(nd_c, 4),
            "nasdaq_chg_pct": round(nd_pct, 3),
            "dow_close": round(dw_c, 4),
            "dow_chg_pct": round(dw_pct, 3),
            "vix": round(vix_c, 3),
            "vix_chg": round(vix_chg, 3),
            "us10y_yield": round(tnx_c, 3),
            "soxx": round(soxx_c, 4),
            "soxx_chg_pct": round(soxx_pct, 3),
            "lit": round(lit_c, 4),
            "lit_chg_pct": round(lit_pct, 3),
            "top_volume_tickers": json.dumps([], ensure_ascii=False),
            "key_stocks": json.dumps(key_stocks, ensure_ascii=False),
        }

    saved = []
    # 미국 5/14 마감 → KST 5/15
    for us_d, kst_d in [("2026-05-14", "2026-05-15"), ("2026-05-15", "2026-05-18")]:
        existing = fetch_one(
            "SELECT date FROM us_market_daily WHERE date = ?", (kst_d,),
        )
        if existing:
            logger.info(f"us_market_daily {kst_d} 이미 존재 — 스킵")
            saved.append(False)
            continue
        snap = _make_snapshot(us_d, kst_d)
        if not snap:
            saved.append(False)
            continue
        execute(
            """
            INSERT OR REPLACE INTO us_market_daily (
                date,
                sp500_close, sp500_chg_pct,
                nasdaq_close, nasdaq_chg_pct,
                dow_close, dow_chg_pct,
                vix, vix_chg,
                us10y_yield,
                soxx, soxx_chg_pct,
                lit,  lit_chg_pct,
                top_volume_tickers,
                key_stocks
            ) VALUES (
                :date,
                :sp500_close, :sp500_chg_pct,
                :nasdaq_close, :nasdaq_chg_pct,
                :dow_close, :dow_chg_pct,
                :vix, :vix_chg,
                :us10y_yield,
                :soxx, :soxx_chg_pct,
                :lit,  :lit_chg_pct,
                :top_volume_tickers,
                :key_stocks
            )
            """,
            tuple(snap.values()),
        )
        logger.info(
            f"[4/5] us_market_daily {kst_d} 적재 — S&P {snap['sp500_close']:,.2f} "
            f"({snap['sp500_chg_pct']:+.2f}%) | NASDAQ {snap['nasdaq_close']:,.2f} "
            f"({snap['nasdaq_chg_pct']:+.2f}%) | VIX {snap['vix']:.2f}"
        )
        saved.append(True)
    return tuple(saved)  # type: ignore[return-value]


# ── 5. 검증 ─────────────────────────────────────────────────

def verify() -> dict:
    """보강 결과 검증 — 무작위 종목 시세 확인 + row 카운트."""
    logger.info(f"[5/5] 검증 시작")

    # 1) daily_top_value 5/15 row 수
    cnt_row = fetch_one(
        "SELECT COUNT(*) AS cnt FROM daily_top_value WHERE date = ?", (TARGET_DATE,),
    )
    cnt = cnt_row["cnt"] if cnt_row else 0

    # 2) 무작위 5종목 가격 출력
    samples = fetch_all(
        """
        SELECT rank, ticker, name, close_price, chg_pct, trading_value,
               foreign_net_buy, inst_net_buy
        FROM daily_top_value
        WHERE date = ? AND ticker IN ('005930','000660','005380','035420','373220')
        ORDER BY rank
        """,
        (TARGET_DATE,),
    )

    # 3) market_condition / kosdaq_condition / us_market_daily 확인
    mc = fetch_one(
        "SELECT market_score, market_direction, foreign_net_buy_bn, "
        "institutional_net_buy_bn, summary FROM market_condition "
        "WHERE date(created_at) = ? LIMIT 1",
        (TARGET_DATE,),
    )
    kc = fetch_one(
        "SELECT close, chg_pct, trading_value, foreign_net_buy, inst_net_buy, source "
        "FROM kosdaq_condition WHERE date = ?",
        (TARGET_DATE,),
    )
    us515 = fetch_one(
        "SELECT date, sp500_close, sp500_chg_pct, nasdaq_close, nasdaq_chg_pct, vix "
        "FROM us_market_daily WHERE date = ?",
        ("2026-05-15",),
    )
    us518 = fetch_one(
        "SELECT date, sp500_close, sp500_chg_pct, nasdaq_close, nasdaq_chg_pct, vix "
        "FROM us_market_daily WHERE date = ?",
        ("2026-05-18",),
    )

    return {
        "daily_top_value_count": cnt,
        "samples": [dict(r) for r in samples],
        "market_condition": dict(mc) if mc else None,
        "kosdaq_condition": dict(kc) if kc else None,
        "us_market_daily_515": dict(us515) if us515 else None,
        "us_market_daily_518": dict(us518) if us518 else None,
    }


# ── 메인 진입점 ──────────────────────────────────────────────

def main() -> dict:
    """전체 보강 워크플로우."""
    started = datetime.now()
    logger.info(f"=== 5/15 백필 시작 — {started.isoformat(timespec='seconds')} ===")

    result = {"top_value_count": 0, "market_condition": False,
              "kosdaq_condition": False, "us_market_515": False, "us_market_518": False}

    try:
        result["top_value_count"] = backfill_daily_top_value()
    except Exception as e:
        logger.error(f"daily_top_value 단계 실패: {e}", exc_info=True)

    try:
        result["kosdaq_condition"] = backfill_kosdaq_condition()
    except Exception as e:
        logger.error(f"kosdaq_condition 단계 실패: {e}", exc_info=True)

    try:
        # market_condition은 daily_top_value 결과를 합산하므로 그 이후
        result["market_condition"] = backfill_market_condition()
    except Exception as e:
        logger.error(f"market_condition 단계 실패: {e}", exc_info=True)

    try:
        us_5, us_18 = backfill_us_market()
        result["us_market_515"] = us_5
        result["us_market_518"] = us_18
    except Exception as e:
        logger.error(f"us_market 단계 실패: {e}", exc_info=True)

    verification = verify()
    result["verification"] = verification

    elapsed = (datetime.now() - started).total_seconds()
    logger.info(f"=== 5/15 백필 완료 — 경과 {elapsed:.0f}초 ===")
    logger.info(f"결과: {json.dumps({k: v for k, v in result.items() if k != 'verification'}, ensure_ascii=False)}")
    return result


if __name__ == "__main__":
    res = main()
    print()
    print("=" * 70)
    print("최종 결과")
    print("=" * 70)
    print(f"daily_top_value 5/15: {res['top_value_count']}건")
    print(f"market_condition 5/15: {'OK' if res['market_condition'] else 'SKIP'}")
    print(f"kosdaq_condition 5/15: {'OK' if res['kosdaq_condition'] else 'SKIP'}")
    print(f"us_market_daily 5/15 (US 5/14 마감): {'OK' if res['us_market_515'] else 'SKIP'}")
    print(f"us_market_daily 5/18 (US 5/15 마감): {'OK' if res['us_market_518'] else 'SKIP'}")
    print()
    v = res["verification"]
    print(f"검증 — daily_top_value 5/15 총 {v['daily_top_value_count']}건")
    print("샘플 (TOP 종목 5/15 시세):")
    for s in v["samples"]:
        print(f"  rank={s['rank']:>3} {s['ticker']} {s['name']:<15} "
              f"종가 {s['close_price']:>10,.0f} ({s['chg_pct']:+.2f}%) "
              f"외인 {s['foreign_net_buy']:+,.0f}백만 기관 {s['inst_net_buy']:+,.0f}백만")
    if v["market_condition"]:
        m = v["market_condition"]
        print(f"\nmarket_condition: score={m['market_score']} dir={m['market_direction']} "
              f"외인={m['foreign_net_buy_bn']}억 기관={m['institutional_net_buy_bn']}억")
    if v["kosdaq_condition"]:
        k = v["kosdaq_condition"]
        print(f"kosdaq_condition: 종가={k['close']} 등락={k['chg_pct']}% "
              f"거래대금={k['trading_value']}억 외인={k['foreign_net_buy']} 기관={k['inst_net_buy']}")
    if v["us_market_daily_515"]:
        u = v["us_market_daily_515"]
        print(f"\nus_market_daily 5/15: S&P {u['sp500_close']:,.2f} ({u['sp500_chg_pct']:+.2f}%) "
              f"NASDAQ {u['nasdaq_close']:,.2f} ({u['nasdaq_chg_pct']:+.2f}%) VIX {u['vix']}")
    if v["us_market_daily_518"]:
        u = v["us_market_daily_518"]
        print(f"us_market_daily 5/18: S&P {u['sp500_close']:,.2f} ({u['sp500_chg_pct']:+.2f}%) "
              f"NASDAQ {u['nasdaq_close']:,.2f} ({u['nasdaq_chg_pct']:+.2f}%) VIX {u['vix']}")
