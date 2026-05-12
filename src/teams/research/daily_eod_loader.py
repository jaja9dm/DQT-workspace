"""
daily_eod_loader.py — 장 마감 후 EOD 데이터 일괄 적재

어시스턴트 모델 전환 (2026-05-12) — Phase 4.

실행 시각: 평일 15:35 (장 마감 5분 후, 일일 리포트 15:40 전)
역할:
  - 거래대금 TOP 100 종목 메타 → daily_top_value
  - 미국 야간장 마감 스냅샷 → us_market_daily
  - KOSDAQ 시황 → kosdaq_condition (+ pykrx 보강 시도)
  - 뉴스 기반 테마 자동 추출 → ticker_themes

핵심 함수:
  run_daily_eod_load() -> dict

각 단계는 try/except로 격리되어 한 단계 실패해도 다음 단계로 진행한다.
KIS API rate limit (20 req/s)를 준수하기 위해 종목당 sleep 0.05~0.1s 적용.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, RequestPriority
from src.infra.sector_mapper import get_sector, refresh_themes_from_news
from src.infra.short_selling import get_short_ratio
from src.infra.us_market import fetch_us_market_data, save_us_market_snapshot
from src.teams.domestic_market.collector import save_kosdaq_condition
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ── 상수 ──────────────────────────────────────────────────────

_TOP_N = 100
_INTER_CALL_SLEEP = 0.07            # KIS rate limit 여유 (10/s 예산)
_KIS_INVESTOR_DAILY_PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"
_KIS_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"


# ── 메인 진입점 ───────────────────────────────────────────────

def run_daily_eod_load() -> dict:
    """장 마감 후 EOD 데이터 일괄 적재.

    Returns:
        {
            "top_value_count": int,   # daily_top_value 저장 건수
            "us_loaded":       bool,
            "kosdaq_loaded":   bool,
            "themes_refreshed": int,
        }
    """
    today = date.today().isoformat()
    logger.info(f"[EOD] 일괄 적재 시작 — {today}")

    result = {
        "top_value_count":   0,
        "us_loaded":         False,
        "kosdaq_loaded":     False,
        "themes_refreshed":  0,
    }

    # ─── 1. 거래대금 TOP 100 ───────────────────────────────
    try:
        result["top_value_count"] = _load_top_value_snapshot()
        logger.info(f"[EOD] daily_top_value 적재 완료 — {result['top_value_count']}건")
    except Exception as e:
        logger.error(f"[EOD] daily_top_value 적재 오류: {e}", exc_info=True)

    # ─── 2. 미국 시장 스냅샷 ───────────────────────────────
    try:
        snap = fetch_us_market_data()
        save_us_market_snapshot(snap)
        result["us_loaded"] = True
        logger.info("[EOD] us_market_daily 적재 완료")
    except Exception as e:
        logger.error(f"[EOD] us_market_daily 적재 오류: {e}", exc_info=True)

    # ─── 3. KOSDAQ 시황 보강 ──────────────────────────────
    try:
        row = save_kosdaq_condition()
        # pykrx로 거래대금 보강 시도 (현재 KIS에서 값 0)
        _augment_kosdaq_with_pykrx(today)
        # 외인/기관 net buy가 NULL이면 daily_top_value(KOSDAQ 종목들) 합계로 폴백
        _augment_kosdaq_flow_from_top_value(today)
        result["kosdaq_loaded"] = True
        logger.info(f"[EOD] kosdaq_condition 적재 완료 ({row['close']:,.2f})")
    except Exception as e:
        logger.error(f"[EOD] kosdaq_condition 적재 오류: {e}", exc_info=True)

    # ─── 4. 테마 갱신 ─────────────────────────────────────
    try:
        n = refresh_themes_from_news()
        result["themes_refreshed"] = n
        logger.info(f"[EOD] ticker_themes 갱신 완료 — {n}건")
    except Exception as e:
        logger.error(f"[EOD] ticker_themes 갱신 오류: {e}", exc_info=True)

    logger.info(f"[EOD] 완료 — {result}")
    return result


# ── 1. 거래대금 TOP 100 ───────────────────────────────────────

def _load_top_value_snapshot() -> int:
    """거래대금 TOP 100 종목을 daily_top_value에 INSERT OR REPLACE.

    KOSPI 50 + KOSDAQ 50 을 거래대금 내림차순으로 합치고 다시 정렬 → TOP 100.
    Returns:
        저장된 row 수.
    """
    today = date.today().isoformat()
    gw = KISGateway()

    # 1. KIS API: 거래대금 순위 (KOSPI + KOSDAQ 각 60)
    kospi_top = []
    kosdaq_top = []
    try:
        kospi_top = gw.get_trading_value_ranking(market="J", top_n=60)
    except Exception as e:
        logger.warning(f"[EOD] KOSPI 거래대금 순위 조회 실패: {e}")
    try:
        kosdaq_top = gw.get_trading_value_ranking(market="Q", top_n=60)
    except Exception as e:
        logger.warning(f"[EOD] KOSDAQ 거래대금 순위 조회 실패: {e}")

    combined: list[dict] = []
    seen: set[str] = set()
    for item in (kospi_top or []) + (kosdaq_top or []):
        tk = item.get("ticker") or ""
        if not tk or tk in seen:
            continue
        seen.add(tk)
        combined.append(item)

    # KIS 순위 API가 두 시장 모두 실패한 경우 FDR로 폴백 (정확한 거래대금 TOP)
    if not combined:
        logger.warning("[EOD] KIS 거래대금 순위 데이터 없음 — 폴백: FDR StockListing")
        combined = _fallback_top_from_fdr()

    # 그래도 비면 마지막 폴백: hot_list
    if not combined:
        logger.warning("[EOD] FDR 폴백도 실패 — hot_list 사용")
        combined = _fallback_top_from_hot_list()

    # 거래대금 내림차순 → TOP N
    combined.sort(key=lambda x: int(x.get("trading_value") or 0), reverse=True)
    top_list = combined[:_TOP_N]

    if not top_list:
        logger.warning("[EOD] 적재할 종목 없음 — hot_list도 비어있음")
        return 0

    # 2. 각 종목 메타 조회 + DB 저장
    saved = 0
    for idx, item in enumerate(top_list, 1):
        ticker = str(item.get("ticker") or "").zfill(6)
        if not ticker or len(ticker) != 6:
            continue
        try:
            row = _build_snapshot_row(
                ticker=ticker,
                rank=idx,
                seed_item=item,
                today=today,
                gw=gw,
            )
            _save_daily_top_value(row)
            saved += 1
        except Exception as e:
            logger.debug(f"[EOD] {ticker} 메타 적재 스킵: {e}")
        # rate limit 보호
        time.sleep(_INTER_CALL_SLEEP)

    return saved


def _fallback_top_from_fdr() -> list[dict]:
    """FDR StockListing(KOSPI/KOSDAQ)에서 거래대금 TOP 100.

    KIS 거래대금 순위 API가 막혔을 때 사용. Amount/Close/ChagesRatio 즉시 제공되므로
    종목별 KIS get_price 호출 실패해도 seed 값으로 시세 채우기 가능.
    """
    try:
        import FinanceDataReader as fdr  # noqa: WPS433 (지연 import)
        items: list[dict] = []
        for market in ("KOSPI", "KOSDAQ"):
            try:
                df = fdr.StockListing(market)
                if df is None or df.empty:
                    continue
                if "Amount" not in df.columns:
                    continue
                df = df.dropna(subset=["Code"])
                df = df.sort_values("Amount", ascending=False, na_position="last")
                top = df.head(80)
                for _, r in top.iterrows():
                    try:
                        tk = str(r.get("Code") or "").zfill(6)
                        if not tk or len(tk) != 6:
                            continue
                        items.append({
                            "ticker":        tk,
                            "name":          str(r.get("Name") or ""),
                            "trading_value": int(r.get("Amount") or 0),
                            "change_pct":    float(r.get("ChagesRatio") or 0),
                            "price":         float(r.get("Close") or 0),
                            "volume":        int(r.get("Volume") or 0),
                            "frgn_net_buy":  0,
                            "inst_net_buy":  0,
                            "market":        market,  # KOSPI / KOSDAQ
                        })
                    except Exception:
                        continue
            except Exception as e:
                logger.warning(f"[EOD] FDR 폴백 — {market} 조회 실패: {e}")
        return items
    except Exception as e:
        logger.warning(f"[EOD] FDR 폴백 전체 실패: {e}")
        return []


def _fallback_top_from_hot_list() -> list[dict]:
    """KIS API 순위 조회 실패 시 hot_list에서 폴백."""
    rows = fetch_all(
        """
        SELECT ticker, name, trading_value, price_change_pct
        FROM hot_list
        WHERE date(created_at) = date('now', 'localtime')
        ORDER BY trading_value DESC
        LIMIT 100
        """
    )
    items: list[dict] = []
    for r in rows:
        items.append({
            "ticker":       (r["ticker"] or "").zfill(6),
            "name":         r["name"] or "",
            "trading_value": int(r["trading_value"] or 0),
            "change_pct":   float(r["price_change_pct"] or 0),
            "price":        0.0,
            "volume":       0,
            "frgn_net_buy": 0,
            "inst_net_buy": 0,
        })
    return items


def _build_snapshot_row(
    *,
    ticker: str,
    rank: int,
    seed_item: dict,
    today: str,
    gw: KISGateway,
) -> dict:
    """KIS API + 부가 데이터 결합 → daily_top_value row dict."""
    # 1. 현재가/시세 (FHKST01010100) — seed에 일부 값이 있어도 메타가 더 풍부
    price_resp: dict[str, Any] = {}
    try:
        price_resp = gw.get_price(ticker, priority=RequestPriority.BACKGROUND)
    except Exception as e:
        logger.debug(f"get_price 실패 [{ticker}]: {e}")
    out = price_resp.get("output", {}) if price_resp else {}

    def _f(key: str, default: float = 0.0) -> float:
        try:
            return float(out.get(key, default) or default)
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int = 0) -> int:
        try:
            return int(out.get(key, default) or default)
        except (TypeError, ValueError):
            return default

    open_p   = _f("stck_oprc")
    high_p   = _f("stck_hgpr")
    low_p    = _f("stck_lwpr")
    close_p  = _f("stck_prpr") or float(seed_item.get("price") or 0)
    prev_c   = _f("stck_sdpr")     # 전일 종가
    chg_pct  = _f("prdy_ctrt") if out else float(seed_item.get("change_pct") or 0)
    volume   = _i("acml_vol") or int(seed_item.get("volume") or 0)
    tv       = _i("acml_tr_pbmn") or int(seed_item.get("trading_value") or 0)
    market_cap = _f("hts_avls")          # 단위: 억원
    listed_shr = _i("lstn_stcn")
    high_52w   = _f("w52_hgpr")
    low_52w    = _f("w52_lwpr")
    per        = _f("per")
    pbr        = _f("pbr")
    eps        = _f("eps")
    bps        = _f("bps")
    # 외인 보유율
    frgn_hold_pct = _f("hts_frgn_ehrt")

    # 2. 투자자별 매매동향 (FHKST01010900) — 종목별
    indiv_nb = foreign_nb = inst_nb = prog_nb = 0.0
    try:
        inv_resp = gw.request(
            method="GET",
            path=_KIS_INVESTOR_DAILY_PATH,
            tr_id="FHKST01010900",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
            },
            priority=RequestPriority.BACKGROUND,
        )
        inv_out = inv_resp.get("output", {})
        if isinstance(inv_out, list):
            inv_out = inv_out[0] if inv_out else {}
        # 단위: 원 → 백만원 단위로 변환
        def _bn(k: str) -> float:
            try:
                return round(float(inv_out.get(k, 0) or 0) / 1_000_000, 1)
            except (TypeError, ValueError):
                return 0.0
        indiv_nb   = _bn("prsn_ntby_qty")
        foreign_nb = _bn("frgn_ntby_qty")
        inst_nb    = _bn("orgn_ntby_qty")
        prog_nb    = 0.0  # KIS 종목별 프로그램 미제공
    except Exception as e:
        logger.debug(f"investor flow 실패 [{ticker}]: {e}")
        # seed에 있던 외인/기관 수량을 폴백으로 사용
        foreign_nb = float(seed_item.get("frgn_net_buy") or 0) / 1_000_000
        inst_nb    = float(seed_item.get("inst_net_buy") or 0) / 1_000_000

    # 3. 공매도 비율
    try:
        short_ratio = float(get_short_ratio(ticker) or 0.0)
    except Exception:
        short_ratio = 0.0

    # 4. 섹터
    try:
        sector = get_sector(ticker)
    except Exception:
        sector = "기타"

    # 5. 기술지표: 오늘 hot_list 우선 사용 (이미 계산된 값)
    rsi_14 = atr_pct = bb_w = None
    macd_signal = None
    try:
        hl_row = fetch_one(
            """
            SELECT rsi_14, atr_pct, bb_width_ratio, momentum_score
            FROM hot_list
            WHERE ticker = ? AND date(created_at) = date('now','localtime')
            ORDER BY created_at DESC LIMIT 1
            """,
            (ticker,),
        )
        if hl_row:
            rsi_14   = float(hl_row["rsi_14"] or 0) or None
            atr_pct  = float(hl_row["atr_pct"] or 0) or None
            bb_w     = float(hl_row["bb_width_ratio"] or 0) or None
    except Exception:
        pass
    # MACD 신호는 intraday_macd_signal 최신값 활용
    try:
        macd_row = fetch_one(
            """
            SELECT signal
            FROM intraday_macd_signal
            WHERE ticker = ? AND date(created_at) = date('now','localtime')
            ORDER BY created_at DESC LIMIT 1
            """,
            (ticker,),
        )
        if macd_row:
            s = (macd_row["signal"] or "").lower()
            if s in ("buy", "bull"):
                macd_signal = "bull"
            elif s in ("sell", "bear"):
                macd_signal = "bear"
            else:
                macd_signal = "neutral"
    except Exception:
        pass

    # 6. 종목명
    name = str(seed_item.get("name") or out.get("hts_kor_isnm") or "").strip()
    if not name:
        # universe / hot_list 보강
        try:
            u_row = fetch_one(
                "SELECT name FROM universe WHERE ticker = ? LIMIT 1", (ticker,),
            )
            if u_row and u_row["name"]:
                name = u_row["name"]
        except Exception:
            pass

    return {
        "date":             today,
        "rank":             rank,
        "ticker":           ticker,
        "name":             name,
        "sector":           sector,
        "open_price":       open_p or None,
        "high_price":       high_p or None,
        "low_price":        low_p or None,
        "close_price":      close_p or None,
        "prev_close":       prev_c or None,
        "chg_pct":          chg_pct,
        "volume":           volume,
        "trading_value":    tv,
        "market_cap":       market_cap or None,
        "listed_shares":    listed_shr or None,
        "high_52w":         high_52w or None,
        "low_52w":          low_52w or None,
        "per":              per or None,
        "pbr":              pbr or None,
        "eps":              eps or None,
        "bps":              bps or None,
        "indiv_net_buy":    indiv_nb,
        "foreign_net_buy":  foreign_nb,
        "foreign_hold_pct": frgn_hold_pct or None,
        "inst_net_buy":     inst_nb,
        "program_net_buy":  prog_nb,
        "margin_balance":   None,        # KIS 직접 미제공 — 별도 보강 단계 필요
        "short_volume":     None,
        "short_value":      None,
        "short_ratio":      short_ratio,
        "rsi_14":           rsi_14,
        "macd_signal":      macd_signal,
        "atr_pct":          atr_pct,
        "bb_width_ratio":   bb_w,
    }


def _save_daily_top_value(row: dict) -> None:
    """daily_top_value INSERT OR REPLACE."""
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


# ── 3. KOSDAQ 거래대금 보강 ──────────────────────────────────
#
# 2026-05-12: pykrx의 get_index_ohlcv_by_date가 KRX 응답 스키마 변경으로
# KeyError("지수명") 발생 — 사용 불가. KRX 직접 JSON API도 403 차단.
# → Naver Finance 스크래핑 (안정·공식 사이트, 광범위 사용)으로 대체.

def _augment_kosdaq_with_pykrx(today: str) -> None:
    """KOSDAQ 종합 거래대금/거래량을 Naver Finance에서 가져와 갱신.

    함수명은 기존 호환을 위해 유지. 내부 구현은 Naver 스크래핑.
    Naver Finance가 차단/장애일 경우에는 조용히 스킵.
    """
    import re

    import requests

    try:
        hdr = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko)"
            ),
            "Referer": "https://finance.naver.com/",
        }
        url = "https://finance.naver.com/sise/sise_index_day.naver?code=KOSDAQ&page=1"
        resp = requests.get(url, headers=hdr, timeout=8)
        if resp.status_code != 200:
            logger.debug(f"[EOD] Naver KOSDAQ HTTP {resp.status_code} — 보강 스킵")
            return
        # 각 행: 날짜·종가·등락폭·등락률·거래량(천주)·거래대금(백만원)
        rx = re.compile(
            r'<tr>\s*'
            r'<td class="date">(\d{4}\.\d{2}\.\d{2})</td>\s*'
            r'<td class="number_1">([\d,]+\.\d+)</td>'
            r'.*?'
            r'<td class="number_1"[^>]*>\s*<span[^>]*>\s*([+-]?[\d.]+)%\s*</span>'
            r'.*?'
            r'<td class="number_1"[^>]*>([\d,]+)</td>\s*'
            r'<td class="number_1"[^>]*>([\d,]+)</td>',
            re.DOTALL,
        )
        rows = rx.findall(resp.text)
        if not rows:
            logger.debug("[EOD] Naver KOSDAQ 파싱 실패 — 0건")
            return
        # 가장 최근(첫 행)이 오늘 (장 마감 후 15:35+ 호출)
        date_str, _close, _chg, vol_thr, tv_mil = rows[0]
        # 날짜 일치 확인 (today=YYYY-MM-DD, date_str=YYYY.MM.DD)
        date_norm = date_str.replace(".", "-")
        if date_norm != today:
            logger.debug(
                f"[EOD] Naver 첫 행 날짜({date_norm}) != today({today}) — "
                f"장 마감 전이거나 휴장일 → 보강 스킵"
            )
            return
        vol_int = int(vol_thr.replace(",", ""))    # 천주
        tv_int  = int(tv_mil.replace(",", ""))     # 백만원
        # DB 스키마: volume=만주, trading_value=억원
        vol_man = round(vol_int / 10.0, 1)         # 천주 → 만주 (÷10)
        tv_eok  = round(tv_int / 100.0, 1)         # 백만원 → 억원 (÷100)
        execute(
            """
            UPDATE kosdaq_condition
            SET volume = ?, trading_value = ?
            WHERE date = ?
            """,
            (vol_man, tv_eok, today),
        )
        logger.info(
            f"[EOD] kosdaq_condition Naver 보강 — 거래대금 {tv_eok:,.0f}억 | "
            f"거래량 {vol_man:,.0f}만주"
        )
    except Exception as e:
        logger.debug(f"[EOD] Naver KOSDAQ 보강 스킵: {e}")


# ── 4. KOSDAQ 외인/기관 폴백 ──────────────────────────────────

def _augment_kosdaq_flow_from_top_value(today: str) -> None:
    """KOSDAQ 외인/기관 순매수가 NULL인 경우 daily_top_value 합계로 폴백.

    daily_top_value에 적재된 KOSDAQ 종목들의 foreign_net_buy/inst_net_buy를 합산.
    완전한 시장 전체 수치는 아니지만 거래대금 TOP 종목 합계라 방향성·규모 가늠 가능.

    원래 데이터가 있으면 건드리지 않음.
    """
    try:
        row = fetch_one(
            "SELECT foreign_net_buy, inst_net_buy FROM kosdaq_condition WHERE date = ?",
            (today,),
        )
        if not row:
            return
        f_val = row["foreign_net_buy"]
        i_val = row["inst_net_buy"]
        # 둘 다 이미 NULL 아니고 정상값이면 그대로 둠
        if f_val is not None and i_val is not None and (f_val != 0 or i_val != 0):
            return

        # KOSDAQ 종목 ticker 셋 구해서 daily_top_value 합산
        import FinanceDataReader as fdr  # noqa: WPS433
        try:
            df_q = fdr.StockListing("KOSDAQ")
            kosdaq_set = set(df_q["Code"].astype(str).str.zfill(6).tolist())
        except Exception:
            kosdaq_set = set()
        if not kosdaq_set:
            return

        rows = fetch_all(
            """
            SELECT ticker, foreign_net_buy, inst_net_buy
            FROM daily_top_value
            WHERE date = ?
            """,
            (today,),
        )
        f_sum = 0.0
        i_sum = 0.0
        n = 0
        for r in rows:
            tk = (r["ticker"] or "").zfill(6)
            if tk not in kosdaq_set:
                continue
            f_sum += float(r["foreign_net_buy"] or 0)
            i_sum += float(r["inst_net_buy"] or 0)
            n += 1
        if n == 0:
            return

        # 단위: 단일 종목 row는 백만원 단위(daily_eod_loader._build_snapshot_row에서
        # _bn으로 변환됨) — 억원으로 다시 환산: ÷100
        f_eok = round(f_sum / 100.0, 1)
        i_eok = round(i_sum / 100.0, 1)
        # 데이터가 충분히 모이지 않은 경우 (rows < 5)는 그래도 표기 — 가치보다 부재가 더 안 좋음
        execute(
            """
            UPDATE kosdaq_condition
            SET foreign_net_buy = COALESCE(foreign_net_buy, ?),
                inst_net_buy    = COALESCE(inst_net_buy, ?)
            WHERE date = ?
            """,
            (f_eok, i_eok, today),
        )
        logger.info(
            f"[EOD] kosdaq_condition flow 폴백 (top_value {n}종목 합계) — "
            f"외인 {f_eok:+.0f}억 | 기관 {i_eok:+.0f}억"
        )
    except Exception as e:
        logger.debug(f"[EOD] kosdaq flow 폴백 실패: {e}")


# ── CLI 진입점 ────────────────────────────────────────────────

if __name__ == "__main__":
    res = run_daily_eod_load()
    print(f"완료: {res}")
