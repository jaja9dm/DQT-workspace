"""
kosdaq_flow_collector.py — KOSDAQ 시장 외인·기관·개인 매매동향 수집기

배경:
  - KIS API `FHKST01010900` + `FID_COND_MRKT_DIV_CODE=Q` 는 INVALID FID 응답
    (KIS가 KOSDAQ 시장 전체 매매동향 미지원). KOSPI는 `J`로 정상.
  - kosdaq_condition.foreign_net_buy / inst_net_buy 가 거의 0 또는 NULL 로
    채워져, morning_brief / evening_review 가 "KIS API 한계" 경고를 송출하던 문제.

소스 우선순위:
  1) Naver 모바일 통합 API (m.stock.naver.com/api/index/KOSDAQ/integration)
     - 응답 JSON `dealTrendInfo` 에 개인·외국인·기관 순매수 (단위: 억원)
     - `programTrendInfo.indexTotalReal` 에 프로그램 순매수 (억원)
     - bizdate 가 함께 와서 어느 영업일자의 값인지 확정 가능
     - 장중에는 실시간 누적, 장 마감(15:30) 이후에는 EOD 확정값
     - 이 EOD 적재기 (daily_eod_loader, 15:35)에서 호출하므로 EOD 값 보장
  2) pykrx (현재 KRX 응답 스키마 변경으로 1.0.51 기준 KeyError("거래대금")
     발생 — 동작 안 함. 추후 패키지 복구 시 자동 사용되도록 try/except 로 보존)
  3) NULL + 명시 로그

호출 진입점:
  fetch_kosdaq_flow(target_date=None) -> dict
    {
      "foreign_net_buy":  float | None,   # 억원
      "inst_net_buy":     float | None,
      "indiv_net_buy":    float | None,
      "program_net_buy":  float | None,
      "source":           "naver" | "pykrx" | "none",
      "bizdate":          "YYYY-MM-DD" | None,   # 데이터 영업일
      "reliable":         bool,
    }
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


# ── 상수 ──────────────────────────────────────────────────────

_NAVER_KOSDAQ_URL = "https://m.stock.naver.com/api/index/KOSDAQ/integration"
_NAVER_KOSPI_URL = "https://m.stock.naver.com/api/index/KOSPI/integration"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
    "Accept": "application/json",
}


# ── 유틸 ──────────────────────────────────────────────────────

def _parse_signed_int(s: str | int | float | None) -> float | None:
    """'+1,234' / '-999' / '12,345' / 1234 → float. 실패 시 None."""
    if s is None:
        return None
    try:
        if isinstance(s, (int, float)):
            return float(s)
        cleaned = str(s).strip().replace(",", "").replace("+", "")
        if not cleaned:
            return None
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _norm_bizdate(s: str | None) -> str | None:
    """'20260514' → '2026-05-14'. 실패 시 None."""
    if not s:
        return None
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


# ── 1) Naver 모바일 통합 API ─────────────────────────────────

def fetch_kosdaq_flow_naver(target_date: date | None = None) -> dict | None:
    """Naver 모바일 통합 API에서 KOSDAQ 시장 매매동향 추출.

    Args:
        target_date: 목표 영업일자. None이면 응답 그대로 반환.
                     주어진 경우 응답 bizdate와 다르면 None 반환(과거 데이터 보강 불가).

    Returns:
        dict | None — 실패/날짜 불일치 시 None.
    """
    try:
        resp = requests.get(_NAVER_KOSDAQ_URL, headers=_HEADERS, timeout=8)
        if resp.status_code != 200:
            logger.warning(f"[kosdaq_flow] Naver HTTP {resp.status_code}")
            return None
        try:
            data: dict[str, Any] = resp.json()
        except ValueError as e:
            logger.warning(f"[kosdaq_flow] Naver JSON 파싱 실패: {e}")
            return None
    except Exception as e:
        logger.warning(f"[kosdaq_flow] Naver 요청 실패: {e}")
        return None

    deal = data.get("dealTrendInfo") or {}
    prog = data.get("programTrendInfo") or {}

    bizdate_raw = deal.get("bizdate") or prog.get("bizdate")
    bizdate = _norm_bizdate(bizdate_raw)

    # target_date 검증 — 과거 보강에는 사용 불가 (Naver 통합 API는 당일만 제공)
    if target_date is not None and bizdate and bizdate != target_date.isoformat():
        logger.info(
            f"[kosdaq_flow] Naver bizdate({bizdate}) != target({target_date}) — 사용 불가"
        )
        return None

    foreign = _parse_signed_int(deal.get("foreignValue"))
    inst = _parse_signed_int(deal.get("institutionalValue"))
    indiv = _parse_signed_int(deal.get("personalValue"))
    program = _parse_signed_int(prog.get("indexTotalReal"))

    # 최소 한 값은 있어야 의미 있음
    if foreign is None and inst is None and indiv is None:
        logger.warning("[kosdaq_flow] Naver 응답에서 매매동향 필드 모두 누락")
        return None

    return {
        "foreign_net_buy": foreign,
        "inst_net_buy": inst,
        "indiv_net_buy": indiv,
        "program_net_buy": program,
        "bizdate": bizdate,
    }


# ── 2) pykrx 폴백 ────────────────────────────────────────────

def fetch_kosdaq_flow_pykrx(target_date: date | None = None) -> dict | None:
    """pykrx 시장 투자자별 매매대금 조회.

    주의: 1.0.51 / 1.0.45 / 1.0.30 모두 KRX 응답 스키마 변경으로 KeyError("거래대금")
    또는 JSONDecodeError 발생 (2026-05-14 재검증). 다운그레이드도 해결 불가 —
    KRX 페이지(data.krx.co.kr)가 변경된 것이라 pykrx 패키지 업데이트 대기 중.
    현재는 항상 None 반환 (try/except로 무해하게 폴백). 향후 패키지 복구 시 자동 활용.

    Returns:
        dict | None
    """
    if target_date is None:
        target_date = date.today()
    ds = target_date.strftime("%Y%m%d")
    try:
        from pykrx import stock  # noqa: WPS433  (lazy import)
    except Exception as e:
        logger.debug(f"[kosdaq_flow] pykrx import 실패: {e}")
        return None

    try:
        df = stock.get_market_trading_value_by_investor(ds, ds, "KOSDAQ")
    except Exception as e:
        logger.debug(f"[kosdaq_flow] pykrx 호출 실패: {e}")
        return None

    if df is None or df.empty:
        return None

    try:
        # 단위: 원 → 억원
        def _eok(label: str) -> float | None:
            try:
                for idx in df.index:
                    if label in str(idx):
                        v = df.loc[idx, "순매수"]
                        return round(float(v) / 1e8, 1)
            except Exception:
                pass
            return None

        foreign = _eok("외국인")
        # pykrx는 "기관계" 또는 "기관" 라벨이 다양 — 여러 매핑 시도
        inst = _eok("기관합계") or _eok("기관계") or _eok("기관")
        indiv = _eok("개인")
    except Exception as e:
        logger.debug(f"[kosdaq_flow] pykrx 파싱 실패: {e}")
        return None

    if foreign is None and inst is None and indiv is None:
        return None

    return {
        "foreign_net_buy": foreign,
        "inst_net_buy": inst,
        "indiv_net_buy": indiv,
        "program_net_buy": None,    # pykrx 시장 전체 프로그램은 별도 API 필요
        "bizdate": target_date.isoformat(),
    }


# ── 3) 통합 진입점 ───────────────────────────────────────────

def fetch_kosdaq_flow(target_date: date | None = None) -> dict:
    """다중 폴백 KOSDAQ 매매동향.

    Args:
        target_date: 목표 영업일자 (None이면 오늘).

    Returns:
        {
            "foreign_net_buy": float | None,
            "inst_net_buy":    float | None,
            "indiv_net_buy":   float | None,
            "program_net_buy": float | None,
            "source":          "naver" | "pykrx" | "none",
            "bizdate":         "YYYY-MM-DD" | None,
            "reliable":        bool,
        }
    """
    if target_date is None:
        target_date = date.today()

    # 1차: Naver
    naver = fetch_kosdaq_flow_naver(target_date)
    if naver and naver.get("foreign_net_buy") is not None:
        logger.info(
            f"[kosdaq_flow] Naver 적중 — {naver.get('bizdate')} | "
            f"외인 {naver['foreign_net_buy']:+,.0f}억 | "
            f"기관 {naver.get('inst_net_buy', 0):+,.0f}억"
        )
        return {**naver, "source": "naver", "reliable": True}

    # 2차: pykrx
    pykrx_data = fetch_kosdaq_flow_pykrx(target_date)
    if pykrx_data and pykrx_data.get("foreign_net_buy") is not None:
        logger.info(
            f"[kosdaq_flow] pykrx 적중 — {pykrx_data.get('bizdate')} | "
            f"외인 {pykrx_data['foreign_net_buy']:+,.0f}억"
        )
        return {**pykrx_data, "source": "pykrx", "reliable": True}

    # 3차: 실패
    logger.warning(
        f"[kosdaq_flow] 모든 소스 실패 — target={target_date} (Naver/pykrx 모두 None)"
    )
    return {
        "foreign_net_buy": None,
        "inst_net_buy": None,
        "indiv_net_buy": None,
        "program_net_buy": None,
        "source": "none",
        "bizdate": None,
        "reliable": False,
    }


# ── KOSPI 보조 (KIS 보강용) ──────────────────────────────────

def fetch_kospi_flow_naver(target_date: date | None = None) -> dict | None:
    """KOSPI 매매동향 (KIS와 교차검증용)."""
    try:
        resp = requests.get(_NAVER_KOSPI_URL, headers=_HEADERS, timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
    except Exception:
        return None
    deal = data.get("dealTrendInfo") or {}
    prog = data.get("programTrendInfo") or {}
    bizdate = _norm_bizdate(deal.get("bizdate") or prog.get("bizdate"))
    if target_date is not None and bizdate and bizdate != target_date.isoformat():
        return None
    foreign = _parse_signed_int(deal.get("foreignValue"))
    if foreign is None:
        return None
    return {
        "foreign_net_buy": foreign,
        "inst_net_buy": _parse_signed_int(deal.get("institutionalValue")),
        "indiv_net_buy": _parse_signed_int(deal.get("personalValue")),
        "program_net_buy": _parse_signed_int(prog.get("indexTotalReal")),
        "bizdate": bizdate,
    }


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    import sys

    tgt = None
    if len(sys.argv) >= 2:
        try:
            tgt = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except Exception:
            print(f"Invalid date: {sys.argv[1]} (expected YYYY-MM-DD)")
            sys.exit(2)
    result = fetch_kosdaq_flow(tgt)
    print(json.dumps(result, ensure_ascii=False, indent=2))
