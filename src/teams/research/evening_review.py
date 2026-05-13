"""
evening_review.py — 저녁 회고 (15:40 발송)

어시스턴트 모델 전환 (2026-05-12) — Phase 6.

역할:
  매일 15:40 (한국 시각) 오늘 결과를 회고하고 학습을 도출.
  - 오늘 아침 picks/avoids 결과 평가 (적중률)
  - 오늘 시장 종합 (KOSPI/KOSDAQ/거래대금/수급)
  - 강·약세 섹터 TOP 5
  - Claude 분석으로 새 lessons 도출 + 기존 lessons 검증/실패
  - 텔레그램 발송 + DB 저장

핵심 함수:
  run_evening_review() -> dict
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import check_claude_error, notify

try:
    from src.infra.news_collector import (
        collect_and_save as _collect_and_save_news,
        get_news_for_brief as _get_news_for_brief,
    )
except Exception:
    _collect_and_save_news = None
    _get_news_for_brief = None

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

try:
    from src.scheduler.scheduler import is_trading_day
except Exception:
    def is_trading_day(dt: datetime | None = None) -> bool:
        if dt is None:
            dt = datetime.now()
        return dt.weekday() < 5


_SYSTEM_PROMPT = """당신은 한국 주식시장 매매 어시스턴트의 회고·학습 모듈입니다.
오늘 아침 브리핑의 추천·회피 종목 실제 결과, 오늘 시장 종합, 그리고 적용했던 누적 학습을 받아
어떤 패턴이 통했고 안 통했는지 분석하고, 새 교훈 3~5개를 도출합니다.

## 🟢 한글 우선 — 영어 단어 최소화 (절대 위반 금지)

사용자에게 보이는 모든 텍스트(content, evidence, tomorrow_outlook, headline)는 **한국어로만** 작성.
영어 단어 사용은 다음 예외에만 허용:
  - 고유명사: KOSPI, KOSDAQ, NASDAQ, S&P500, Dow, VIX, SOXX, LIT, AI, EV, ETF, USD, KRW, TSMC, NVDA 등
  - 종목명 영어 일부 (예: SK하이닉스)
  - 약어: RSI, MACD, PER, PBR, ROE, EPS, GDP, FOMC, CPI, PPI 등

**금지 영어 단어** — 반드시 한글로 치환:
  - regime → "국면" / reversal → "반등 국면" / weak → "약세 국면" / strong → "강세 국면"
  - sideways → "횡보" / volatile → "변동성 큼"
  - bullish → "상승/강세" / bearish → "하락/약세" / neutral → "중립"
  - pullback_rebound → "조정 후 반등 신호" / opening_plunge_rebound → "장 시작 급락 후 반등"
  - breakout → "돌파" / momentum → "모멘텀(추세)" / FOMO → "추격 매수 심리"
  - confidence → "신뢰도" / strategy_tone → "전략 톤"

JSON의 enum 필드(category, applicable_regime)는 영문값 유지(파이프라인 호환).
하지만 content·evidence·tomorrow_outlook·headline 등 사용자 노출 텍스트에서는 enum 단어를
한글로 풀어쓸 것. headline에 "weak 판단 고수" 같은 표현 절대 금지 — "약세 국면 판단 고수"처럼 한글로.

## 분석 원칙
1. 적중 4단계 평가:
   - full(✅):    conf>=3 AND chg>=+1.5%  OR  conf<=2 AND chg<=-1.5%
   - partial(🟢): conf>=3 AND 0<chg<1.5%  OR  conf<=2 AND (-1.5%<chg<0 또는 chg>=+1.5%)
   - neutral(⚠️): |chg|<0.5%
   - miss(❌):    conf>=3 AND chg<0  OR  conf<=2 AND 0<chg<1.5%
   - 회피 적중 = chg_pct<0 (별도 평가)
2. 새 lessons는 구체적·반복 가능해야 함. "외인 -2000억+ 4일 연속 → 다음날 KOSPI 약세 70%" 같은 식.
3. 일반론·시황 단어("주의 필요" 등) 금지.
4. 기존 lessons 중 오늘 시장에서 검증된 것은 validated_ids에, 실패한 것은 failed_ids에 정확한 ID 명시.
5. 카테고리: pattern | sector | macro | avoid | entry_timing | risk
6. 회피 룰 만료: 전일 KOSPI -1.5%↓ 급락 후 오늘 +1%↑ 반등 같은 통계적 반전이 나오면, "weak/reversal 국면 회피" 류 학습은 무차별 적용 X — 실제 결과로 검증/실패 판단할 것.

## 응답 형식 (STRICT JSON만 — 코드 펜스/주석/설명문/trailing comma 금지)
{
  "new_lessons": [
    {
      "category": "pattern|sector|macro|avoid|entry_timing|risk",
      "content": "<구체 교훈 1~2줄 한국어>",
      "evidence": "<오늘 어떤 케이스에서 도출됐는지>",
      "applicable_regime": ["strong"|"sideways"|"weak"|"reversal"|"volatile"]
    }
  ],
  "lessons_validated_ids": [<int id>, ...],
  "lessons_failed_ids":    [<int id>, ...],
  "tomorrow_outlook": "<내일 전망 2~4문장>",
  "headline": "<한 줄 요약 — 30자 이내>"
}

규칙:
- 첫 글자 `{` 마지막 글자 `}` — 그 외 문자 없음.
- new_lessons 0~5개. validated/failed 0~10개 각각.
- applicable_regime: 어떤 시장 국면에만 적용되는 교훈인지 1~5개 배열로 표시.
  특정 국면 없이 전체 적용이면 빈 배열 []. 예) 약세장 한정이면 ["weak","volatile"]."""


_TELEGRAM_LIMIT = 4000


# ── 영어 → 한글 후처리 (Claude가 잊고 영어 쓴 경우 대비) ─────
_EN_TO_KR: dict[str, str] = {
    "reversal":               "반등 국면",
    "sideways":               "횡보",
    "volatile":               "변동성 큼",
    "regime":                 "국면",
    "bullish":                "강세",
    "bearish":                "약세",
    "neutral":                "중립",
    "pullback_rebound":       "조정 후 반등 신호",
    "opening_plunge_rebound": "장 시작 급락 후 반등",
    "breakout":               "돌파",
    "momentum":               "모멘텀(추세)",
    "strategy_tone":          "전략 톤",
    "confidence":             "신뢰도",
    "FOMO":                   "추격 매수 심리",
}
_EN_TO_KR_WORD_BOUNDARY: dict[str, str] = {
    "weak":   "약세 국면",
    "strong": "강세 국면",
}


def _kr_postprocess(text: str) -> str:
    """Claude 출력이 영어 단어를 그대로 두면 후처리로 한글 치환.

    Python의 \\b는 한글을 단어 문자로 인식하므로 lookbehind/lookahead 기반 경계 사용.
    """
    if not text:
        return text
    out = text
    boundary_l = r"(?<![A-Za-z0-9_])"
    boundary_r = r"(?![A-Za-z0-9_])"
    items = list(_EN_TO_KR.items()) + list(_EN_TO_KR_WORD_BOUNDARY.items())
    for en, kr in sorted(items, key=lambda x: -len(x[0])):
        out = re.sub(
            rf"{boundary_l}{re.escape(en)}{boundary_r}",
            kr, out, flags=re.IGNORECASE,
        )
    return out


# market_direction enum → 한글 라벨 (사용자 노출용)
_MARKET_DIR_LABEL = {
    "bullish": "강세",
    "bearish": "약세",
    "neutral": "중립",
}


# ── JSON 추출 ────────────────────────────────────────────────

def _extract_json(raw: str) -> str:
    text = raw.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if m:
        text = m.group(1)
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start: end + 1]
    text = re.sub(r",(\s*[\]}])", r"\1", text)
    return text


# ── 데이터 수집 ──────────────────────────────────────────────

def _fetch_today_briefing(today: str) -> dict | None:
    row = fetch_one(
        "SELECT * FROM morning_briefing WHERE date = ?", (today,),
    )
    return dict(row) if row else None


def _fetch_today_top_value(today: str) -> list[dict]:
    rows = fetch_all(
        "SELECT * FROM daily_top_value WHERE date = ? ORDER BY rank ASC",
        (today,),
    )
    return [dict(r) for r in rows] if rows else []


def _fetch_yesterday_top_value(today: str) -> list[dict]:
    rows = fetch_all(
        """
        SELECT date, rank, ticker, name
        FROM daily_top_value
        WHERE date < ?
        ORDER BY date DESC, rank ASC
        LIMIT 100
        """,
        (today,),
    )
    return [dict(r) for r in rows] if rows else []


def _fetch_today_kosdaq() -> dict | None:
    row = fetch_one(
        "SELECT * FROM kosdaq_condition ORDER BY date DESC LIMIT 1"
    )
    return dict(row) if row else None


def _fetch_today_market() -> dict | None:
    row = fetch_one(
        "SELECT * FROM market_condition ORDER BY created_at DESC LIMIT 1"
    )
    return dict(row) if row else None


def _fetch_active_learnings(limit: int = 20) -> list[dict]:
    rows = fetch_all(
        """
        SELECT id, category, content, confidence, times_validated, times_failed
        FROM learnings
        WHERE status = 'active'
        ORDER BY confidence DESC, times_validated DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [dict(r) for r in rows] if rows else []


def _fetch_yesterday_review() -> dict | None:
    row = fetch_one(
        "SELECT * FROM evening_review ORDER BY date DESC LIMIT 1"
    )
    return dict(row) if row else None


# ── 결과 평가 ────────────────────────────────────────────────
#
# 4단계 평가 (2026-05-13 개선):
#   ✅ full_hit:  conf>=3 AND chg>=+1.5%  (강추 + 강상승)
#                 OR conf<=2 AND chg<=-1.5% (약추/회피권 + 강하락 = 회피 정답)
#   🟢 partial:   conf>=3 AND 0 < chg < 1.5%  (강추했는데 미미한 상승 — 살짝 빗나감)
#                 OR conf<=2 AND chg >= +1.5% (낮은 confidence였는데 강상승 — 신뢰도 미스)
#   ⚠️ neutral:  |chg| < 0.5% (사실상 무변동)
#   ❌ miss:     conf>=3 AND chg<0  (강추인데 하락)
#                 OR conf<=2 AND chg가 partial/full hit에 해당 안 함

_HIT_STRONG_PCT = 1.5     # full hit 임계
_HIT_NEUTRAL_PCT = 0.5    # neutral 임계 (|chg| < 0.5%)


def _classify_pick(conf: int, chg: float) -> tuple[str, str]:
    """단일 추천 평가 → (grade, note).
    grade: 'full' | 'partial' | 'neutral' | 'miss'
    note: 한 줄 설명
    """
    if abs(chg) < _HIT_NEUTRAL_PCT:
        return "neutral", "변동 미미"
    if conf >= 3:
        if chg >= _HIT_STRONG_PCT:
            return "full", "강추 + 강상승"
        if chg > 0:
            return "partial", "강추 + 약상승"
        return "miss", "강추했으나 하락"
    # conf <= 2 (낮은 신뢰도)
    if chg <= -_HIT_STRONG_PCT:
        return "full", "회피 정답 — 강하락"
    if chg < 0:
        return "partial", "회피 정답 — 약하락"
    if chg >= _HIT_STRONG_PCT:
        return "partial", "신뢰도 미스 — 더 강하게 추천했어야"
    # 0 <= chg < 1.5% AND conf<=2
    return "miss", "낮은 신뢰도였는데 상승 시작"


def _evaluate_picks(
    picks: list[dict], today_top: list[dict]
) -> tuple[list[dict], float, float]:
    """추천 종목 평가 — 4단계.

    Returns: (results, full_hit_rate, partial_or_better_rate)
    """
    chg_map = {r["ticker"]: float(r.get("chg_pct") or 0) for r in today_top}
    results: list[dict] = []
    full_hits = 0
    partial_or_better = 0
    for p in picks:
        tk = p.get("ticker") or ""
        if not tk:
            continue
        conf = int(p.get("confidence") or 3)
        actual = chg_map.get(tk)
        if actual is None:
            results.append({
                "ticker": tk, "name": p.get("name") or tk,
                "predicted_conf": conf, "actual_chg_pct": None,
                "grade": None, "note": "no_data",
                "hit": None,   # 하위호환
            })
            continue
        grade, note = _classify_pick(conf, actual)
        if grade == "full":
            full_hits += 1
            partial_or_better += 1
        elif grade == "partial":
            partial_or_better += 1
        results.append({
            "ticker": tk, "name": p.get("name") or tk,
            "predicted_conf": conf, "actual_chg_pct": actual,
            "grade": grade, "note": note,
            "hit": grade in ("full", "partial"),   # 하위호환
            "entry": p.get("entry"),
        })
    n = len([r for r in results if r["grade"] is not None])
    full_rate = round(full_hits / n * 100, 1) if n else 0.0
    partial_rate = round(partial_or_better / n * 100, 1) if n else 0.0
    return results, full_rate, partial_rate


def _evaluate_avoids(avoids: list[dict], today_top: list[dict]) -> tuple[list[dict], float]:
    """회피 적중 = chg_pct < 0."""
    chg_map = {r["ticker"]: float(r.get("chg_pct") or 0) for r in today_top}
    results: list[dict] = []
    hits = 0
    for a in avoids:
        tk = a.get("ticker") or ""
        if not tk:
            continue
        actual = chg_map.get(tk)
        if actual is None:
            # 거래대금 TOP 100 밖이면 chg를 daily_top_value 외 데이터로는 알기 어렵
            results.append({
                "ticker": tk, "name": a.get("name") or tk,
                "actual_chg_pct": None, "hit": None, "note": "no_data",
            })
            continue
        hit = actual < 0
        if hit:
            hits += 1
        results.append({
            "ticker": tk, "name": a.get("name") or tk,
            "actual_chg_pct": actual, "hit": hit,
        })
    n = len([r for r in results if r["hit"] is not None])
    acc = round(hits / n * 100, 1) if n else 0.0
    return results, acc


# ── 시장 종합 ────────────────────────────────────────────────

_INVALID_SECTORS = {"", "nan", "none", "None", "NaN", "NULL", "null"}
# 시장 소속부는 진짜 업종이 아니므로 섹터 분석에서 제외
_SOSOK_SECTORS = {
    "우량기업부", "중견기업부", "벤처기업부", "기술성장기업부", "일반기업부",
    "관리종목(소속부없음)", "SPAC(소속부없음)", "투자주의환기종목(소속부없음)",
    "외국기업(소속부없음)",
}
_MIN_STOCKS_PER_SECTOR = 3  # 종목 수 ≥ 3인 섹터만 강·약세 분석


def _sector_strength(today_top: list[dict]) -> tuple[list[dict], list[dict]]:
    """오늘 거래대금 TOP 100 기반 섹터별 평균 등락률.

    개선:
      1. 'nan'/'None'/빈 문자열/시장 소속부(우량/중견/기술성장…) 섹터는 제외
      2. 종목 수 ≥ 3인 섹터만 분석 (단일 종목은 섹터 평균이라 부르기 부적절)
      3. 강세 = 평균 > 0 (양수만), 약세 = 평균 < 0 (음수만) — 중복 X

    Returns: (strong_top5, weak_top5)
    """
    by_sector: dict[str, list[float]] = defaultdict(list)
    for r in today_top:
        sec_raw = r.get("sector")
        sec = (sec_raw or "").strip()
        if not sec or sec in _INVALID_SECTORS or sec in _SOSOK_SECTORS:
            continue
        if sec == "기타":   # 명시적 '기타'도 노이즈
            continue
        chg = r.get("chg_pct")
        if chg is None:
            continue
        by_sector[sec].append(float(chg))

    summary = []
    for sec, chgs in by_sector.items():
        if len(chgs) < _MIN_STOCKS_PER_SECTOR:
            continue
        summary.append({
            "sector": sec,
            "avg_chg_pct": round(sum(chgs) / len(chgs), 2),
            "stock_count": len(chgs),
        })

    # 양수만 강세, 내림차순 / 음수만 약세, 오름차순
    strong = sorted(
        [s for s in summary if s["avg_chg_pct"] > 0],
        key=lambda x: x["avg_chg_pct"], reverse=True,
    )[:5]
    weak = sorted(
        [s for s in summary if s["avg_chg_pct"] < 0],
        key=lambda x: x["avg_chg_pct"],
    )[:5]
    return strong, weak


_KOSDAQ_TICKER_CACHE: set[str] | None = None


def _is_kosdaq(ticker: str) -> bool:
    """ticker가 KOSDAQ 종목인지 — FDR StockListing('KOSDAQ') 캐시 사용.

    호출 시 1회 캐시. 실패 시 False(=KOSPI 가정).
    """
    global _KOSDAQ_TICKER_CACHE
    if _KOSDAQ_TICKER_CACHE is None:
        try:
            import FinanceDataReader as fdr  # noqa: WPS433
            df = fdr.StockListing("KOSDAQ")
            codes = df["Code"].astype(str).str.zfill(6).tolist() if df is not None and not df.empty else []
            _KOSDAQ_TICKER_CACHE = set(codes)
        except Exception as e:
            logger.debug(f"KOSDAQ 캐시 로드 실패: {e}")
            _KOSDAQ_TICKER_CACHE = set()
    return ticker in _KOSDAQ_TICKER_CACHE


def _is_preferred_stock(ticker: str, name: str | None = None) -> bool:
    """우선주 판별.

    한국 우선주 코드 규칙:
      - 보통주 코드 끝자리 0/5 → 우선주는 보통주 코드 + 5 (예: 005930 → 005935)
      - 종목명에 '우' 접미사 또는 '(전환)'/'(우B)'/'(2우B)' 포함
    """
    tk = (ticker or "").strip()
    nm = (name or "").strip()
    # 6자리 숫자, 끝자리 5 + 종목명에 '우' 들어가면 우선주로 확정
    if len(tk) == 6 and tk.isdigit() and tk[-1] == "5":
        # 종목명에 '우' 들어가는지 (마지막 글자 또는 '우B'/'2우B')
        if nm.endswith("우") or "우B" in nm or "우(전환)" in nm:
            return True
        # 이름 정보가 없거나 단순 패턴이면 코드만으로는 부족 — false로 안전 처리
    # 이름만 보고 추가 판별 (코드가 0으로 끝나는 케이스 대비)
    if nm.endswith("우") or nm.endswith("우B") or "우B)" in nm:
        return True
    return False


def _top10_with_rank_delta(
    today_top: list[dict], prev_top: list[dict]
) -> list[dict]:
    """오늘 TOP 10 + 어제 대비 순위 변화.

    2026-05-13: 우선주(예: 005935 삼성전자우)는 동일 회사 보통주와 중복되므로 TOP 10에서 제외.
    """
    yesterday_rank: dict[str, int] = {}
    if prev_top:
        latest_date = prev_top[0]["date"] if prev_top else None
        for r in prev_top:
            if r["date"] == latest_date:
                yesterday_rank[r["ticker"]] = r["rank"]

    # 우선주 제외하고 TOP 10 추출
    filtered = [
        r for r in today_top
        if not _is_preferred_stock(r.get("ticker") or "", r.get("name"))
    ]

    out = []
    for r in filtered[:10]:
        tk = r["ticker"]
        cur = r["rank"]
        prev = yesterday_rank.get(tk)
        delta = prev - cur if prev is not None else None
        out.append({
            "rank":      cur,
            "ticker":    tk,
            "name":      r.get("name") or tk,
            "chg_pct":   r.get("chg_pct"),
            "trading_value": r.get("trading_value"),
            "market":    "KOSDAQ" if _is_kosdaq(tk) else "KOSPI",
            "prev_rank": prev,
            "delta":     delta,
        })
    return out


# ── Claude 호출 ──────────────────────────────────────────────

def _ask_claude(
    *,
    today: str,
    briefing: dict | None,
    picks_results: list[dict],
    avoids_results: list[dict],
    full_hit_rate: float,
    partial_hit_rate: float,
    accuracy_avoid: float,
    sector_strong: list[dict],
    sector_weak: list[dict],
    top10: list[dict],
    market_row: dict | None,
    kosdaq_row: dict | None,
    yesterday_rev: dict | None,
    learnings: list[dict],
) -> dict:
    # 입력 요약
    if briefing:
        applied = []
        try:
            applied = json.loads(briefing.get("lessons_applied") or "[]")
        except Exception:
            applied = []
        applied_block = f"오늘 적용 학습 ID: {applied}"
        brief_block = (
            f"market_regime={briefing.get('market_regime')}, "
            f"strategy_tone={briefing.get('strategy_tone')}, "
            f"headline={briefing.get('headline')}"
        )
    else:
        applied = []
        applied_block = "(아침 브리핑 없음)"
        brief_block = "(아침 브리핑 없음)"

    pr_lines = []
    _grade_label = {
        "full": "적중✅", "partial": "부분적중🟢",
        "neutral": "중립⚠️", "miss": "실패❌",
    }
    for r in picks_results:
        actual = r.get("actual_chg_pct")
        actual_s = f"{actual:+.2f}%" if actual is not None else "N/A"
        grade = r.get("grade")
        label = _grade_label.get(grade, "데이터없음")
        note = r.get("note") or ""
        pr_lines.append(
            f"  - {r['name']}({r['ticker']}) conf={r.get('predicted_conf')} "
            f"실제={actual_s} → {label} ({note})"
        )

    av_lines = []
    for r in avoids_results:
        actual = r.get("actual_chg_pct")
        actual_s = f"{actual:+.2f}%" if actual is not None else "N/A"
        hit_s = "적중" if r.get("hit") else ("실패" if r.get("hit") is False else "데이터없음")
        av_lines.append(f"  - {r['name']}({r['ticker']}) 실제={actual_s} → {hit_s}")

    sec_strong_block = "\n".join(
        f"  - {s['sector']} {s['avg_chg_pct']:+.2f}% ({s['stock_count']}종목)"
        for s in sector_strong
    ) or "  (없음)"
    sec_weak_block = "\n".join(
        f"  - {s['sector']} {s['avg_chg_pct']:+.2f}% ({s['stock_count']}종목)"
        for s in sector_weak
    ) or "  (없음)"

    top_lines = []
    for t in top10:
        delta = t.get("delta")
        delta_s = f"({delta:+d})" if delta is not None else "(NEW)"
        chg = t.get("chg_pct")
        chg_s = f"{chg:+.2f}%" if chg is not None else "N/A"
        top_lines.append(
            f"  {t['rank']}. {t['name']}({t['ticker']}) {chg_s} {delta_s}"
        )

    def _fmt(v, spec, default="-"):
        try:
            return format(float(v), spec)
        except (TypeError, ValueError):
            return default

    mkt_lines = []
    if market_row:
        mkt_lines.append(
            f"market_score={_fmt(market_row.get('market_score'), '.2f')} "
            f"dir={market_row.get('market_direction') or '-'}"
        )
        try:
            ms = json.loads(market_row.get("summary") or "{}")
            if isinstance(ms, dict) and ms.get("kospi") is not None:
                mkt_lines.append(
                    f"KOSPI {_fmt(ms.get('kospi'), '+.2f')}% "
                    f"KOSDAQ {_fmt(ms.get('kosdaq', 0), '+.2f')}%"
                )
        except Exception:
            pass
    if kosdaq_row:
        mkt_lines.append(
            f"KOSDAQ 종가={_fmt(kosdaq_row.get('close'), ',.2f')} | "
            f"외인={_fmt(kosdaq_row.get('foreign_net_buy'), '+.0f')}억 | "
            f"기관={_fmt(kosdaq_row.get('inst_net_buy'), '+.0f')}억"
        )
    mkt_block = "\n".join(f"  {x}" for x in mkt_lines) or "  (데이터 없음)"

    if yesterday_rev:
        yrev_block = (
            f"  date={yesterday_rev.get('date')} accuracy={yesterday_rev.get('accuracy_pct')}% "
            f"headline={yesterday_rev.get('headline')}"
        )
    else:
        yrev_block = "  (없음 — 첫 회고)"

    if learnings:
        lr_block = "\n".join(
            f"  #{l['id']} [{l['category']}, conf={l['confidence']:.2f}, "
            f"v={l['times_validated']}/f={l['times_failed']}] {l['content']}"
            for l in learnings
        )
    else:
        lr_block = "  (학습 없음)"

    user_content = f"""## 오늘 일자: {today}

## 아침 브리핑
  {brief_block}
  {applied_block}

## 추천 종목 결과 (적중 {full_hit_rate:.1f}% / 부분 적중 포함 {partial_hit_rate:.1f}%)
{chr(10).join(pr_lines) if pr_lines else '  (없음)'}

## 회피 종목 결과 (적중률 {accuracy_avoid:.1f}%)
{chr(10).join(av_lines) if av_lines else '  (없음)'}

## 강세 섹터 TOP 5
{sec_strong_block}

## 약세 섹터 TOP 5
{sec_weak_block}

## 거래대금 TOP 10 (어제 대비 순위 변화)
{chr(10).join(top_lines) if top_lines else '  (없음)'}

## 오늘 시장 종합
{mkt_block}

## 어제 회고 (참조)
{yrev_block}

## 현재 활성 학습 (검증/실패 판단 대상)
{lr_block}

시스템 프롬프트 규칙에 따라 STRICT JSON으로만 응답하세요."""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=2500,
            temperature=0,
            timeout=60.0,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_content}],
        )
        raw = response.content[0].text.strip()
        cleaned = _extract_json(raw)
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            return {}
        return parsed
    except Exception as e:
        logger.error(f"[evening_review] Claude 호출 실패: {type(e).__name__}: {e}")
        check_claude_error(e, "evening_review")
        return {}


# ── learnings 업데이트 ───────────────────────────────────────

def _update_learnings(
    today: str,
    new_lessons: list[dict],
    validated_ids: list[int],
    failed_ids: list[int],
) -> int:
    """new_lessons INSERT + 기존 ID validated/failed UPDATE.
    Returns: 새로 INSERT된 lesson 수.
    """
    inserted = 0
    # 1) 신규 INSERT — applicable_regime도 함께 저장 (옵션 Q Phase 2-D)
    for ls in new_lessons or []:
        cat = (ls.get("category") or "").strip()
        content = (ls.get("content") or "").strip()
        evidence = ls.get("evidence") or ""
        if not cat or not content:
            continue
        regime_list = ls.get("applicable_regime") or []
        regime_json: str | None = None
        if isinstance(regime_list, list) and regime_list:
            # 화이트리스트 검증
            allowed = {"strong", "sideways", "weak", "reversal", "volatile"}
            clean = [r for r in regime_list if isinstance(r, str) and r in allowed]
            if clean:
                regime_json = json.dumps(clean, ensure_ascii=False)
        try:
            execute(
                """
                INSERT INTO learnings (
                    discovered_at, category, content, evidence,
                    confidence, times_validated, times_failed, status,
                    applicable_regime
                ) VALUES (?, ?, ?, ?, 0.5, 0, 0, 'active', ?)
                """,
                (today, cat, content,
                 json.dumps([{"date": today, "observation": evidence}],
                            ensure_ascii=False),
                 regime_json),
            )
            inserted += 1
        except Exception as e:
            logger.warning(f"[evening_review] learnings INSERT 실패: {e}")

    # 2) validated UPDATE
    for lid in validated_ids or []:
        try:
            execute(
                """
                UPDATE learnings
                SET confidence = MIN(1.0, confidence + 0.1),
                    times_validated = times_validated + 1,
                    last_validated = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (today, int(lid)),
            )
        except Exception as e:
            logger.warning(f"[evening_review] learnings validated UPDATE 실패 [{lid}]: {e}")

    # 3) failed UPDATE — failed 5회 + win_rate<50% 시 deprecate
    for lid in failed_ids or []:
        try:
            execute(
                """
                UPDATE learnings
                SET times_failed = times_failed + 1,
                    confidence = MAX(0.0, confidence - 0.05),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (int(lid),),
            )
            # deprecation 조건 체크
            row = fetch_one(
                "SELECT times_validated, times_failed FROM learnings WHERE id = ?",
                (int(lid),),
            )
            if row:
                v = row["times_validated"] or 0
                f = row["times_failed"] or 0
                total = v + f
                if f >= 5 and total > 0 and (v / total) < 0.5:
                    execute(
                        "UPDATE learnings SET status = 'deprecated' WHERE id = ?",
                        (int(lid),),
                    )
                    logger.info(f"[evening_review] lesson #{lid} deprecated (v={v} f={f})")
        except Exception as e:
            logger.warning(f"[evening_review] learnings failed UPDATE 실패 [{lid}]: {e}")

    return inserted


# ── 메시지 작성 ──────────────────────────────────────────────

def _fmt_lessons_ids(
    ids: list[int],
    learnings_map: dict[int, dict],
    preview_len: int = 120,
) -> list[str]:
    """ID 리스트 → "#9 \"앞 120자 미리보기…\"" 형식 문자열 리스트.

    2026-05-13: 미리보기 38→120자 — 의미 파악 가능한 수준으로 확장.
    """
    out: list[str] = []
    for i in ids[:10]:
        try:
            iid = int(i)
        except (TypeError, ValueError):
            continue
        ent = learnings_map.get(iid)
        preview = ""
        if ent:
            content = (ent.get("content") or "").replace("\n", " ").strip()
            if content:
                preview = content[:preview_len] + ("…" if len(content) > preview_len else "")
        if preview:
            out.append(f"#{iid} \"{preview}\"")
        else:
            out.append(f"#{iid}")
    return out


_NEWS_CAT_META = [
    ("macro",   "🌐 거시/정책"),
    ("sector",  "🏭 섹터"),
    ("company", "🏢 개별 기업"),
    ("risk",    "⚠️ 리스크"),
]


def _format_news_section(news: dict | None) -> list[str]:
    """뉴스 섹션 텔레그램 라인 작성. 항목 없으면 빈 리스트 반환."""
    if not news:
        return []
    total = sum(len(news.get(k) or []) for k, _ in _NEWS_CAT_META)
    if total == 0:
        return []
    lines: list[str] = ["📰 <b>오늘 주요 뉴스</b>"]
    for cat, label in _NEWS_CAT_META:
        items = news.get(cat) or []
        if not items:
            continue
        lines.append(f"  {label}")
        for n in items:
            imp = int(n.get("importance") or 3)
            stars = "★" * imp
            headline = (n.get("headline") or "").replace("\n", " ").strip()
            if not headline:
                continue
            if len(headline) > 90:
                headline = headline[:87] + "…"
            lines.append(f"    {stars} {headline}")
    lines.append("")
    return lines


def _format_message(
    today: str,
    review: dict,
    picks_results: list[dict],
    avoids_results: list[dict],
    full_hit_rate: float,
    partial_hit_rate: float,
    accuracy_avoid: float,
    sector_strong: list[dict],
    sector_weak: list[dict],
    top10: list[dict],
    market_row: dict | None,
    kosdaq_row: dict | None,
    learnings: list[dict] | None = None,
    news: dict | None = None,
) -> str:
    lines: list[str] = []
    # 헤더에 요일 포함
    _wk = ['월', '화', '수', '목', '금', '토', '일']
    header_today = today
    try:
        if len(today) == 10 and today[4] == '-':
            from datetime import date as _date
            header_today = f"{today} ({_wk[_date.fromisoformat(today).weekday()]})"
    except Exception:
        pass
    lines.append(f"🌆 <b>저녁 회고 — {header_today}</b>")
    headline = review.get("headline") or ""
    if headline:
        lines.append(f"💬 <i>{headline}</i>")
    lines.append("")

    # 시장 요약
    def _fmt(v, spec, default="-"):
        try:
            return format(float(v), spec)
        except (TypeError, ValueError):
            return default

    if market_row or kosdaq_row:
        lines.append("📊 <b>오늘 시장</b>")
        if market_row:
            try:
                ms = json.loads(market_row.get("summary") or "{}")
                if isinstance(ms, dict) and ms.get("kospi") is not None:
                    lines.append(
                        f"  KOSPI <b>{_fmt(ms.get('kospi'), '+.2f')}%</b> | "
                        f"KOSDAQ <b>{_fmt(ms.get('kosdaq', 0), '+.2f')}%</b>"
                    )
            except Exception:
                pass
            md_raw = market_row.get('market_direction') or '-'
            md_kr = _MARKET_DIR_LABEL.get(md_raw, md_raw)
            lines.append(
                f"  방향: {md_kr} "
                f"(점수 {_fmt(market_row.get('market_score'), '.2f')})"
            )
        if kosdaq_row:
            def _fmt_flow(v):
                """수급 금액 표기 — KIS API 한계 반영.
                  - None  → "수집 실패 (KIS API 한계)"
                  - |v|<1 → "0억 (KIS API 한계 — 신뢰도 낮음)"
                  - 그 외 → "+1,234억"
                """
                if v is None:
                    return "❓ 수집 실패 (KIS API 한계)"
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    return "❓ 수집 실패"
                if abs(fv) < 1.0:
                    return f"{fv:+.1f}억 ⚠️ KIS API 한계 (신뢰도 낮음)"
                return f"{fv:+,.0f}억"

            f_s = _fmt_flow(kosdaq_row.get('foreign_net_buy'))
            i_s = _fmt_flow(kosdaq_row.get('inst_net_buy'))
            lines.append(f"  KOSDAQ 외인 {f_s}")
            lines.append(f"  KOSDAQ 기관 {i_s}")
        lines.append("")

    # 주요 뉴스 (4분류)
    news_lines = _format_news_section(news)
    if news_lines:
        lines.extend(news_lines)

    # 추천 결과 — 4단계 평가
    if picks_results:
        # 평가 카운트
        cnt_full = sum(1 for r in picks_results if r.get("grade") == "full")
        cnt_part = sum(1 for r in picks_results if r.get("grade") == "partial")
        cnt_neu  = sum(1 for r in picks_results if r.get("grade") == "neutral")
        cnt_miss = sum(1 for r in picks_results if r.get("grade") == "miss")
        cnt_total_eval = cnt_full + cnt_part + cnt_neu + cnt_miss
        lines.append(
            f"⭐ <b>추천 결과 (적중 {cnt_full}/{cnt_total_eval} · "
            f"부분 적중 포함 {cnt_full + cnt_part}/{cnt_total_eval})</b>"
        )
        grade_emoji = {
            "full": "✅", "partial": "🟢",
            "neutral": "⚠️", "miss": "❌",
        }
        for r in picks_results[:5]:
            actual = r.get("actual_chg_pct")
            actual_s = f"{actual:+.2f}%" if actual is not None else "N/A"
            grade = r.get("grade")
            emoji = grade_emoji.get(grade, "❓")
            note = r.get("note") or ""
            note_s = f" — {note}" if note else ""
            lines.append(
                f"  {emoji} {r['name']}({r['ticker']}) "
                f"conf={r.get('predicted_conf')}  실제 {actual_s}{note_s}"
            )
        lines.append("")

    # 회피 결과
    if avoids_results:
        lines.append(
            f"🚫 <b>회피 결과 (적중률 {accuracy_avoid:.1f}%)</b>"
        )
        for r in avoids_results[:5]:
            actual = r.get("actual_chg_pct")
            actual_s = f"{actual:+.2f}%" if actual is not None else "N/A"
            hit_emoji = "✅" if r.get("hit") else ("❌" if r.get("hit") is False else "❓")
            lines.append(f"  {hit_emoji} {r['name']}({r['ticker']}) 실제 {actual_s}")
        lines.append("")

    # 섹터 강·약
    if sector_strong:
        lines.append("🔥 <b>강세 섹터</b>")
        for s in sector_strong[:5]:
            lines.append(
                f"  ↑ {s['sector']} <b>{s['avg_chg_pct']:+.2f}%</b> ({s['stock_count']}종목)"
            )
        lines.append("")
    if sector_weak:
        lines.append("❄️ <b>약세 섹터</b>")
        for s in sector_weak[:5]:
            lines.append(
                f"  ↓ {s['sector']} <b>{s['avg_chg_pct']:+.2f}%</b> ({s['stock_count']}종목)"
            )
        lines.append("")

    # 거래대금 TOP 10
    if top10:
        lines.append("💰 <b>거래대금 TOP 10</b>")
        lines.append("  <i>(범례: 🆕 어제 미진입 / ▲n n계단↑ / ▼n n계단↓ / －변동없음)</i>")
        for i, t in enumerate(top10[:10], 1):
            delta = t.get("delta")
            if delta is None:
                delta_s = "🆕"
            elif delta > 0:
                delta_s = f"▲{delta}"
            elif delta < 0:
                delta_s = f"▼{-delta}"
            else:
                delta_s = "－"
            chg = t.get("chg_pct")
            chg_s = f"{chg:+.2f}%" if chg is not None else "N/A"
            mkt = t.get("market") or ""
            mkt_s = f" [{mkt}]" if mkt else ""
            # 표시 순번은 1..10 — DB rank가 듬성하더라도 사용자에겐 1부터 보임
            lines.append(
                f"  {i:2d}. {t['name']}({t['ticker']}){mkt_s} {chg_s} {delta_s}"
            )
        lines.append("")

    # 새 lessons
    new_lessons = review.get("new_lessons") or []
    if new_lessons:
        lines.append(f"📚 <b>새 교훈 ({len(new_lessons)})</b>")
        for ls in new_lessons[:5]:
            cat = ls.get("category") or "-"
            content = (ls.get("content") or "").replace("\n", " ")
            lines.append(f"  • [{cat}] {content[:140]}")
        lines.append("")

    # validated / failed — ID 옆에 content 미리보기
    v_ids = review.get("lessons_validated_ids") or []
    f_ids = review.get("lessons_failed_ids") or []
    if v_ids or f_ids:
        learnings_map: dict[int, dict] = {}
        for l in (learnings or []):
            try:
                learnings_map[int(l["id"])] = l
            except (KeyError, TypeError, ValueError):
                continue
        if v_ids:
            lines.append("✅ <b>검증된 교훈</b>")
            for s in _fmt_lessons_ids(v_ids, learnings_map):
                lines.append(f"  • {s}")
        if f_ids:
            lines.append("⚠️ <b>실패한 교훈</b>")
            for s in _fmt_lessons_ids(f_ids, learnings_map):
                lines.append(f"  • {s}")
        lines.append("")

    # 내일 전망
    outlook = review.get("tomorrow_outlook") or ""
    if outlook:
        lines.append("🔮 <b>내일 전망</b>")
        lines.append(f"  {outlook[:400]}")

    # 데이터 출처 범례 — 항상 끝에 보장 (truncate 시 본문을 자름)
    legend = (
        "\n\n📌 <i>출처: [KIS] 한국투자증권 시세·KOSPI 수급 / "
        "[KIS-한계] KOSDAQ 매매동향은 KIS 미지원·신뢰도 낮음 / "
        "[yfinance] 미국 지표 / [Claude] AI 분석(사실 기반·수치 추정 X)</i>"
    )

    # 한글 후처리 + 컷
    msg = "\n".join(lines)
    msg = _kr_postprocess(msg)
    body_limit = _TELEGRAM_LIMIT - len(legend) - 20
    if len(msg) > body_limit:
        msg = msg[:body_limit] + "\n...[중략]"
    msg += legend
    return msg


# ── DB 저장 ──────────────────────────────────────────────────

def _save_review(
    today: str,
    review: dict,
    picks_results: list[dict],
    avoids_results: list[dict],
    full_hit_rate: float,
    partial_hit_rate: float,
    accuracy_avoid: float,
    sector_strong: list[dict],
    sector_weak: list[dict],
    top10: list[dict],
    market_row: dict | None,
    kosdaq_row: dict | None,
    full_message: str,
    sent: bool,
) -> None:
    # FK 보호: evening_review.date는 morning_briefing.date를 참조한다.
    # 아침 브리핑이 없는 날(휴장 직후 / 첫 운영일)에는 placeholder row 선삽입.
    try:
        mb_exists = fetch_one(
            "SELECT 1 FROM morning_briefing WHERE date = ?", (today,)
        )
        if not mb_exists:
            execute(
                """
                INSERT OR IGNORE INTO morning_briefing (date, headline, full_message)
                VALUES (?, ?, ?)
                """,
                (today, "(아침 브리핑 미실행 — 회고만 진행)", ""),
            )
    except Exception as e:
        logger.debug(f"[evening_review] morning_briefing placeholder 삽입 스킵: {e}")

    try:
        execute(
            """
            INSERT OR REPLACE INTO evening_review (
                date, market_summary, sectors_strong, sectors_weak, top10_volume,
                picks_result, avoids_result, accuracy_pct, accuracy_avoid_pct,
                new_lessons, lessons_validated, lessons_failed,
                tomorrow_outlook, headline, full_message, sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                today,
                json.dumps(
                    {"market": market_row, "kosdaq": kosdaq_row},
                    ensure_ascii=False, default=str,
                ),
                json.dumps(sector_strong, ensure_ascii=False),
                json.dumps(sector_weak, ensure_ascii=False),
                json.dumps(top10, ensure_ascii=False),
                json.dumps(picks_results, ensure_ascii=False),
                json.dumps(avoids_results, ensure_ascii=False),
                full_hit_rate,
                accuracy_avoid,
                json.dumps(review.get("new_lessons") or [], ensure_ascii=False),
                json.dumps(review.get("lessons_validated_ids") or [], ensure_ascii=False),
                json.dumps(review.get("lessons_failed_ids") or [], ensure_ascii=False),
                review.get("tomorrow_outlook", ""),
                review.get("headline", ""),
                full_message,
                datetime.now().isoformat(timespec="seconds") if sent else None,
            ),
        )
    except Exception as e:
        logger.error(f"[evening_review] evening_review 저장 오류: {e}", exc_info=True)


# ── 메타 학습 (옵션 Q Phase 2-B) ────────────────────────────

def _meta_learn(today: str) -> dict:
    """최근 7일 evening_review 결과로 카테고리별 적중률 산출 + learnings 신뢰도 자동 조정.

    동작:
      1. 최근 7일 evening_review.picks_result 합산 — hit/miss 카운트
      2. 각 카테고리(learning.category)별 적중률 추정:
         - 동기 매핑은 어렵기에, 학습 자체의 times_validated/times_failed 누계 사용
      3. 카테고리 평균 적중률 ≥70% → confidence boost +0.05 / <50% → -0.05
      4. 결과를 learnings.evidence(JSON) 끝에 누적

    Returns:
      {
        "category_accuracy": {cat: ratio, ...},
        "boosted_ids":  [int, ...],
        "demoted_ids":  [int, ...],
        "days_in_window": int
      }
    """
    # 최근 7일 회고 데이터 수집
    rows = fetch_all(
        """
        SELECT date, accuracy_pct, picks_result
        FROM evening_review
        WHERE date >= date(?, '-7 days') AND date <= ?
        ORDER BY date DESC
        """,
        (today, today),
    )
    if not rows:
        return {
            "category_accuracy": {},
            "boosted_ids": [],
            "demoted_ids": [],
            "days_in_window": 0,
        }

    # 카테고리별 누적 — learnings 자체 통계 사용
    cat_rows = fetch_all(
        """
        SELECT category, id, times_validated, times_failed, confidence, status
        FROM learnings
        WHERE status = 'active'
        """
    )
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for r in cat_rows:
        d = dict(r)
        by_cat[d["category"]].append(d)

    cat_acc: dict[str, float] = {}
    boosted: list[int] = []
    demoted: list[int] = []

    for cat, items in by_cat.items():
        total_v = sum(int(i.get("times_validated") or 0) for i in items)
        total_f = sum(int(i.get("times_failed") or 0) for i in items)
        total = total_v + total_f
        if total < 3:   # 표본 부족
            continue
        ratio = total_v / total
        cat_acc[cat] = round(ratio, 3)

        # boost / demote
        if ratio >= 0.70:
            # 가장 confidence 낮은 항목부터 boost (위로 끌어올림 의미)
            target = sorted(items, key=lambda x: float(x.get("confidence") or 0))[:1]
            for t in target:
                try:
                    execute(
                        """
                        UPDATE learnings
                        SET confidence = MIN(1.0, confidence + 0.05),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (int(t["id"]),),
                    )
                    boosted.append(int(t["id"]))
                except Exception as e:
                    logger.warning(f"[meta_learn] boost 실패 #{t['id']}: {e}")
        elif ratio < 0.50:
            # 가장 confidence 높은 항목 demote (책임 큰 항목부터 신뢰도 차감)
            target = sorted(items, key=lambda x: -float(x.get("confidence") or 0))[:1]
            for t in target:
                try:
                    execute(
                        """
                        UPDATE learnings
                        SET confidence = MAX(0.0, confidence - 0.05),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (int(t["id"]),),
                    )
                    demoted.append(int(t["id"]))
                except Exception as e:
                    logger.warning(f"[meta_learn] demote 실패 #{t['id']}: {e}")

    # evidence에 누적 기록
    meta_entry = {
        "date": today,
        "type": "meta_learn",
        "window_days": len(rows),
        "category_accuracy": cat_acc,
        "boosted": boosted,
        "demoted": demoted,
    }
    for lid in boosted + demoted:
        try:
            row = fetch_one("SELECT evidence FROM learnings WHERE id = ?", (int(lid),))
            ev_old = []
            if row and row["evidence"]:
                try:
                    ev_old = json.loads(row["evidence"]) or []
                except Exception:
                    ev_old = []
            ev_old.append(meta_entry)
            execute(
                "UPDATE learnings SET evidence = ? WHERE id = ?",
                (json.dumps(ev_old, ensure_ascii=False), int(lid)),
            )
        except Exception as e:
            logger.warning(f"[meta_learn] evidence 갱신 실패 #{lid}: {e}")

    logger.info(
        f"[meta_learn] window={len(rows)}일 — "
        f"카테고리 적중={cat_acc} / boost={len(boosted)} / demote={len(demoted)}"
    )
    return {
        "category_accuracy": cat_acc,
        "boosted_ids": boosted,
        "demoted_ids": demoted,
        "days_in_window": len(rows),
    }


# ── 메인 진입점 ───────────────────────────────────────────────

def run_evening_review() -> dict:
    """오늘 결과 회고 + 학습 도출. 텔레그램 발송 + DB 저장.

    Returns:
        {"date": str, "accuracy": float, "new_lessons": int, "sent": bool}
    """
    today = date.today().isoformat()
    if not is_trading_day(datetime.now()):
        logger.info(f"[evening_review] {today} 휴장일 — 스킵")
        return {"date": today, "accuracy": 0.0, "new_lessons": 0, "sent": False}

    logger.info(f"[evening_review] 시작 — {today}")

    # 뉴스 수집/분류/저장 (오늘 한국 장중 + 오늘 아침 미국, 최근 18시간)
    news_for_msg: dict | None = None
    if _collect_and_save_news and _get_news_for_brief:
        try:
            _collect_and_save_news(hours=18)
        except Exception as e:
            logger.warning(f"[evening_review] 뉴스 수집 실패 — 회고는 계속: {e}")
        try:
            news_for_msg = _get_news_for_brief(today)
        except Exception as e:
            logger.warning(f"[evening_review] 뉴스 조회 실패: {e}")
            news_for_msg = None

    # 데이터 수집
    briefing       = _fetch_today_briefing(today)
    today_top      = _fetch_today_top_value(today)
    prev_top       = _fetch_yesterday_top_value(today)
    kosdaq_row     = _fetch_today_kosdaq()
    market_row     = _fetch_today_market()
    learnings      = _fetch_active_learnings(limit=30)
    yesterday_rev  = _fetch_yesterday_review()

    # 평가
    picks: list[dict] = []
    avoids: list[dict] = []
    if briefing:
        try:
            picks = json.loads(briefing.get("picks") or "[]") or []
        except Exception:
            picks = []
        try:
            avoids = json.loads(briefing.get("avoids") or "[]") or []
        except Exception:
            avoids = []

    picks_results, full_hit_rate, partial_hit_rate = _evaluate_picks(picks, today_top)
    avoids_results, accuracy_avoid = _evaluate_avoids(avoids, today_top)

    sector_strong, sector_weak = _sector_strength(today_top)
    top10 = _top10_with_rank_delta(today_top, prev_top)

    # Claude 분석
    review = _ask_claude(
        today=today,
        briefing=briefing,
        picks_results=picks_results,
        avoids_results=avoids_results,
        full_hit_rate=full_hit_rate,
        partial_hit_rate=partial_hit_rate,
        accuracy_avoid=accuracy_avoid,
        sector_strong=sector_strong,
        sector_weak=sector_weak,
        top10=top10,
        market_row=market_row,
        kosdaq_row=kosdaq_row,
        yesterday_rev=yesterday_rev,
        learnings=learnings,
    )

    # learnings 업데이트
    new_lesson_count = 0
    if review:
        new_lesson_count = _update_learnings(
            today,
            review.get("new_lessons") or [],
            review.get("lessons_validated_ids") or [],
            review.get("lessons_failed_ids") or [],
        )

    # 메시지 작성 + 발송
    if not review:
        review = {
            "new_lessons": [],
            "lessons_validated_ids": [],
            "lessons_failed_ids": [],
            "tomorrow_outlook": "(Claude 분석 실패)",
            "headline": "회고 부분 실패 — 결과 데이터는 정상 저장",
        }

    msg = _format_message(
        today, review,
        picks_results, avoids_results,
        full_hit_rate, partial_hit_rate, accuracy_avoid,
        sector_strong, sector_weak, top10,
        market_row, kosdaq_row,
        learnings=learnings,
        news=news_for_msg,
    )

    sent = notify(msg)
    if not sent:
        logger.warning("[evening_review] 텔레그램 발송 실패 — DB 저장은 진행")

    _save_review(
        today, review,
        picks_results, avoids_results,
        full_hit_rate, partial_hit_rate, accuracy_avoid,
        sector_strong, sector_weak, top10,
        market_row, kosdaq_row,
        msg, sent,
    )

    # 메타 학습 (옵션 Q Phase 2-B) — 회고 저장 후 실행
    meta = {}
    try:
        meta = _meta_learn(today)
    except Exception as e:
        logger.warning(f"[evening_review] meta_learn 실패: {e}")

    logger.info(
        f"[evening_review] 완료 — full_hit={full_hit_rate:.1f}% "
        f"partial+={partial_hit_rate:.1f}% avoid_hit={accuracy_avoid:.1f}% "
        f"new_lessons={new_lesson_count} sent={sent} "
        f"meta_boost={len(meta.get('boosted_ids') or [])}"
        f"/demote={len(meta.get('demoted_ids') or [])}"
    )
    return {
        "date": today,
        "accuracy": full_hit_rate,
        "partial_accuracy": partial_hit_rate,
        "new_lessons": new_lesson_count,
        "sent": sent,
        "meta_learn": meta,
    }


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    r = run_evening_review()
    print(r)
