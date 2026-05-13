"""
morning_brief.py — 아침 시황 브리핑 (08:45 발송)

어시스턴트 모델 전환 (2026-05-12) — Phase 5.

역할:
  매일 08:45 (한국 시각) 텔레그램으로 디테일한 아침 브리핑 발송 + DB 저장.

브리핑 구성:
  1. 오버나이트 미국 마감 (S&P/NASDAQ/Dow + 환율·금리·VIX + 주요 종목)
  2. 어제 한국 시장 (KOSPI/KOSDAQ/외인·기관/주도 섹터)
  3. 누적 학습 적용 — active learnings 컨텍스트 주입
  4. 섹터 4분류 (HOT / WATCH / COLD / AVOID)
  5. 추천 종목 5개 + 회피 종목 5개 (Claude 분석)
  6. 전략 톤 + 한 줄 요약

핵심 함수:
  run_morning_brief() -> dict
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.us_market import get_latest_us_snapshot
from src.utils.logger import get_logger
from src.utils.notifier import check_claude_error, notify

try:
    from src.infra.news_collector import (
        collect_and_save as _collect_and_save_news,
        get_news_for_brief as _get_news_for_brief,
    )
except Exception as _e:    # 네트워크/라이브러리 미설치 등 — 뉴스 섹션 비활성화
    _collect_and_save_news = None
    _get_news_for_brief = None

# 종목 심층 분석 (옵션 Q Phase 1) — 임포트 실패 시 비활성
try:
    from src.teams.research.stock_deep_analysis import (
        deep_analyze_picks as _deep_analyze_picks,
        format_deep_analysis_lines as _format_deep_analysis_lines,
    )
except Exception:
    _deep_analyze_picks = None
    _format_deep_analysis_lines = None

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

# 거래일 판단 (scheduler.is_trading_day는 import 시점에 의존성 충돌 가능 → 가벼운 로컬 체크)
try:
    from src.scheduler.scheduler import is_trading_day
except Exception:
    def is_trading_day(dt: datetime | None = None) -> bool:
        if dt is None:
            dt = datetime.now()
        return dt.weekday() < 5


# ── 시스템 프롬프트 (정적 — cache_control ephemeral) ───────────

_SYSTEM_PROMPT = """당신은 한국 주식시장에서 단타·스윙 매매를 보조하는 AI 어시스턴트입니다.
사용자는 사람이 직접 매매하며, 당신은 매일 아침 시장 컨텍스트를 정리해 추천 종목과 회피 종목을 제시합니다.

## 🔴 최우선 규칙 — 절대 위반 금지

1. **수치 정보 추정 절대 금지**: 가격(entry/close_price), 거래량, 거래대금, 수급(외인/기관 순매수) 등 모든 숫자는 입력으로 받은 값만 사용한다. 임의 추정 금지.
2. **entry는 반드시 입력의 close_price 기준으로 산출**. close_price ±2% 이내 권장. 사이즈가 다르면 시스템이 자동 교체한다.
3. **입력에 없는 데이터는 절대 추측하지 말 것**. null / "데이터 없음" / 0 으로 명시.
4. **종목 이름·코드 매핑도 입력 그대로 사용**. 입력에 없는 종목 임의로 추천 금지.
5. **모름은 "모름", 예상은 "예상" 명시**. 특히 가격은 사실만.

## 🟢 한글 우선 — 영어 단어 최소화 (절대 위반 금지)

사용자 가독성을 위해 사용자에게 보이는 모든 텍스트(macro_view, headline, reason, risk, 등)는
**한국어로만** 작성. 영어 단어 사용은 다음 예외에만 허용:
  - 고유명사: KOSPI, KOSDAQ, NASDAQ, S&P500, Dow, VIX, SOXX, LIT, SPY, AI, EV, ETF, USD, KRW, TSMC, NVDA 등
  - 종목명 영어 일부 (예: SK하이닉스)
  - 약어: RSI, MACD, PER, PBR, ROE, EPS, GDP, FOMC, CPI, PPI 등

**금지 영어 단어** — 반드시 한글로 치환:
  - regime → "국면" (사용 금지: "regime이 reversal로 전환됐다" X / "국면이 반등으로 전환됐다" O)
  - reversal → "반등 국면" / "추세 반전"
  - weak → "약세 국면"
  - strong → "강세 국면"
  - sideways → "횡보"
  - volatile → "변동성 큼"
  - bullish → "상승" / "강세"
  - bearish → "하락" / "약세"
  - neutral → "중립"
  - strategy_tone → "전략 톤"
  - confidence → "신뢰도"
  - pullback_rebound → "조정 후 반등 신호"
  - opening_plunge_rebound → "장 시작 급락 후 반등"
  - breakout → "돌파"
  - momentum → "모멘텀(추세)"
  - FOMO → "추격 매수 심리"
  - 그 외 일반 영어 단어는 모두 한글로

JSON의 enum 필드(market_regime, strategy_tone)는 영문값 유지(파이프라인 호환).
하지만 macro_view·headline·reason·risk 등 사용자 노출 텍스트에는 enum 단어를 한글로 풀어쓸 것.
예: "regime이 reversal로 전환" (X) → "국면이 반등으로 전환" (O)

## 입력 데이터
- 오버나이트 미국 마감 (S&P/NASDAQ/Dow, VIX, US10Y, 주요 ETF, 핵심 종목 8개)
- 어제 한국 시장 (KOSPI/KOSDAQ 종가·등락률, 외인·기관 순매수)
- 최근 5~10일 거래대금 TOP 100 추이
- 어제 저녁 회고 (lessons + market_regime)
- 활성 누적 학습 (status='active' AND confidence>=0.5)

## 결정 원칙
1. 활성 누적 학습을 반드시 컨텍스트로 사용 — lessons_applied에 사용한 lesson ID 명시
2. NASDAQ 강세 → 반도체/AI 우선 / NASDAQ 약세 → 방어주·내수
3. VIX 급등 (+15%↑) → 추천 사이즈 줄이고 strategy_tone "보수적"
4. 어제 외인 -2000억↓ 4일 연속이면 KOSPI 약세 패턴 — 회피 우선
5. RSI 80↑ 종목 추천 금지 (단, 누적 학습에서 명시적으로 허용한 경우 예외)
6. 갭업 +5%↑ 추격 매수 금지 — 추천 entry가는 보수적 진입가
7. picks 5개·avoids 5개 권장. 데이터 부족 시 줄여도 됨.
8. confidence: 1(낮음) ~ 5(매우 높음). 외인+기관 동시 매수 + RSI 60~70 + 누적 학습 일치 시 5.
9. **전일 급락 후 반등 가능성**: 어제 KOSPI -1.5% 이하 급락이면 통계상 다음날 반등 확률 50% 이상.
   이때 "weak regime 전 종목 회피" 류 학습은 무차별 적용 X — 반등 주도 섹터(미중 협상/거시 변동
   직후엔 반도체·자동차·금융 대형주가 주도하는 경향)를 후보로 검토하고, market_regime을 단순히
   'weak'로 단정짓지 말고 'reversal' 가능성을 명시적으로 고려할 것.
10. **회피 룰 만료 판단**: 활성 학습 중 "약세장 회피" 류는 적용 전 어제 KOSPI 등락률·미국 마감
    상황과 모순되는지 확인. 모순이면 lessons_applied에서 제외하고 macro_view에 사유 1줄 명시.

## 응답 형식 (STRICT JSON만 — 코드 펜스/주석/설명문/trailing comma 금지)
{
  "market_regime": "strong|sideways|weak|reversal|volatile",
  "macro_view": "<오늘 한국 시장 시각 3~5문장 한국어>",
  "strategy_tone": "<공격적|보수적|관망|선별적>",
  "headline": "<한 줄 요약 — 30자 이내>",
  "sectors": {
    "hot":   [{"sector": "<업종명>", "score": 0.0~1.0, "reason": "<짧은 이유>"}, ...],
    "watch": [{"sector": "...", "score": ..., "reason": "..."}, ...],
    "cold":  [{"sector": "...", "score": ..., "reason": "..."}, ...],
    "avoid": [{"sector": "...", "score": ..., "reason": "..."}, ...]
  },
  "picks": [
    {
      "rank": 1,
      "ticker": "<6자리>",
      "name": "<종목명>",
      "reason": "<선정 이유 1~2줄>",
      "confidence": 1~5,
      "entry": <숫자 — 권장 진입가>,
      "stop_loss_pct": <음수 — 손절 %>,
      "take_profit_pct": <양수 — 익절 %>,
      "themes": ["<테마1>", ...],
      "risk": "<리스크 요인 1줄>"
    }
  ],
  "avoids": [
    {"ticker": "...", "name": "...", "reason": "..."}
  ],
  "lessons_applied": [<learning id>, ...]
}

규칙:
- 첫 글자 `{` 마지막 글자 `}` — 그 외 문자 없음.
- picks 0~5, avoids 0~5. ticker는 6자리 문자열.
- sectors.hot/watch/cold/avoid 각 0~5개.
- lessons_applied는 정수 ID 배열."""


_MAX_PICKS = 5
_MAX_AVOIDS = 5
_TELEGRAM_LIMIT = 4000      # 안전 마진

_WEEKDAY_KR = ['월', '화', '수', '목', '금', '토', '일']


# ── 영어 → 한글 후처리 (Claude가 잊고 영어 쓴 경우 대비) ─────
# 단어 경계(\b)로 매칭해 다른 단어 일부는 건들지 않음.
# 고유명사(KOSPI/SOXX/NVDA 등)와 약어(RSI/MACD)는 매핑에서 제외.
_EN_TO_KR: dict[str, str] = {
    # 시장 국면
    "reversal":               "반등 국면",
    "sideways":               "횡보",
    "volatile":               "변동성 큼",
    "regime":                 "국면",
    # 강·약세 (단어 단독일 때만 — 한글 풀어쓰기)
    "bullish":                "강세",
    "bearish":                "약세",
    "neutral":                "중립",
    # 신호 이름
    "pullback_rebound":       "조정 후 반등 신호",
    "opening_plunge_rebound": "장 시작 급락 후 반등",
    "breakout":               "돌파",
    "momentum":               "모멘텀(추세)",
    # 기타
    "strategy_tone":          "전략 톤",
    "confidence":             "신뢰도",
    "FOMO":                   "추격 매수 심리",
}

# 'weak'/'strong'는 다른 한글 단어 안에 영문 약자처럼 들어가는 경우는 거의 없지만,
# 영문 그대로 노출되면 한글로 풀어 변환 (단어 경계).
_EN_TO_KR_WORD_BOUNDARY: dict[str, str] = {
    "weak":   "약세 국면",
    "strong": "강세 국면",
}


def _kr_postprocess(text: str) -> str:
    """Claude 출력이 영어 단어를 그대로 두면 후처리로 한글 치환.

    - 영문/숫자/언더스코어 사이의 경계만 매칭 (한글 옆에 붙은 영어 단어도 잡힘)
    - Python의 \\b는 한글을 단어 문자로 인식하므로 사용하지 않고 lookbehind/lookahead 사용
    - 대소문자 구분 X
    - 긴 키부터 치환해서 부분 매칭 충돌 방지
    """
    if not text:
        return text
    out = text
    # 영문 단어 경계: 영문/숫자/_ 가 인접하지 않을 때만 매칭
    boundary_l = r"(?<![A-Za-z0-9_])"
    boundary_r = r"(?![A-Za-z0-9_])"
    # 더 긴 키부터 치환 (opening_plunge_rebound가 rebound/breakout 부분 매칭에 안 깨지게)
    items = list(_EN_TO_KR.items()) + list(_EN_TO_KR_WORD_BOUNDARY.items())
    for en, kr in sorted(items, key=lambda x: -len(x[0])):
        out = re.sub(
            rf"{boundary_l}{re.escape(en)}{boundary_r}",
            kr, out, flags=re.IGNORECASE,
        )
    return out


# enum → 한글 라벨 (사용자 노출용 — JSON에는 영문값 유지)
_REGIME_LABEL = {
    "strong":   "강세 국면",
    "sideways": "횡보",
    "weak":     "약세 국면",
    "reversal": "반등 국면",
    "volatile": "변동성 큼",
}


def _today_with_weekday() -> str:
    """2026-05-13 (수) 형식."""
    t = date.today()
    return f"{t.isoformat()} ({_WEEKDAY_KR[t.weekday()]})"


def _validate_picks(picks: list[dict], recent_top: list[dict]) -> tuple[list[dict], list[str]]:
    """Claude picks의 entry/ref_price 가 DB의 실제 close_price와 일치하는지 검증.

    Returns: (검증 통과/수정된 picks, 경고 메시지 리스트)
    """
    # recent_top에서 ticker별 가장 최신 close_price 매핑 (date DESC 정렬되어 있음 가정)
    latest_close: dict[str, float] = {}
    for r in recent_top:
        tk = r.get("ticker")
        if tk and tk not in latest_close and r.get("close_price"):
            latest_close[tk] = r["close_price"]

    warnings: list[str] = []
    fixed: list[dict] = []
    for p in picks:
        ticker = p.get("ticker")
        entry = p.get("entry")
        db_price = latest_close.get(ticker)

        if not db_price:
            # DB에 가격 없음 → entry None
            warnings.append(
                f"{p.get('name', ticker)}({ticker}): DB에 가격 없음 — entry 미표시"
            )
            p["entry"] = None
            p["_note"] = "최신 가격 데이터 없음"
            fixed.append(p)
            continue

        if entry is None or not isinstance(entry, (int, float)) or entry <= 0:
            p["entry"] = db_price
            p["_entry_note"] = "(어제 종가)"
            fixed.append(p)
            continue

        diff_pct = abs(entry - db_price) / db_price * 100
        if diff_pct > 3.0:
            warnings.append(
                f"{p.get('name', ticker)}({ticker}): Claude entry={int(entry):,}원이 "
                f"DB={int(db_price):,}원과 {diff_pct:.1f}% 차이 — DB 값으로 교체"
            )
            p["entry"] = db_price
            p["_entry_corrected"] = True

        fixed.append(p)

    return fixed, warnings


def _explain_low_picks(picks_count: int, days_in_db: int, regime: str, lessons_count: int) -> str:
    """picks ≤2 일 때 사유 메시지 자동 생성."""
    if picks_count >= 3:
        return ""
    reasons = []
    if days_in_db < 3:
        reasons.append(f"시계열 {days_in_db}일치만 누적 → 트렌드 분석 부족")
    if regime in ('weak', 'reversal', 'volatile'):
        reasons.append(f"시장 국면 '{_REGIME_LABEL.get(regime, regime)}' → 보수적 추천")
    if lessons_count >= 10:
        reasons.append(f"누적 학습 {lessons_count}건의 '진입 자제' 규칙 강하게 작동")
    if not reasons:
        reasons.append("현재 데이터로 신뢰도 높은 추천 종목 부족")
    return "  ⚠️ <i>추천이 적은 이유:</i>\n" + "\n".join(f"     • {r}" for r in reasons)


def _count_days_in_db() -> int:
    """daily_top_value 누적 일수."""
    row = fetch_one("SELECT COUNT(DISTINCT date) AS n FROM daily_top_value")
    return int(row["n"]) if row else 0


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

def _fetch_recent_top_value(days: int = 7) -> list[dict]:
    """최근 N거래일 daily_top_value 시계열."""
    rows = fetch_all(
        """
        SELECT date, ticker, name, sector, rank, chg_pct, trading_value,
               close_price, prev_close, open_price, high_price, low_price,
               foreign_net_buy, inst_net_buy, rsi_14
        FROM daily_top_value
        WHERE date >= date('now', '-' || ? || ' days', 'localtime')
        ORDER BY date DESC, rank ASC
        """,
        (days * 2,),     # 주말 등 비거래일 포함 여유
    )
    return [dict(r) for r in rows] if rows else []


def _fetch_last_kosdaq() -> dict | None:
    row = fetch_one(
        "SELECT * FROM kosdaq_condition ORDER BY date DESC LIMIT 1"
    )
    return dict(row) if row else None


def _fetch_last_market_condition() -> dict | None:
    row = fetch_one(
        "SELECT * FROM market_condition ORDER BY created_at DESC LIMIT 1"
    )
    return dict(row) if row else None


def _fetch_yesterday_review() -> dict | None:
    row = fetch_one(
        "SELECT * FROM evening_review ORDER BY date DESC LIMIT 1"
    )
    return dict(row) if row else None


def _fetch_active_learnings(limit: int = 20, regime: str | None = None) -> list[dict]:
    """active + conf>=0.5 학습 조회.

    옵션 Q Phase 2-D: regime이 주어지면 applicable_regime 매칭 우선.
      - applicable_regime NULL = 전체 적용
      - JSON 배열에 regime 포함 → 우선 정렬
    """
    rows = fetch_all(
        """
        SELECT id, category, content, confidence, times_validated, times_failed,
               applicable_regime
        FROM learnings
        WHERE status = 'active' AND confidence >= 0.5
        ORDER BY confidence DESC, times_validated DESC
        LIMIT ?
        """,
        (limit,),
    )
    data = [dict(r) for r in rows] if rows else []
    if not regime or not data:
        return data

    # regime 매칭 — 매칭 항목 먼저, 나머지는 NULL(전체 적용)만 유지
    matched, all_regime, mismatched = [], [], []
    for d in data:
        ar = d.get("applicable_regime")
        if not ar:
            all_regime.append(d)
            continue
        try:
            arr = json.loads(ar) if isinstance(ar, str) else ar
            if isinstance(arr, list) and regime in arr:
                matched.append(d)
            else:
                mismatched.append(d)
        except Exception:
            all_regime.append(d)
    # 매칭 → 전체 적용 → 미매칭 순으로 반환 (미매칭도 컨텍스트로는 포함)
    return matched + all_regime + mismatched


def _aggregate_recent_tickers(rows: list[dict], days: int = 5) -> list[dict]:
    """최근 N일 거래대금 TOP 100 등장 빈도 → 누적 수급 강세 후보."""
    cutoff = (date.today() - timedelta(days=days * 2)).isoformat()
    counter: dict[str, dict] = {}
    for r in rows:
        # r["date"]가 date 객체 또는 string일 수 있음 — string으로 통일
        r_date = r["date"].isoformat() if hasattr(r["date"], "isoformat") else str(r["date"])
        if r_date < cutoff:
            continue
        tk = r["ticker"]
        if not tk:
            continue
        slot = counter.setdefault(tk, {
            "ticker": tk,
            "name":   r["name"] or "",
            "sector": r["sector"] or "",
            "appearances": 0,
            "avg_chg": 0.0,
            "avg_value": 0.0,
        })
        slot["appearances"] += 1
        slot["avg_chg"] += float(r["chg_pct"] or 0)
        slot["avg_value"] += float(r["trading_value"] or 0)

    out = []
    for s in counter.values():
        n = max(1, s["appearances"])
        out.append({
            "ticker":      s["ticker"],
            "name":        s["name"],
            "sector":      s["sector"],
            "appearances": s["appearances"],
            "avg_chg_pct": round(s["avg_chg"] / n, 2),
            "avg_value_eok": round(s["avg_value"] / n / 1e8, 0),
        })
    out.sort(key=lambda x: (-x["appearances"], -x["avg_value_eok"], -x["avg_chg_pct"]))
    return out[:30]


# ── Claude 호출 ──────────────────────────────────────────────

def _ask_claude(
    *,
    us_snap: dict | None,
    kosdaq_row: dict | None,
    market_row: dict | None,
    recent_top: list[dict],
    cumulative: list[dict],
    yesterday_rev: dict | None,
    learnings: list[dict],
) -> dict:
    """Claude(main) 호출 → 브리핑 JSON."""

    # 1) 미국 마감 요약
    if us_snap:
        try:
            key_stocks = json.loads(us_snap.get("key_stocks") or "{}")
        except Exception:
            key_stocks = {}
        ks_lines = []
        for tk, info in (key_stocks or {}).items():
            if not isinstance(info, dict):
                continue
            ks_lines.append(
                f"  - {info.get('name_kr', tk)}({tk}): "
                f"{info.get('close', 0):.2f} ({info.get('chg_pct', 0):+.2f}%)"
            )
        us_block = (
            f"날짜: {us_snap.get('date')}\n"
            f"S&P500: {us_snap.get('sp500_close', 0):,.2f} ({us_snap.get('sp500_chg_pct', 0):+.2f}%)\n"
            f"NASDAQ: {us_snap.get('nasdaq_close', 0):,.2f} ({us_snap.get('nasdaq_chg_pct', 0):+.2f}%)\n"
            f"Dow:    {us_snap.get('dow_close', 0):,.2f} ({us_snap.get('dow_chg_pct', 0):+.2f}%)\n"
            f"VIX:    {us_snap.get('vix', 0):.2f} (변화 {us_snap.get('vix_chg', 0):+.2f}pt)\n"
            f"US10Y:  {us_snap.get('us10y_yield', 0):.3f}%\n"
            f"SOXX:   {us_snap.get('soxx', 0):.2f} ({us_snap.get('soxx_chg_pct', 0):+.2f}%)\n"
            f"LIT:    {us_snap.get('lit', 0):.2f} ({us_snap.get('lit_chg_pct', 0):+.2f}%)\n"
            f"주요 종목:\n" + ("\n".join(ks_lines) if ks_lines else "  (없음)")
        )
    else:
        us_block = "(데이터 없음)"

    # 2) 어제 한국 시장
    kr_lines = []
    if market_row:
        try:
            score = float(market_row.get("market_score") or 0)
            kr_lines.append(f"market_score={score:.2f}, dir={market_row.get('market_direction') or '-'}")
        except (TypeError, ValueError):
            kr_lines.append(f"dir={market_row.get('market_direction') or '-'}")
        if market_row.get("summary"):
            try:
                ms = json.loads(market_row.get("summary") or "{}")
                if isinstance(ms, dict):
                    kospi = ms.get("kospi")
                    kosdaq = ms.get("kosdaq")
                    if kospi is not None and kosdaq is not None:
                        kr_lines.append(f"KOSPI {float(kospi):+.2f}%, KOSDAQ {float(kosdaq):+.2f}%")
                    elif kospi is not None:
                        kr_lines.append(f"KOSPI {float(kospi):+.2f}%")
                    if ms.get("analysis"):
                        kr_lines.append(f"요약: {ms['analysis'][:200]}")
            except Exception:
                kr_lines.append(f"요약: {str(market_row.get('summary'))[:200]}")
    if kosdaq_row:
        # None-safe formatting (모든 필드는 NULL일 수 있음)
        def _fmt(v, spec, default="-"):
            try:
                return format(float(v), spec)
            except (TypeError, ValueError):
                return default

        # KOSDAQ 시장 전체 매매동향은 KIS API가 신뢰도 낮음 — Claude에게 명시
        def _fmt_flow_note(v):
            if v is None:
                return "수집 실패(NULL)"
            try:
                fv = float(v)
            except (TypeError, ValueError):
                return "수집 실패"
            if abs(fv) < 1.0:
                # KIS API 한계로 거의 0 (실제 0이 아닐 가능성 매우 높음)
                return f"{fv:+.1f}억 (KIS API 한계 — 신뢰도 낮음)"
            return f"{fv:+,.0f}억"

        kr_lines.append(
            f"KOSDAQ 종합: 종가={_fmt(kosdaq_row.get('close'), ',.2f')} "
            f"등락={_fmt(kosdaq_row.get('chg_pct'), '+.2f')}% | "
            f"거래대금={_fmt(kosdaq_row.get('trading_value'), ',.0f')}억 | "
            f"외인={_fmt_flow_note(kosdaq_row.get('foreign_net_buy'))} | "
            f"기관={_fmt_flow_note(kosdaq_row.get('inst_net_buy'))}"
        )
        kr_lines.append(
            "  ⚠️ 주의: KOSDAQ 시장 전체 외인·기관 매매동향은 KIS API 미지원으로 "
            "절대값이 매우 작거나 NULL인 경우가 많음. 수치 자체는 신뢰도 낮으니 "
            "macro_view에서 KOSDAQ 수급 단정 금지."
        )
    kr_block = "\n".join(kr_lines) if kr_lines else "(데이터 없음)"

    # 3) 최근 일자별 시장 흐름 (TOP10 거래대금)
    by_date: dict[str, list[dict]] = {}
    for r in recent_top:
        by_date.setdefault(r["date"], []).append(r)
    daily_lines = []
    for d in sorted(by_date.keys(), reverse=True)[:5]:
        rows = by_date[d][:10]
        # 가격 정보 포함 — Claude가 추정 진입가 정확히 산출하도록
        names = ", ".join(
            f"{x['name'] or x['ticker']}({x['ticker']},{x['chg_pct']:+.1f}%,{_fmt(x.get('close_price'), ',.0f')}원)"
            for x in rows
        )
        daily_lines.append(f"[{d}] TOP10: {names}")
    daily_block = "\n".join(daily_lines) if daily_lines else "(데이터 없음)"

    # 4) 누적 등장 종목 (수급 시계열)
    cum_lines = []
    for i, c in enumerate(cumulative[:15], 1):
        cum_lines.append(
            f"{i}. {c['name'] or c['ticker']}({c['ticker']}) "
            f"등장={c['appearances']}회 평균등락={c['avg_chg_pct']:+.2f}% "
            f"평균거래대금={c['avg_value_eok']:,.0f}억 섹터={c['sector'] or '-'}"
        )
    cum_block = "\n".join(cum_lines) if cum_lines else "(데이터 없음)"

    # 5) 어제 회고
    if yesterday_rev:
        rv_block = (
            f"date={yesterday_rev.get('date')}, "
            f"accuracy={yesterday_rev.get('accuracy_pct')}%, "
            f"headline={yesterday_rev.get('headline')}\n"
            f"내일 전망: {yesterday_rev.get('tomorrow_outlook') or '-'}"
        )
    else:
        rv_block = "(없음 — 첫 운영)"

    # 6) 활성 학습
    if learnings:
        lr_block = "\n".join(
            f"  #{l['id']} [{l['category']}, conf={l['confidence']:.2f}] {l['content']}"
            for l in learnings
        )
    else:
        lr_block = "(아직 학습 없음)"

    user_content = f"""## 오버나이트 미국 마감
{us_block}

## 어제 한국 시장
{kr_block}

## 최근 5거래일 거래대금 TOP10 흐름
{daily_block}

## 최근 5일 누적 등장 종목 (수급 시계열)
{cum_block}

## 어제 저녁 회고
{rv_block}

## 활성 누적 학습 (반드시 적용·참조)
{lr_block}

시스템 프롬프트 규칙에 따라 STRICT JSON으로만 응답하세요."""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=3500,
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
        logger.error(f"[morning_brief] Claude 호출 실패: {type(e).__name__}: {e}")
        check_claude_error(e, "morning_brief")
        return {}


# ── 메시지 작성 ──────────────────────────────────────────────

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
    lines: list[str] = ["📰 <b>주요 뉴스</b>"]
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
            # 한 줄 길이 컷 (텔레그램 줄바꿈 최소화)
            if len(headline) > 90:
                headline = headline[:87] + "…"
            lines.append(f"    {stars} {headline}")
    lines.append("")
    return lines


def _format_message(today: str, brief: dict, us_snap: dict | None,
                    news: dict | None = None) -> str:
    """텔레그램 HTML 메시지 작성 (4096자 이내)."""
    regime_emoji = {
        "strong":   "📈",
        "sideways": "↔️",
        "weak":     "📉",
        "reversal": "🔄",
        "volatile": "🌪",
    }.get(brief.get("market_regime", "sideways"), "")

    lines: list[str] = []
    # 헤더에 요일 포함 (today가 'YYYY-MM-DD' 형식이면 요일 추가)
    header_today = today
    try:
        if len(today) == 10 and today[4] == '-':
            t_obj = date.fromisoformat(today)
            header_today = f"{today} ({_WEEKDAY_KR[t_obj.weekday()]})"
    except Exception:
        pass
    lines.append(f"🌅 <b>아침 시황 브리핑 — {header_today}</b>")
    headline = brief.get("headline") or ""
    if headline:
        lines.append(f"💬 <i>{headline}</i>")
    lines.append("")

    # 오버나이트
    if us_snap:
        lines.append("🌃 <b>오버나이트 미국 마감</b>")
        lines.append(
            f"  S&amp;P500 <b>{us_snap.get('sp500_chg_pct', 0):+.2f}%</b> | "
            f"NASDAQ <b>{us_snap.get('nasdaq_chg_pct', 0):+.2f}%</b> | "
            f"Dow {us_snap.get('dow_chg_pct', 0):+.2f}%"
        )
        lines.append(
            f"  VIX {us_snap.get('vix', 0):.1f} ({us_snap.get('vix_chg', 0):+.1f}pt) | "
            f"US10Y {us_snap.get('us10y_yield', 0):.2f}%"
        )
        lines.append(
            f"  SOXX {us_snap.get('soxx_chg_pct', 0):+.2f}% | "
            f"LIT {us_snap.get('lit_chg_pct', 0):+.2f}%"
        )
        # 주요 종목 5개 (한글)
        try:
            ks = json.loads(us_snap.get("key_stocks") or "{}")
        except Exception:
            ks = {}
        if ks:
            top5 = list(ks.items())[:5]
            ks_line = " · ".join(
                f"{v.get('name_kr', k)} {v.get('chg_pct', 0):+.1f}%"
                for k, v in top5 if isinstance(v, dict)
            )
            if ks_line:
                lines.append(f"  {ks_line}")
        lines.append("")

    # 매크로/시장 시각
    macro_view = brief.get("macro_view") or ""
    if macro_view:
        lines.append(f"{regime_emoji} <b>시황 분석</b>")
        # 텔레그램 4096자 한도 — 시황은 600자로 컷
        lines.append(f"  {macro_view[:600]}")
        lines.append("")

    # 주요 뉴스 (4분류)
    news_lines = _format_news_section(news)
    if news_lines:
        lines.extend(news_lines)

    # 섹터 4분류
    sectors = brief.get("sectors") or {}
    sect_keys = [("hot", "🔥 HOT"), ("watch", "👀 WATCH"),
                 ("cold", "❄️ COLD"), ("avoid", "🚫 AVOID")]
    has_sectors = any(sectors.get(k) for k, _ in sect_keys)
    if has_sectors:
        lines.append("📊 <b>섹터 분류</b>")
        for k, label in sect_keys:
            items = sectors.get(k) or []
            if not items:
                continue
            names = " · ".join(
                f"{(s.get('sector') or '-')}" for s in items[:5]
            )
            lines.append(f"  {label}: {names}")
        lines.append("")

    # 추천 종목 5개 (디테일)
    picks = brief.get("picks") or []
    lines.append(f"⭐ <b>추천 종목 ({len(picks)})</b>")
    # picks ≤2면 사유 표시
    low_reason = brief.get("_low_picks_reason", "")
    if low_reason:
        lines.append(low_reason)
    if picks:
        for p in picks[:_MAX_PICKS]:
            rk = p.get("rank") or "?"
            tk = p.get("ticker") or ""
            nm = p.get("name") or tk
            conf = p.get("confidence") or 0
            stars = "★" * int(conf) + "☆" * (5 - int(conf))
            entry = p.get("entry")
            sl = p.get("stop_loss_pct")
            tp = p.get("take_profit_pct")
            themes = p.get("themes") or []
            risk = p.get("risk") or ""
            reason = (p.get("reason") or "").replace("\n", " ")
            entry_note = p.get("_entry_note", "")
            entry_corrected = p.get("_entry_corrected", False)
            pick_note = p.get("_note", "")
            lines.append(f"  <b>{rk}. {nm}({tk})</b>  {stars}")
            if reason:
                lines.append(f"     {reason[:200]}")
            range_parts = []
            if entry:
                entry_str = f"진입 {int(entry):,}원"
                if entry_corrected:
                    entry_str += " ⚠️DB교체"
                elif entry_note:
                    entry_str += f" {entry_note}"
                range_parts.append(entry_str)
            elif pick_note:
                range_parts.append(f"진입 {pick_note}")
            if sl is not None:
                range_parts.append(f"손절 {sl:+.1f}%")
            if tp is not None:
                range_parts.append(f"익절 {tp:+.1f}%")
            if range_parts:
                lines.append(f"     {' · '.join(range_parts)}")
            if themes:
                lines.append(f"     테마: {', '.join(themes[:4])}")
            if risk:
                lines.append(f"     ⚠️ {risk[:120]}")
        lines.append("")

    # 종목 심층 분석 (옵션 Q Phase 1)
    if _format_deep_analysis_lines is not None:
        try:
            deep_lines = _format_deep_analysis_lines(picks)
            if deep_lines:
                lines.extend(deep_lines)
        except Exception as _e:
            logger.warning(f"[morning_brief] deep_analysis 메시지 포매팅 실패: {_e}")

    # 회피 종목 5개
    avoids = brief.get("avoids") or []
    if avoids:
        lines.append(f"🚫 <b>회피 종목 ({len(avoids)})</b>")
        for a in avoids[:_MAX_AVOIDS]:
            tk = a.get("ticker") or ""
            nm = a.get("name") or tk
            reason = (a.get("reason") or "").replace("\n", " ")
            lines.append(f"  • {nm}({tk}) — {reason[:120]}")
        lines.append("")

    # 누적 학습 적용
    applied = brief.get("lessons_applied") or []
    if applied:
        lines.append(f"📚 적용 학습: {', '.join('#' + str(i) for i in applied[:10])}")

    # 전략 톤
    tone = brief.get("strategy_tone") or ""
    if tone:
        lines.append(f"🎯 전략 톤: <b>{tone}</b>")

    # 데이터 출처 범례
    lines.append("")
    lines.append("📌 <i>데이터 출처</i>")
    lines.append("  <i>[KIS] 한국투자증권 시세·KOSPI 수급 (정확)</i>")
    lines.append("  <i>[KIS-한계] KOSDAQ 시장 전체 매매동향은 KIS API 미지원 — 신뢰도 낮음, 0억 표기는 수집 실패 가능성 포함</i>")
    lines.append("  <i>[yfinance] 미국 시장 지표 (정확)</i>")
    lines.append("  <i>[Claude] AI 분석 — 사실 기반, 수치 추정 X</i>")

    # 컷 — 한글 후처리 (Claude가 영어 단어 남긴 경우 자동 치환)
    msg = "\n".join(lines)
    msg = _kr_postprocess(msg)
    if len(msg) > _TELEGRAM_LIMIT:
        msg = msg[:_TELEGRAM_LIMIT] + "\n...[truncated]"
    return msg


# ── DB 저장 ──────────────────────────────────────────────────

def _save_briefing(today: str, brief: dict, us_snap: dict | None,
                   market_row: dict | None, full_message: str,
                   sent: bool) -> None:
    try:
        sectors = brief.get("sectors") or {}
        execute(
            """
            INSERT OR REPLACE INTO morning_briefing (
                date, overnight_us, macro, kr_context,
                market_regime, sectors_hot, sectors_watch, sectors_cold, sectors_avoid,
                picks, avoids, lessons_applied, strategy_tone, headline,
                full_message, sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                today,
                json.dumps(us_snap, ensure_ascii=False, default=str) if us_snap else None,
                json.dumps({"view": brief.get("macro_view", "")}, ensure_ascii=False),
                json.dumps(market_row, ensure_ascii=False, default=str) if market_row else None,
                brief.get("market_regime", ""),
                json.dumps(sectors.get("hot") or [], ensure_ascii=False),
                json.dumps(sectors.get("watch") or [], ensure_ascii=False),
                json.dumps(sectors.get("cold") or [], ensure_ascii=False),
                json.dumps(sectors.get("avoid") or [], ensure_ascii=False),
                json.dumps(brief.get("picks") or [], ensure_ascii=False),
                json.dumps(brief.get("avoids") or [], ensure_ascii=False),
                json.dumps(brief.get("lessons_applied") or [], ensure_ascii=False),
                brief.get("strategy_tone", ""),
                brief.get("headline", ""),
                full_message,
                datetime.now().isoformat(timespec="seconds") if sent else None,
            ),
        )
        # learnings.last_used 갱신
        applied = brief.get("lessons_applied") or []
        for lid in applied:
            try:
                execute(
                    "UPDATE learnings SET last_used = ? WHERE id = ?",
                    (today, int(lid)),
                )
            except Exception:
                continue
    except Exception as e:
        logger.error(f"[morning_brief] morning_briefing 저장 오류: {e}", exc_info=True)


# ── 메인 진입점 ───────────────────────────────────────────────

def run_morning_brief() -> dict:
    """오늘 아침 시황 분석 + 추천 5종 + 회피 5종. 텔레그램 발송 + DB 저장.

    Returns:
        {
            "date": str,
            "picks_count": int,
            "avoids_count": int,
            "sent": bool
        }
    """
    today = date.today().isoformat()
    if not is_trading_day(datetime.now()):
        logger.info(f"[morning_brief] {today} 휴장일 — 스킵")
        return {"date": today, "picks_count": 0, "avoids_count": 0, "sent": False}

    logger.info(f"[morning_brief] 시작 — {today}")

    # 뉴스 수집/분류/저장 (어제 한국 마감 + 오버나이트 미국, 최근 18시간)
    news_for_msg: dict | None = None
    if _collect_and_save_news and _get_news_for_brief:
        try:
            _collect_and_save_news(hours=18)
        except Exception as e:
            logger.warning(f"[morning_brief] 뉴스 수집 실패 — 브리핑은 계속: {e}")
        try:
            news_for_msg = _get_news_for_brief(today)
        except Exception as e:
            logger.warning(f"[morning_brief] 뉴스 조회 실패: {e}")
            news_for_msg = None

    # 데이터 수집
    us_snap        = get_latest_us_snapshot()
    market_row     = _fetch_last_market_condition()
    kosdaq_row     = _fetch_last_kosdaq()
    recent_top     = _fetch_recent_top_value(days=7)
    cumulative     = _aggregate_recent_tickers(recent_top, days=5)
    yesterday_rev  = _fetch_yesterday_review()
    learnings      = _fetch_active_learnings(limit=20)

    if not us_snap and not market_row and not recent_top:
        msg = (
            f"⚠️ <b>[아침 브리핑]</b> {today}\n"
            f"데이터 부족 — us_market/market_condition/daily_top_value 모두 비어 있습니다.\n"
            f"운영 초기에는 며칠간 데이터를 누적해야 정상 브리핑이 가능합니다."
        )
        notify(msg)
        _save_briefing(today, {}, None, None, msg, sent=True)
        return {"date": today, "picks_count": 0, "avoids_count": 0, "sent": True}

    brief = _ask_claude(
        us_snap=us_snap,
        kosdaq_row=kosdaq_row,
        market_row=market_row,
        recent_top=recent_top,
        cumulative=cumulative,
        yesterday_rev=yesterday_rev,
        learnings=learnings,
    )

    if not brief:
        fallback = (
            f"⚠️ <b>[아침 브리핑]</b> {today}\n"
            f"Claude 분석 실패 — API 오류 또는 응답 파싱 실패\n"
        )
        if learnings:
            fallback += "\n📚 <b>오늘 적용할 활성 학습</b>\n"
            for l in learnings[:5]:
                fallback += f"  • #{l['id']} [{l['category']}] {l['content'][:80]}\n"
        sent = notify(fallback)
        _save_briefing(today, {}, us_snap, market_row, fallback, sent)
        return {"date": today, "picks_count": 0, "avoids_count": 0, "sent": sent}

    # 🔴 자기 검증 — picks의 entry가 DB close_price와 일치하는지
    raw_picks = brief.get("picks") or []
    validated_picks, warnings = _validate_picks(raw_picks, recent_top)
    brief["picks"] = validated_picks
    if warnings:
        for w in warnings:
            logger.warning(f"[morning_brief 검증] {w}")

    # picks ≤2 시 사유 자동 생성
    picks_n = len(validated_picks)
    if picks_n <= 2:
        days_in_db = _count_days_in_db()
        regime = brief.get("market_regime", "")
        brief["_low_picks_reason"] = _explain_low_picks(
            picks_n, days_in_db, regime, len(learnings)
        )

    # 종목 심층 분석 (옵션 Q Phase 1) — confidence ≥3 상위 3종목
    if _deep_analyze_picks is not None and validated_picks:
        try:
            _deep_analyze_picks(validated_picks, today)
        except Exception as e:
            logger.error(f"[morning_brief] deep_analyze_picks 오류: {e}", exc_info=True)

    # 메시지 작성 + 발송
    msg = _format_message(today, brief, us_snap, news=news_for_msg)
    sent = notify(msg)
    if not sent:
        logger.warning("[morning_brief] 텔레그램 발송 실패 — DB 저장은 진행")

    _save_briefing(today, brief, us_snap, market_row, msg, sent)

    avoids_n = len(brief.get("avoids") or [])
    logger.info(
        f"[morning_brief] 완료 — picks={picks_n} avoids={avoids_n} sent={sent} 검증경고={len(warnings)}"
    )
    return {"date": today, "picks_count": picks_n, "avoids_count": avoids_n, "sent": sent}


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    r = run_morning_brief()
    print(r)
