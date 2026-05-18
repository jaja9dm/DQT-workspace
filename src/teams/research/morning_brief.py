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

# 사람 개입 알림 — 데이터/운영 점검 (실패해도 본 잡은 계속 진행)
try:
    from src.utils.human_alert import run_health_checks as _run_health_checks
except Exception:
    _run_health_checks = None

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

# 정량 점수화 + 시장 국면 자동 분류 (2026-05-18)
try:
    from src.teams.research.pick_scorer import score_pick as _score_pick
except Exception:
    _score_pick = None

try:
    from src.teams.research.market_regime import classify_regime as _classify_regime
except Exception:
    _classify_regime = None

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

### 🎯 손절/익절 산출 — 종목별 변동성 기반 (절대 일률 -2/+3 금지)

추천 종목 각각의 stop_loss_pct / take_profit_pct는 **반드시 종목별로 차등 산출**:

1. **입력 데이터의 atr_pct (일일 평균 변동성, %) 기반**:
   - stop_loss_pct = max(-4.0, min(-1.5, -1.5 × atr_pct))
   - take_profit_pct = max(2.5, min(6.0, 2.5 × atr_pct))
   - Risk:Reward 약 1:1.5~1:1.7 유지

2. **atr_pct 없을 때 — 종가/등락률로 추정**:
   - 대형주 (close_price ≥ 100,000원, 거래대금 TOP 20, 일평균 변동 1~2%):
     → 손절 -1.5~-2.0% / 익절 +2.5~+3.0%
   - 중형주 (close_price 30,000~100,000원):
     → 손절 -2.0~-2.5% / 익절 +3.0~+4.0%
   - 소형주/변동성 종목 (close_price < 30,000원, 또는 어제 chg_pct 절대값 ≥ 5%):
     → 손절 -2.5~-3.5% / 익절 +4.0~+5.5%

3. **금지 — 절대 하지 말 것**:
   - 모든 종목에 -2.0/+3.0 같은 동일값 부여 (게으른 디폴트)
   - 손절이 익절보다 큰 절대값 (Risk:Reward 1 미만)
   - 변동성 큰 종목에 -1.5% 같은 과도하게 좁은 손절 (탭 손절 위험)

4. **표시 정밀도**: 소수점 1자리 (예: -2.3, +4.5). 항상 같은 값 X.
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

    # 🎯 손절/익절 검증 — 종목별 기술적 분석 기반 (2026-05-18)
    #
    # 사용자 비판 수용: "대형/중형/소형 단순 분류 X. 종목 데이터 + 보조지표로 판단"
    # → technical_analysis.analyze_ticker() 결과로 Claude SL/TP를 무조건 교체.
    #
    # 일봉 60일 + 지지선/저항선/볼린저/MA20/ATR/피보나치 → 차트 기반 손절/익절.
    try:
        from src.teams.research.technical_analysis import analyze_ticker
    except Exception as e:
        logger.warning(f"[morning_brief] technical_analysis 임포트 실패 — 기존 폴백 사용: {e}")
        analyze_ticker = None  # type: ignore[assignment]

    # ticker별 최신 close_price (현재가 참조점)
    latest_meta: dict[str, dict] = {}
    for r in recent_top:
        tk = r.get("ticker")
        if tk and tk not in latest_meta:
            latest_meta[tk] = {
                "atr_pct": r.get("atr_pct"),
                "close_price": r.get("close_price"),
                "chg_pct": r.get("chg_pct"),
            }

    for p in fixed:
        tk = p.get("ticker")
        meta = latest_meta.get(tk, {})
        current_price = p.get("entry") or meta.get("close_price") or 0
        if not tk or current_price <= 0:
            continue

        claude_sl = p.get("stop_loss_pct")
        claude_tp = p.get("take_profit_pct")

        ta_result = None
        if analyze_ticker is not None:
            try:
                ta_result = analyze_ticker(tk, float(current_price))
            except Exception as e:
                logger.warning(f"[morning_brief] {tk} 기술적 분석 실패: {e}")

        if ta_result:
            ta_sl = ta_result["stop_loss"]
            ta_tp = ta_result["take_profit"]
            rr = ta_result.get("risk_reward_ratio", 0)

            # Claude SL/TP → 기술적 분석 결과로 무조건 교체
            p["stop_loss_pct"] = ta_sl["pct"]
            p["take_profit_pct"] = ta_tp["pct"]
            p["stop_loss_price"] = int(ta_sl["price"])
            p["take_profit_price"] = int(ta_tp["price"])
            p["stop_loss_basis"] = ta_sl["basis"]
            p["take_profit_basis"] = ta_tp["basis"]
            p["risk_reward_ratio"] = rr
            p["_sl_tp_corrected"] = True
            p["_ta_meta"] = {
                "atr_pct":      ta_result["atr_pct"],
                "rsi_14":       ta_result["rsi_14"],
                "ma20":         ta_result["ma"]["ma20"],
                "bb_upper":     ta_result["bollinger"]["upper"],
                "bb_lower":     ta_result["bollinger"]["lower"],
                "n_supports":   len(ta_result["support_levels"]),
                "n_resistances": len(ta_result["resistance_levels"]),
            }
            warnings.append(
                f"{p.get('name', tk)}({tk}): Claude SL/TP({claude_sl}/{claude_tp}) → "
                f"차트 기반 {ta_sl['pct']:+.2f}%/{ta_tp['pct']:+.2f}% "
                f"(R:R 1:{rr}) — {ta_sl['basis']} | {ta_tp['basis']}"
            )
        else:
            # 기술적 분석 실패 → 기존 ATR 폴백 유지
            atr = meta.get("atr_pct")
            close = meta.get("close_price") or current_price
            chg = abs(meta.get("chg_pct") or 0)
            if atr and atr > 0:
                rec_sl = max(-4.0, min(-1.5, round(-1.5 * atr, 1)))
                rec_tp = max(2.5, min(6.0, round(2.5 * atr, 1)))
            else:
                if close >= 100000:
                    rec_sl, rec_tp = -1.8, 2.8
                elif close >= 30000:
                    rec_sl, rec_tp = -2.3, 3.5
                else:
                    rec_sl, rec_tp = -3.0, 4.5
                if chg >= 5:
                    rec_sl, rec_tp = rec_sl - 0.5, rec_tp + 1.0
            p["stop_loss_pct"] = rec_sl
            p["take_profit_pct"] = rec_tp
            p["stop_loss_basis"] = f"ATR 폴백 (TA 분석 불가)"
            p["take_profit_basis"] = f"ATR 폴백 (TA 분석 불가)"
            p["_sl_tp_corrected"] = True
            warnings.append(
                f"{p.get('name', tk)}({tk}): TA 분석 실패 → ATR 폴백 {rec_sl}/{rec_tp}"
            )

    # 🎯 정량 점수화 (2026-05-18) — Claude confidence를 시스템 점수로 교체.
    # 4개 요소 (거래대금 / 외인기관 수급 / 기술적 신호 / 섹터 동조성) 0~100점.
    if _score_pick is not None:
        for p in fixed:
            tk = p.get("ticker")
            if not tk:
                continue
            meta = latest_meta.get(tk, {})
            current_price = p.get("entry") or meta.get("close_price") or 0
            ta_result_for_score = None
            # TA 결과 재활용 — 위에서 analyze_ticker 호출 시 dict로 저장하지 않았으므로
            # _ta_meta로부터 최소 정보 복원
            ta_meta = p.get("_ta_meta") or {}
            if ta_meta:
                ta_result_for_score = {
                    "rsi_14": ta_meta.get("rsi_14"),
                    "ma": {
                        "ma5":  None,         # ma5는 _ta_meta에 없음 — None이면 정배열 점수 0
                        "ma20": ta_meta.get("ma20"),
                        "ma60": None,
                    },
                    "macd": {"hist": None},   # MACD는 _ta_meta에 없음
                    "atr_pct": ta_meta.get("atr_pct"),
                }
            # 더 정확한 점수를 위해 analyze_ticker 재호출 (이미 호출했어도 비용 미미)
            if analyze_ticker is not None and current_price > 0:
                try:
                    ta_full = analyze_ticker(tk, float(current_price))
                    if ta_full:
                        ta_result_for_score = ta_full
                except Exception:
                    pass

            try:
                score_result = _score_pick(
                    ticker=tk,
                    current_price=float(current_price) if current_price else 0.0,
                    recent_top=recent_top,
                    sector_peers=recent_top,
                    market_regime=p.get("_market_regime", "sideways"),
                    ta_result=ta_result_for_score,
                )
                claude_conf = p.get("confidence")
                p["_claude_confidence"] = claude_conf
                p["confidence"] = score_result["confidence"]
                p["score"] = score_result["score"]
                p["score_components"] = score_result["components"]
                p["score_rationale"] = score_result["rationale"]
                warnings.append(
                    f"{p.get('name', tk)}({tk}): 정량 점수 {score_result['score']}점 "
                    f"(거래대금 {score_result['components']['volume_momentum']} + "
                    f"수급 {score_result['components']['capital_flow']} + "
                    f"기술 {score_result['components']['technical']} + "
                    f"섹터 {score_result['components']['sector_sync']}) → "
                    f"confidence {claude_conf} → {score_result['confidence']}"
                )
            except Exception as e:
                logger.warning(f"[morning_brief] {tk} 정량 점수화 실패: {e}")

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
               foreign_net_buy, inst_net_buy, rsi_14, atr_pct
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

def _format_auto_regime(auto_regime: dict | None) -> str:
    """시스템 자동 분류 결과를 Claude 입력용 텍스트로."""
    if not auto_regime:
        return "(시스템 자동 분류 실패 — Claude가 단독 판단)"
    regime = auto_regime.get("regime") or "sideways"
    ind = auto_regime.get("indicators") or {}
    rationale = auto_regime.get("rationale") or ""
    label = _REGIME_LABEL.get(regime, regime)
    lines = [
        f"자동 분류 결과: {regime} ({label})",
        f"근거: {rationale}",
        "지표 요약:",
    ]
    if ind.get("kospi_ma20_dev_pct") is not None:
        lines.append(f"  - KOSPI 20일선 이격: {ind['kospi_ma20_dev_pct']:+.2f}%")
    if ind.get("kospi_yesterday_chg") is not None:
        lines.append(f"  - KOSPI 어제 등락: {ind['kospi_yesterday_chg']:+.2f}%")
    if ind.get("kospi_std_5d_pct") is not None:
        lines.append(f"  - KOSPI 5일 표준편차: {ind['kospi_std_5d_pct']:.2f}%")
    if ind.get("vix"):
        lines.append(f"  - VIX: {ind['vix']:.2f} ({ind.get('vix_chg', 0):+.2f}pt)")
    lines.append(f"  - 외인 5일 누적: {ind.get('foreign_5d_eok', 0):+,.0f}억")
    lines.append(f"  - TOP100 거래대금 5일 대비: {ind.get('volume_ratio', 1.0):.2f}배")
    lines.append(
        "📌 최종 market_regime은 시스템 분류값을 우선 사용. "
        "Claude는 자동 분류와 다른 견해가 있을 때만 macro_view에 사유 명시."
    )
    return "\n".join(lines)


def _ask_claude(
    *,
    us_snap: dict | None,
    kosdaq_row: dict | None,
    market_row: dict | None,
    recent_top: list[dict],
    cumulative: list[dict],
    yesterday_rev: dict | None,
    learnings: list[dict],
    auto_regime: dict | None = None,
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

        # KOSDAQ 시장 전체 매매동향 — Naver 통합 API 우선, KIS 폴백.
        # 신뢰도는 source 컬럼으로 구분 (naver/pykrx = 신뢰 / kis = 한계)
        kd_source = (kosdaq_row.get("source") or "").lower()

        def _fmt_flow_note(v, src=kd_source):
            if v is None:
                return "수집 실패(NULL)"
            try:
                fv = float(v)
            except (TypeError, ValueError):
                return "수집 실패"
            if src in ("naver", "pykrx"):
                label = "[네이버]" if src == "naver" else "[KRX]"
                return f"{fv:+,.0f}억 {label}"
            # KIS 폴백 또는 source 미상 — KIS는 KOSDAQ 미지원이라 거의 0
            if abs(fv) < 1.0:
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

## 시스템 자동 분류 — 시장 국면 (참고 + 검증용)
{_format_auto_regime(auto_regime)}

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


def _build_picks_lines(picks: list, low_reason: str, reason_cut: int = 200,
                       show_themes: bool = True, show_risk: bool = True,
                       show_score_breakdown: bool = True) -> list[str]:
    """추천 종목 섹션 라인 빌더 (재호출 가능 — 동적 컷 조절)."""
    lines: list[str] = [f"⭐ <b>추천 종목 ({len(picks)})</b>"]
    if low_reason:
        lines.append(low_reason)
    if not picks:
        lines.append("")
        return lines
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
        # 정량 점수 표시 (2026-05-18)
        score = p.get("score")
        comp = p.get("score_components") or {}
        score_str = ""
        if score is not None:
            if show_score_breakdown and comp:
                score_str = (
                    f" <i>{score}점</i> "
                    f"(거래대금 {comp.get('volume_momentum', 0)} + "
                    f"수급 {comp.get('capital_flow', 0)} + "
                    f"기술 {comp.get('technical', 0)} + "
                    f"섹터 {comp.get('sector_sync', 0)})"
                )
            else:
                score_str = f" <i>{score}점</i>"
        lines.append(f"  <b>{rk}. {nm}({tk})</b>  {stars}{score_str}")
        if reason and reason_cut > 0:
            lines.append(f"     {reason[:reason_cut]}")
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
        sl_price = p.get("stop_loss_price")
        tp_price = p.get("take_profit_price")
        sl_basis = p.get("stop_loss_basis") or ""
        tp_basis = p.get("take_profit_basis") or ""
        rr = p.get("risk_reward_ratio")
        if sl is not None:
            if sl_price:
                range_parts.append(f"손절 {sl:+.1f}% ({int(sl_price):,}원)")
            else:
                range_parts.append(f"손절 {sl:+.1f}%")
        if tp is not None:
            if tp_price:
                range_parts.append(f"익절 {tp:+.1f}% ({int(tp_price):,}원)")
            else:
                range_parts.append(f"익절 {tp:+.1f}%")
        if rr:
            range_parts.append(f"R:R 1:{rr}")
        if range_parts:
            lines.append(f"     {' · '.join(range_parts)}")
        # 산출 근거 — 차트 기반 손절/익절일 때만 표시 (라인 절약)
        if sl_basis and tp_basis and reason_cut > 0:
            lines.append(f"     <i>근거: SL={sl_basis} | TP={tp_basis}</i>")
        if show_themes and themes:
            lines.append(f"     테마: {', '.join(themes[:4])}")
        if show_risk and risk:
            lines.append(f"     ⚠️ {risk[:120]}")
    lines.append("")
    return lines


def _build_avoids_lines(avoids: list, reason_cut: int = 120) -> list[str]:
    lines: list[str] = [f"🚫 <b>회피 종목 ({len(avoids)})</b>"]
    for a in avoids[:_MAX_AVOIDS]:
        tk = a.get("ticker") or ""
        nm = a.get("name") or tk
        reason = (a.get("reason") or "").replace("\n", " ")
        if reason_cut > 0:
            lines.append(f"  • {nm}({tk}) — {reason[:reason_cut]}")
        else:
            lines.append(f"  • {nm}({tk})")
    lines.append("")
    return lines


def _format_message(today: str, brief: dict, us_snap: dict | None,
                    news: dict | None = None) -> str:
    """텔레그램 HTML 메시지 작성 (4096자 이내).

    섹션을 우선순위별로 빌드하고, 한도 초과 시 우선순위 낮은 섹션을
    축약/제거하는 적응형 길이 조절을 수행한다.

    우선순위(높음→낮음): header > overnight > picks > macro > strategy/lessons
                       > sectors > avoids > news > deep_analysis
    """
    regime_emoji = {
        "strong":   "📈",
        "sideways": "↔️",
        "weak":     "📉",
        "reversal": "🔄",
        "volatile": "🌪",
    }.get(brief.get("market_regime", "sideways"), "")

    # ── 헤더 ────────────────────────────────────────
    header_lines: list[str] = []
    header_today = today
    try:
        if len(today) == 10 and today[4] == '-':
            t_obj = date.fromisoformat(today)
            header_today = f"{today} ({_WEEKDAY_KR[t_obj.weekday()]})"
    except Exception:
        pass
    header_lines.append(f"🌅 <b>아침 시황 브리핑 — {header_today}</b>")
    headline = brief.get("headline") or ""
    if headline:
        header_lines.append(f"💬 <i>{headline}</i>")
    # 자동 시장 국면 분류 (2026-05-18)
    auto_regime = brief.get("_auto_regime") or {}
    if auto_regime:
        regime_label = _REGIME_LABEL.get(auto_regime.get("regime"), auto_regime.get("regime", ""))
        ind = auto_regime.get("indicators") or {}
        kospi_dev = ind.get("kospi_ma20_dev_pct")
        vix_val = ind.get("vix")
        foreign_5d = ind.get("foreign_5d_eok")
        parts = []
        if kospi_dev is not None:
            parts.append(f"KOSPI MA20 {kospi_dev:+.2f}%")
        if vix_val:
            parts.append(f"VIX {vix_val:.1f}")
        if foreign_5d is not None:
            parts.append(f"외인 5일 {foreign_5d:+,.0f}억")
        ind_str = " · ".join(parts)
        header_lines.append(
            f"🔍 <b>시장 국면</b>: {regime_emoji} {regime_label} <i>({ind_str})</i>"
        )
    header_lines.append("")

    # ── 오버나이트 미국 마감 ────────────────────────
    overnight_lines: list[str] = []
    if us_snap:
        overnight_lines.append("🌃 <b>오버나이트 미국 마감</b>")
        overnight_lines.append(
            f"  S&amp;P500 <b>{us_snap.get('sp500_chg_pct', 0):+.2f}%</b> | "
            f"NASDAQ <b>{us_snap.get('nasdaq_chg_pct', 0):+.2f}%</b> | "
            f"Dow {us_snap.get('dow_chg_pct', 0):+.2f}%"
        )
        overnight_lines.append(
            f"  VIX {us_snap.get('vix', 0):.1f} ({us_snap.get('vix_chg', 0):+.1f}pt) | "
            f"US10Y {us_snap.get('us10y_yield', 0):.2f}%"
        )
        overnight_lines.append(
            f"  SOXX {us_snap.get('soxx_chg_pct', 0):+.2f}% | "
            f"LIT {us_snap.get('lit_chg_pct', 0):+.2f}%"
        )
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
                overnight_lines.append(f"  {ks_line}")
        overnight_lines.append("")

    # ── 매크로 시각 (시황) ──────────────────────────
    macro_view = brief.get("macro_view") or ""
    def _build_macro(cut: int) -> list[str]:
        if not macro_view:
            return []
        return [
            f"{regime_emoji} <b>시황 분석</b>",
            f"  {macro_view[:cut]}",
            "",
        ]

    # ── 주요 뉴스 (4분류) ───────────────────────────
    news_full = _format_news_section(news)

    def _build_news(max_per_cat: int) -> list[str]:
        """뉴스 라인을 카테고리당 max_per_cat개로 제한."""
        if not news_full or max_per_cat <= 0 or not news:
            return []
        out: list[str] = ["📰 <b>주요 뉴스</b>"]
        for cat, label in _NEWS_CAT_META:
            items = news.get(cat) or []
            if not items:
                continue
            out.append(f"  {label}")
            for n in items[:max_per_cat]:
                imp = int(n.get("importance") or 3)
                stars = "★" * imp
                headline_n = (n.get("headline") or "").replace("\n", " ").strip()
                if not headline_n:
                    continue
                if len(headline_n) > 90:
                    headline_n = headline_n[:87] + "…"
                out.append(f"    {stars} {headline_n}")
        out.append("")
        return out

    # ── 섹터 4분류 ─────────────────────────────────
    sectors = brief.get("sectors") or {}
    sect_keys = [("hot", "🔥 HOT"), ("watch", "👀 WATCH"),
                 ("cold", "❄️ COLD"), ("avoid", "🚫 AVOID")]
    has_sectors = any(sectors.get(k) for k, _ in sect_keys)
    sectors_lines: list[str] = []
    if has_sectors:
        sectors_lines.append("📊 <b>섹터 분류</b>")
        for k, label in sect_keys:
            items = sectors.get(k) or []
            if not items:
                continue
            names = " · ".join(
                f"{(s.get('sector') or '-')}" for s in items[:5]
            )
            sectors_lines.append(f"  {label}: {names}")
        sectors_lines.append("")

    # ── 추천 종목 (디테일) ─────────────────────────
    picks = brief.get("picks") or []
    low_reason = brief.get("_low_picks_reason", "")

    # ── 종목 심층 분석 ─────────────────────────────
    deep_analysis_full: list[str] = []
    if _format_deep_analysis_lines is not None:
        try:
            d = _format_deep_analysis_lines(picks)
            if d:
                deep_analysis_full = list(d)
        except Exception as _e:
            logger.warning(f"[morning_brief] deep_analysis 메시지 포매팅 실패: {_e}")

    # ── 회피 종목 ──────────────────────────────────
    avoids = brief.get("avoids") or []

    # ── 누적 학습 / 전략 톤 ───────────────────────
    applied = brief.get("lessons_applied") or []
    tone = brief.get("strategy_tone") or ""
    footer_lines: list[str] = []
    if applied:
        footer_lines.append(
            f"📚 적용 학습: {', '.join('#' + str(i) for i in applied[:10])}"
        )
    if tone:
        footer_lines.append(f"🎯 전략 톤: <b>{tone}</b>")

    legend = "\n\n📌 <i>데이터 KIS·yfinance·네이버 / 분석 Claude (추정 X)</i>"
    body_limit = _TELEGRAM_LIMIT - len(legend) - 20

    # ── 적응형 조립 ─────────────────────────────────
    # 동적 컷 단계 (Step 0 = 풍부, Step N = 최소).
    # 우선순위 낮은 순으로 축약: news → deep_analysis → avoids → sectors → picks(테마/리스크) → macro
    pick_reason_cut_steps = [200, 160, 120, 80, 60]
    avoid_reason_cut_steps = [120, 80, 60, 0, 0]
    news_per_cat_steps = [99, 3, 2, 1, 0]
    macro_cut_steps = [600, 500, 400, 300, 200]
    deep_steps = [True, True, False, False, False]
    show_themes_steps = [True, True, True, False, False]
    show_risk_steps = [True, True, True, False, False]
    sectors_steps = [True, True, True, True, False]
    show_score_breakdown_steps = [True, True, True, False, False]

    last_step = 0
    msg = ""
    for step in range(len(pick_reason_cut_steps)):
        last_step = step
        all_lines: list[str] = []
        all_lines.extend(header_lines)
        all_lines.extend(overnight_lines)
        all_lines.extend(_build_macro(macro_cut_steps[step]))
        all_lines.extend(_build_news(news_per_cat_steps[step]))
        if sectors_steps[step]:
            all_lines.extend(sectors_lines)
        all_lines.extend(_build_picks_lines(
            picks, low_reason,
            reason_cut=pick_reason_cut_steps[step],
            show_themes=show_themes_steps[step],
            show_risk=show_risk_steps[step],
            show_score_breakdown=show_score_breakdown_steps[step],
        ))
        if deep_steps[step]:
            all_lines.extend(deep_analysis_full)
        if avoids:
            all_lines.extend(_build_avoids_lines(
                avoids, reason_cut=avoid_reason_cut_steps[step]
            ))
        all_lines.extend(footer_lines)

        candidate = "\n".join(all_lines)
        candidate = _kr_postprocess(candidate)
        if len(candidate) <= body_limit:
            msg = candidate
            if step > 0:
                logger.info(
                    f"[morning_brief] 메시지 길이 자동 조절 — step {step} 적용 "
                    f"({len(candidate)}/{body_limit}자)"
                )
            elif len(candidate) > body_limit - 200:
                logger.info(
                    f"[morning_brief] 메시지 길이 {len(candidate)}/{body_limit}자 (한도 근접)"
                )
            break
        msg = candidate  # 마지막 시도 결과 보존 (fallback)

    # 마지막 단계까지 가도 초과면 hard truncate (legend 보존)
    if len(msg) > body_limit:
        orig_len = len(msg)
        msg = msg[:body_limit] + "\n...[중략]"
        logger.warning(
            f"[morning_brief] 메시지 hard truncate — step {last_step}에서도 본문 "
            f"{orig_len}자 > 한도 {body_limit}자 (예외적 케이스)"
        )

    msg += legend
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

    # 사람 개입 필요 점검 (데이터 + 운영) — 본 잡과 독립 실행
    if _run_health_checks is not None:
        try:
            _run_health_checks()
        except Exception as e:
            logger.warning(f"[morning_brief] human_alert 점검 실패 — 브리핑은 계속: {e}")

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

    # 데이터 부족 시 자동 EOD 적재 (회사 노트북 죽어있어 며칠치 누락된 경우 등)
    # daily_top_value 또는 us_market_daily 비어있으면 즉시 재적재 시도.
    if not recent_top or not us_snap:
        logger.warning(
            f"[morning_brief] 데이터 부족 (top={len(recent_top)}건, us={'O' if us_snap else 'X'}) "
            f"→ daily_eod_load 자동 실행"
        )
        try:
            from src.teams.research.daily_eod_loader import run_daily_eod_load
            eod_result = run_daily_eod_load()
            logger.info(f"[morning_brief] EOD 자동 적재 완료 — {eod_result}")
            us_snap     = get_latest_us_snapshot()
            market_row  = _fetch_last_market_condition()
            kosdaq_row  = _fetch_last_kosdaq()
            recent_top  = _fetch_recent_top_value(days=7)
        except Exception as e:
            logger.error(f"[morning_brief] EOD 자동 적재 실패 — 기존 데이터로 계속: {e}", exc_info=True)

    cumulative     = _aggregate_recent_tickers(recent_top, days=5)
    yesterday_rev  = _fetch_yesterday_review()

    # 시장 국면 자동 분류 (2026-05-18) — Claude 입력 + 최종 결과에 우선 적용
    auto_regime: dict | None = None
    if _classify_regime is not None:
        try:
            auto_regime = _classify_regime()
            logger.info(
                f"[morning_brief] 자동 시장 국면 분류: {auto_regime['regime']} — "
                f"{auto_regime.get('rationale', '')[:200]}"
            )
        except Exception as e:
            logger.warning(f"[morning_brief] 시장 국면 자동 분류 실패: {e}")
            auto_regime = None

    # regime별 학습 필터 (자동 분류 결과 우선)
    learnings_regime = auto_regime["regime"] if auto_regime else None
    learnings = _fetch_active_learnings(limit=20, regime=learnings_regime)

    if not us_snap and not market_row and not recent_top:
        msg = (
            f"⚠️ <b>[아침 브리핑]</b> {today}\n"
            f"데이터 부족 — us_market/market_condition/daily_top_value 모두 비어 있습니다.\n"
            f"EOD 자동 적재도 실패했습니다. 네트워크/KIS 토큰/외부 API 키를 확인하세요."
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
        auto_regime=auto_regime,
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

    # 시스템 자동 분류 우선 — Claude가 다르게 판단해도 정량 결과로 교체.
    if auto_regime:
        claude_regime = brief.get("market_regime") or "sideways"
        sys_regime = auto_regime["regime"]
        if claude_regime != sys_regime:
            logger.info(
                f"[morning_brief] market_regime 교체: "
                f"Claude={claude_regime} → 시스템={sys_regime} "
                f"({auto_regime.get('rationale', '')[:120]})"
            )
        brief["market_regime"] = sys_regime
        brief["_auto_regime"] = auto_regime

    # 🔴 자기 검증 — picks의 entry가 DB close_price와 일치하는지
    raw_picks = brief.get("picks") or []
    # 각 pick에 현재 시장 국면 컨텍스트 주입 (score_pick에서 활용)
    current_regime = brief.get("market_regime") or "sideways"
    for p in raw_picks:
        p["_market_regime"] = current_regime
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
