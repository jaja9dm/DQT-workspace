"""
stock_deep_analysis.py — 종목 심층 분석 (옵션 Q Phase 1)

morning_brief 추천 종목 중 신뢰도 ≥3인 상위 3개에 대해 4가지 raw 데이터를 추출하고
Claude(Sonnet)에 한 번 호출해 통합 분석 + 진입 시점 권고 + 손익 시나리오를 생성한다.

4가지 raw 데이터:
  B. 유사 패턴 백테스트  — daily_top_value 30~60일치에서 시그널 유사 사례 + 익일 등락 통계
  C. 외인/기관 자금 흐름 — 최근 5~10일 누적/연속/추세
  E. 유관 종목 동조성    — 같은 sector 어제 평균 등락 + 분포
  G. 뉴스 매핑           — daily_news related_tickers 매칭 + 헤드라인 fallback

핵심 함수:
  deep_analyze_picks(picks, date_str) -> picks (각 항목에 'deep_analysis' 키 추가)

비용 견적:
  종목당 입력 ~3k + 출력 ~800 토큰 (Sonnet)
  3종목/일 × 20거래일 ≈ $1.2/월
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from statistics import median

import anthropic

from src.config.settings import settings
from src.infra.database import fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import check_claude_error

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

# 분석 대상 — 신뢰도 임계값 + 최대 종목 수
_MIN_CONFIDENCE = 3
_MAX_DEEP_PICKS = 3

# 유사 패턴 검색 윈도우
_PATTERN_LOOKBACK_DAYS = 60
_RSI_TOLERANCE = 8.0          # ±8 포인트
_CHG_TOLERANCE = 2.0          # ±2 %p

# 외인/기관 윈도우
_FLOW_LOOKBACK_DAYS = 10

# 뉴스 윈도우
_NEWS_LOOKBACK_DAYS = 2


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


# ── B. 유사 패턴 백테스트 ────────────────────────────────────

def _backtest_similar_pattern(
    ticker: str,
    sector: str | None,
    today_rsi: float | None,
    today_chg: float | None,
    foreign_dir: int,    # +1 매수 / -1 매도 / 0 중립
    date_str: str,
) -> dict:
    """유사 시그널 케이스를 daily_top_value 시계열에서 검색 → 익일 등락 통계.

    검색 기준:
      - 같은 ticker 또는 같은 sector
      - RSI ± _RSI_TOLERANCE
      - chg_pct ± _CHG_TOLERANCE
      - 외인 매수/매도 방향 동일 (foreign_dir=0이면 무관)

    Returns:
      {
        "matched_cases": int,
        "next_day_avg": float | None,    # %
        "next_day_median": float | None,
        "win_rate": float | None,        # 0~1 (다음날 양봉 비율)
        "best_case": {...} | None,
        "worst_case": {...} | None,
        "interpretation": str,
        "data_available": bool
      }
    """
    today_rsi = float(today_rsi) if today_rsi is not None else None
    today_chg = float(today_chg) if today_chg is not None else None

    if today_rsi is None and today_chg is None:
        return {
            "matched_cases": 0, "next_day_avg": None, "next_day_median": None,
            "win_rate": None, "best_case": None, "worst_case": None,
            "interpretation": "시그널(RSI/등락률) 데이터 부족 — 패턴 매칭 불가",
            "data_available": False,
        }

    # 후보 케이스 SELECT — 같은 ticker OR sector
    rows = fetch_all(
        """
        SELECT date, ticker, name, sector, chg_pct, rsi_14,
               foreign_net_buy, close_price
        FROM daily_top_value
        WHERE date < ?
          AND date >= date(?, '-' || ? || ' days')
          AND (ticker = ? OR sector = ?)
          AND rsi_14 IS NOT NULL
          AND chg_pct IS NOT NULL
        ORDER BY date DESC, rank ASC
        """,
        (date_str, date_str, _PATTERN_LOOKBACK_DAYS, ticker, sector or ""),
    )
    candidates = [dict(r) for r in rows]

    matched: list[dict] = []
    for c in candidates:
        rsi = c.get("rsi_14")
        chg = c.get("chg_pct")
        if rsi is None or chg is None:
            continue
        if today_rsi is not None and abs(float(rsi) - today_rsi) > _RSI_TOLERANCE:
            continue
        if today_chg is not None and abs(float(chg) - today_chg) > _CHG_TOLERANCE:
            continue
        if foreign_dir != 0:
            fnb = float(c.get("foreign_net_buy") or 0)
            if foreign_dir > 0 and fnb < 0:
                continue
            if foreign_dir < 0 and fnb > 0:
                continue
        matched.append(c)

    # 익일 등락률 조회
    next_returns: list[dict] = []
    for c in matched:
        next_row = fetch_one(
            """
            SELECT date, chg_pct, close_price
            FROM daily_top_value
            WHERE ticker = ? AND date > ?
            ORDER BY date ASC LIMIT 1
            """,
            (c["ticker"], c["date"]),
        )
        if next_row and next_row["chg_pct"] is not None:
            next_returns.append({
                "date": c["date"],
                "name": c["name"],
                "ticker": c["ticker"],
                "next_date": next_row["date"],
                "next_chg": float(next_row["chg_pct"]),
            })

    if not next_returns:
        return {
            "matched_cases": 0,
            "next_day_avg": None, "next_day_median": None, "win_rate": None,
            "best_case": None, "worst_case": None,
            "interpretation": (
                f"최근 {_PATTERN_LOOKBACK_DAYS}일 내 유사 시그널 사례 0건 — "
                "데이터 누적 부족 또는 신규 패턴"
            ),
            "data_available": False,
        }

    chgs = [r["next_chg"] for r in next_returns]
    wins = sum(1 for c in chgs if c > 0)
    best = max(next_returns, key=lambda x: x["next_chg"])
    worst = min(next_returns, key=lambda x: x["next_chg"])

    avg = sum(chgs) / len(chgs)
    med = median(chgs)
    win_rate = wins / len(chgs)

    return {
        "matched_cases": len(chgs),
        "next_day_avg": round(avg, 2),
        "next_day_median": round(med, 2),
        "win_rate": round(win_rate, 2),
        "best_case": {
            "date": str(best["date"]),
            "name": best["name"],
            "next_chg": round(best["next_chg"], 2),
        },
        "worst_case": {
            "date": str(worst["date"]),
            "name": worst["name"],
            "next_chg": round(worst["next_chg"], 2),
        },
        "interpretation": (
            f"지난 {_PATTERN_LOOKBACK_DAYS}일 유사 패턴 {len(chgs)}건 중 "
            f"{wins}건 익일 양봉 ({win_rate*100:.0f}% 적중), "
            f"평균 {avg:+.2f}% / 중앙 {med:+.2f}%"
        ),
        "data_available": True,
    }


# ── C. 외인/기관 자금 흐름 ───────────────────────────────────

def _analyze_capital_flow(ticker: str, date_str: str) -> dict:
    """최근 N일 외인·기관 순매수 시계열 분석.

    Returns:
      {
        "foreign_net_5d": float | None,    # 원 단위 누계
        "foreign_streak": int,             # +N 연속 매수 / -N 연속 매도 / 0
        "foreign_trend": "강화|약화|반전|혼조",
        "inst_net_5d": float | None,
        "inst_streak": int,
        "interpretation": str,
        "days_in_data": int,
      }
    """
    rows = fetch_all(
        """
        SELECT date, foreign_net_buy, inst_net_buy
        FROM daily_top_value
        WHERE ticker = ?
          AND date <= ?
          AND date >= date(?, '-' || ? || ' days')
        ORDER BY date DESC
        """,
        (ticker, date_str, date_str, _FLOW_LOOKBACK_DAYS),
    )
    data = [dict(r) for r in rows]
    if not data:
        return {
            "foreign_net_5d": None, "foreign_streak": 0, "foreign_trend": "데이터없음",
            "inst_net_5d": None, "inst_streak": 0,
            "interpretation": "최근 거래대금 TOP 100 진입 없음 — 수급 데이터 부족",
            "days_in_data": 0,
        }

    # 최근 5일 누적
    last5 = data[:5]
    f_sum = sum(float(r.get("foreign_net_buy") or 0) for r in last5)
    i_sum = sum(float(r.get("inst_net_buy") or 0) for r in last5)

    # 연속 매수/매도 streak (최신부터)
    def _streak(values: list[float]) -> int:
        if not values:
            return 0
        first = values[0]
        if first > 0:
            sign, n = 1, 0
            for v in values:
                if v > 0:
                    n += 1
                else:
                    break
            return n
        if first < 0:
            sign, n = -1, 0
            for v in values:
                if v < 0:
                    n += 1
                else:
                    break
            return -n
        return 0

    f_vals = [float(r.get("foreign_net_buy") or 0) for r in data]
    i_vals = [float(r.get("inst_net_buy") or 0) for r in data]
    f_streak = _streak(f_vals)
    i_streak = _streak(i_vals)

    # 추세 분류: 최근 3일 vs 이전 3일 비교
    def _trend(values: list[float]) -> str:
        if len(values) < 4:
            return "혼조" if values else "데이터없음"
        recent = sum(values[:3])
        prev = sum(values[3:6]) if len(values) >= 6 else sum(values[3:])
        # 같은 방향 강화/약화 vs 부호 전환
        if recent > 0 and prev > 0:
            return "강화" if recent > prev else "약화"
        if recent < 0 and prev < 0:
            return "강화" if recent < prev else "약화"
        if (recent > 0 and prev < 0) or (recent < 0 and prev > 0):
            return "반전"
        return "혼조"

    f_trend = _trend(f_vals)

    # 해석: 단위 — daily_eod_loader가 백만원 단위로 적재함.
    # 100 백만원 = 1억, 10000 = 100억. 직관 위해 억/십억 단위로 표기.
    def _fmt_amount(v: float) -> str:
        # v는 백만원 단위
        abs_v = abs(v)
        if abs_v >= 10000:
            return f"{v/10000:+,.1f}십억원"
        if abs_v >= 100:
            return f"{v/100:+,.1f}억원"
        if abs_v >= 1:
            return f"{v:+,.0f}백만원"
        return "변화 미미"

    f_streak_msg = (
        f"{abs(f_streak)}일 연속 {'매수' if f_streak > 0 else '매도'}"
        if f_streak != 0 else "방향 혼조"
    )
    i_streak_msg = (
        f"{abs(i_streak)}일 연속 {'매수' if i_streak > 0 else '매도'}"
        if i_streak != 0 else "방향 혼조"
    )
    interp = (
        f"외인 5일 누적 {_fmt_amount(f_sum)} ({f_streak_msg}, 추세: {f_trend}) | "
        f"기관 5일 누적 {_fmt_amount(i_sum)} ({i_streak_msg})"
    )

    return {
        "foreign_net_5d": round(f_sum, 0),
        "foreign_streak": int(f_streak),
        "foreign_trend": f_trend,
        "inst_net_5d": round(i_sum, 0),
        "inst_streak": int(i_streak),
        "interpretation": interp,
        "days_in_data": len(data),
    }


# ── E. 유관 종목 동조성 ─────────────────────────────────────

def _sector_sync(ticker: str, sector: str | None, date_str: str) -> dict:
    """같은 sector의 어제(가장 최근) 평균 등락 + 종목별 분포.

    Returns:
      {
        "sector": str,
        "peers_count": int,
        "peers_avg_chg": float | None,
        "peers": [{ticker, name, chg}, ...],
        "up_count": int, "down_count": int,
        "sync_strength": "강|중|약|N/A",
        "interpretation": str
      }
    """
    if not sector:
        return {
            "sector": "", "peers_count": 0, "peers_avg_chg": None,
            "peers": [], "up_count": 0, "down_count": 0,
            "sync_strength": "N/A",
            "interpretation": "섹터 정보 없음 — 동조성 분석 불가",
        }

    # 가장 최근 거래일 (date_str 자체 포함 — 어제 종가 = morning_brief 기준 시점)
    rows = fetch_all(
        """
        SELECT ticker, name, chg_pct
        FROM daily_top_value
        WHERE sector = ? AND date = ? AND ticker != ?
        ORDER BY ABS(chg_pct) DESC
        """,
        (sector, date_str, ticker),
    )
    peers = [dict(r) for r in rows]
    if not peers:
        # fallback: 가장 최근 sector 데이터
        latest = fetch_one(
            "SELECT MAX(date) as d FROM daily_top_value WHERE sector = ?",
            (sector,),
        )
        latest_date = latest["d"] if latest else None
        if latest_date:
            rows = fetch_all(
                """
                SELECT ticker, name, chg_pct
                FROM daily_top_value
                WHERE sector = ? AND date = ? AND ticker != ?
                """,
                (sector, latest_date, ticker),
            )
            peers = [dict(r) for r in rows]
        if not peers:
            return {
                "sector": sector, "peers_count": 0, "peers_avg_chg": None,
                "peers": [], "up_count": 0, "down_count": 0,
                "sync_strength": "N/A",
                "interpretation": f"섹터 '{sector}' 다른 종목 거래대금 TOP 100 진입 없음",
            }

    chgs = [float(p.get("chg_pct") or 0) for p in peers]
    avg = sum(chgs) / len(chgs)
    up = sum(1 for c in chgs if c > 0)
    down = sum(1 for c in chgs if c < 0)

    # 동조성: 같은 방향 비율 ≥80% = 강, 60~80 = 중, <60 = 약
    if len(chgs) >= 2:
        sync_ratio = max(up, down) / len(chgs)
        if sync_ratio >= 0.8:
            strength = "강"
        elif sync_ratio >= 0.6:
            strength = "중"
        else:
            strength = "약"
    else:
        strength = "N/A"

    peers_disp = [
        {"ticker": p["ticker"], "name": p["name"] or p["ticker"], "chg": round(float(p.get("chg_pct") or 0), 2)}
        for p in peers[:5]
    ]

    interp = (
        f"섹터 '{sector}' {len(peers)}종목 평균 {avg:+.2f}% "
        f"(상승 {up}/하락 {down}) — 동조 {strength}"
    )

    return {
        "sector": sector,
        "peers_count": len(peers),
        "peers_avg_chg": round(avg, 2),
        "peers": peers_disp,
        "up_count": up,
        "down_count": down,
        "sync_strength": strength,
        "interpretation": interp,
    }


# ── G. 뉴스 매핑 ────────────────────────────────────────────

def _map_news_to_ticker(ticker: str, name: str | None, date_str: str) -> list[dict]:
    """daily_news related_tickers 매칭 + 헤드라인 종목명 fallback.

    Returns: [{headline, category, importance, source}, ...] (importance DESC, 최대 3개)
    """
    # 1) related_tickers JSON에 ticker 포함
    rows = fetch_all(
        """
        SELECT headline, category, importance, source, related_tickers
        FROM daily_news
        WHERE date >= date(?, '-' || ? || ' days')
          AND date <= ?
        ORDER BY importance DESC, date DESC
        """,
        (date_str, _NEWS_LOOKBACK_DAYS, date_str),
    )
    matched: list[dict] = []
    for r in rows:
        d = dict(r)
        rel_raw = d.get("related_tickers") or ""
        is_ticker_match = False
        if rel_raw:
            try:
                rel = json.loads(rel_raw)
                if isinstance(rel, list) and ticker in rel:
                    is_ticker_match = True
            except Exception:
                if ticker in rel_raw:
                    is_ticker_match = True

        # fallback: 헤드라인에 종목명 포함
        is_name_match = False
        if not is_ticker_match and name:
            hl = d.get("headline") or ""
            # 종목명 2글자 이상 매치 (단, 너무 흔한 단어 회피 위해 정확히 검색)
            if len(name) >= 2 and name in hl:
                is_name_match = True

        if is_ticker_match or is_name_match:
            matched.append({
                "headline": d.get("headline") or "",
                "category": d.get("category") or "",
                "importance": int(d.get("importance") or 3),
                "source": d.get("source") or "",
                "match_type": "ticker" if is_ticker_match else "name",
            })

    # importance DESC 정렬 후 상위 3개
    matched.sort(key=lambda x: -x["importance"])
    return matched[:3]


# ── Claude 통합 분석 ─────────────────────────────────────────

_DEEP_SYSTEM_PROMPT = """당신은 한국 단타·스윙 종목 심층 분석 어시스턴트입니다.
4가지 raw 데이터(유사 패턴 백테스트 / 외인·기관 수급 / 섹터 동조성 / 관련 뉴스)와
오늘 추천 종목 정보를 받아 1) 통합 분석 2) 진입 시점 권고 3) 손익 시나리오를 도출합니다.

## 절대 규칙
1. **사실만 기반**: 추정·임의 수치 절대 금지. 입력에 없는 통계·비율·종가 만들지 말 것.
2. **데이터 부족 시 "데이터 부족" 명시**: 누적 일수 적은 백테스트, peers 부족한 동조성 등.
3. **매도 신호 명확**: foreign_net_5d < 0 + sector down_count 우세 + 관련 뉴스 부정적이면 진입 보류 권고.
4. **entry는 추천 정보의 close_price 기준 ±2% 이내**.

## 🟢 한글 우선 — 영어 단어 최소화
사용자 노출 텍스트(summary, entry_timing, scenarios, risk_flags)는 **한국어로만** 작성.
영어 단어는 고유명사(KOSPI/SOXX/NVDA 등)와 약어(RSI/MACD/PER/ETF 등)만 허용.
금지 단어: regime, reversal, weak, strong, sideways, volatile, bullish, bearish, neutral,
breakout, momentum, pullback, rebound, FOMO, confidence 등은 한글로 풀어쓸 것.
JSON enum 필드(verdict)는 영문값 유지 X — 한글 그대로("추천 강화"|"보류"|"기각").

## 응답 형식 (STRICT JSON만 — 코드 펜스/주석 금지)
{
  "verdict": "추천 강화|보류|기각",
  "summary": "<1~2문장 통합 의견 한국어>",
  "entry_timing": "<진입 시점 권고 1줄 — 예: '09:30 이후 눌림 확인 후', '시초가 갭다운 시 분할'>",
  "scenarios": {
    "bull": "<상승 시 익절 % 또는 목표가>",
    "bear": "<하락 시 손절 % 또는 임계가>"
  },
  "risk_flags": ["<리스크 1>", "<리스크 2>"]
}

규칙: 첫 글자 `{` 마지막 글자 `}`. risk_flags 0~3개. 모든 텍스트 한국어."""


def _build_user_prompt(pick: dict, raw: dict) -> str:
    """Claude 입력 메시지 작성."""
    pat = raw["similar_pattern"]
    cap = raw["capital_flow"]
    sec = raw["sector_sync"]
    nw  = raw["news_mapping"]

    def _fmt_pat() -> str:
        if not pat.get("data_available"):
            return f"  사례 0건 — {pat.get('interpretation')}"
        return (
            f"  매칭 케이스: {pat['matched_cases']}건\n"
            f"  익일 평균: {pat['next_day_avg']:+.2f}% / 중앙: {pat['next_day_median']:+.2f}%\n"
            f"  적중률: {pat['win_rate']*100:.0f}%\n"
            f"  최선: {pat['best_case']}\n"
            f"  최악: {pat['worst_case']}"
        )

    def _fmt_news() -> str:
        if not nw:
            return "  (관련 뉴스 없음)"
        return "\n".join(
            f"  ★{n['importance']} [{n['category']}/{n['match_type']}] {n['headline'][:80]} ({n['source']})"
            for n in nw
        )

    def _fmt_peers() -> str:
        if not sec.get("peers"):
            return "  (peers 없음)"
        return "\n".join(
            f"  - {p['name']}({p['ticker']}) {p['chg']:+.2f}%" for p in sec["peers"]
        )

    return f"""## 대상 종목
이름: {pick.get('name')} ({pick.get('ticker')})
신뢰도: {pick.get('confidence')} / 어제 종가: {pick.get('entry')}
선정 사유 (morning_brief): {pick.get('reason', '')[:200]}
손절 권고: {pick.get('stop_loss_pct')}% / 익절 권고: {pick.get('take_profit_pct')}%

## B. 유사 패턴 백테스트 (lookback {_PATTERN_LOOKBACK_DAYS}일)
{_fmt_pat()}

## C. 외인/기관 수급 (lookback {_FLOW_LOOKBACK_DAYS}일, 데이터 {cap.get('days_in_data')}일)
{cap.get('interpretation')}

## E. 섹터 동조성
{sec.get('interpretation')}
{_fmt_peers()}

## G. 관련 뉴스 ({_NEWS_LOOKBACK_DAYS}일 이내, 상위 {len(nw)}건)
{_fmt_news()}

위 4가지 raw 데이터만으로 STRICT JSON 응답 생성."""


def _ask_claude_deep(pick: dict, raw: dict) -> dict:
    """단일 종목 통합 분석."""
    user_content = _build_user_prompt(pick, raw)
    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=900,
            temperature=0,
            timeout=45.0,
            system=[
                {
                    "type": "text",
                    "text": _DEEP_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_content}],
        )
        raw_text = response.content[0].text.strip()
        cleaned = _extract_json(raw_text)
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            return {}
        return parsed
    except Exception as e:
        logger.error(
            f"[deep_analysis] Claude 호출 실패 ({pick.get('ticker')}): "
            f"{type(e).__name__}: {e}"
        )
        check_claude_error(e, "stock_deep_analysis")
        return {}


# ── 메인 진입점 ───────────────────────────────────────────────

def deep_analyze_picks(picks: list[dict], date_str: str) -> list[dict]:
    """추천 picks 중 confidence ≥ _MIN_CONFIDENCE 상위 _MAX_DEEP_PICKS 종목 심층 분석.

    Args:
      picks: morning_brief가 생성한 picks 리스트 (entry는 이미 _validate_picks 통과)
      date_str: 분석 기준 날짜 (예: morning_brief 호출 시점의 오늘)

    Returns:
      picks (원본 유지 + deep 대상 항목에 'deep_analysis' 키 추가)
    """
    if not picks:
        return picks

    # 신뢰도 기준 필터
    qualified = [p for p in picks if int(p.get("confidence") or 0) >= _MIN_CONFIDENCE]
    qualified = qualified[:_MAX_DEEP_PICKS]

    if not qualified:
        logger.info(
            f"[deep_analysis] {date_str} — confidence ≥ {_MIN_CONFIDENCE} 종목 없음, 스킵"
        )
        return picks

    # 분석 기준일: morning_brief는 오늘 아침 → 시그널 데이터는 어제(=가장 최근)에 해당
    # daily_top_value의 가장 최근 date를 시그널 기준으로 사용
    last_row = fetch_one(
        "SELECT MAX(date) AS d FROM daily_top_value"
    )
    sig_date = last_row["d"] if last_row else date_str
    # date 객체 → 문자열로 통일
    if hasattr(sig_date, "isoformat"):
        sig_date = sig_date.isoformat()

    logger.info(
        f"[deep_analysis] {date_str} — 대상 {len(qualified)}종목 / 시그널 기준일={sig_date}"
    )

    for pick in qualified:
        ticker = pick.get("ticker") or ""
        name = pick.get("name") or ticker
        if not ticker:
            continue

        # 시그널 데이터 조회 (sig_date)
        sig_row = fetch_one(
            """
            SELECT ticker, sector, chg_pct, rsi_14, foreign_net_buy, inst_net_buy
            FROM daily_top_value
            WHERE ticker = ? AND date = ?
            """,
            (ticker, sig_date),
        )
        if sig_row:
            sig = dict(sig_row)
            sector = sig.get("sector")
            today_rsi = sig.get("rsi_14")
            today_chg = sig.get("chg_pct")
            f_dir = 1 if (sig.get("foreign_net_buy") or 0) > 0 else (
                -1 if (sig.get("foreign_net_buy") or 0) < 0 else 0
            )
        else:
            # sig_date에 없으면 빈 컨텍스트
            sector = None
            today_rsi = None
            today_chg = None
            f_dir = 0

        # 4가지 raw
        try:
            similar = _backtest_similar_pattern(
                ticker, sector, today_rsi, today_chg, f_dir, sig_date
            )
        except Exception as e:
            logger.warning(f"[deep_analysis] B 백테스트 실패 {ticker}: {e}")
            similar = {"data_available": False, "interpretation": "분석 오류"}

        try:
            flow = _analyze_capital_flow(ticker, sig_date)
        except Exception as e:
            logger.warning(f"[deep_analysis] C 수급 실패 {ticker}: {e}")
            flow = {"interpretation": "분석 오류", "days_in_data": 0}

        try:
            sync = _sector_sync(ticker, sector, sig_date)
        except Exception as e:
            logger.warning(f"[deep_analysis] E 동조 실패 {ticker}: {e}")
            sync = {"interpretation": "분석 오류", "peers": []}

        try:
            news = _map_news_to_ticker(ticker, name, date_str)
        except Exception as e:
            logger.warning(f"[deep_analysis] G 뉴스 실패 {ticker}: {e}")
            news = []

        raw_pack = {
            "similar_pattern": similar,
            "capital_flow":    flow,
            "sector_sync":     sync,
            "news_mapping":    news,
        }

        # Claude 통합 분석
        verdict = _ask_claude_deep(pick, raw_pack)

        pick["deep_analysis"] = {
            "similar_pattern": similar,
            "capital_flow":    flow,
            "sector_sync":     sync,
            "news_mapping":    news,
            "verdict":         verdict,
        }
        logger.info(
            f"[deep_analysis] {name}({ticker}) — "
            f"패턴={similar.get('matched_cases')}건 / "
            f"수급={flow.get('foreign_trend')} / "
            f"동조={sync.get('sync_strength')} / "
            f"뉴스={len(news)}건 / "
            f"verdict={verdict.get('verdict','-')}"
        )

    return picks


# ── 영어 → 한글 후처리 (사용자 노출 텍스트용) ────────────────
_EN_TO_KR_DEEP: dict[str, str] = {
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
    "FOMO":                   "추격 매수 심리",
    "confidence":             "신뢰도",
}
_EN_TO_KR_DEEP_WB: dict[str, str] = {"weak": "약세 국면", "strong": "강세 국면"}


def _kr_postprocess_deep(text: str) -> str:
    """Python의 \\b는 한글을 단어 문자로 인식하므로 lookbehind/lookahead 기반 경계 사용."""
    if not text:
        return text
    out = text
    boundary_l = r"(?<![A-Za-z0-9_])"
    boundary_r = r"(?![A-Za-z0-9_])"
    items = list(_EN_TO_KR_DEEP.items()) + list(_EN_TO_KR_DEEP_WB.items())
    for en, kr in sorted(items, key=lambda x: -len(x[0])):
        out = re.sub(
            rf"{boundary_l}{re.escape(en)}{boundary_r}",
            kr, out, flags=re.IGNORECASE,
        )
    return out


# ── 메시지 포매터 (morning_brief에서 import) ────────────────

def format_deep_analysis_lines(picks: list[dict]) -> list[str]:
    """텔레그램 HTML 메시지용 라인 리스트. picks 중 deep_analysis 키 있는 항목만 포함."""
    lines: list[str] = []
    deep_picks = [p for p in picks if p.get("deep_analysis")]
    if not deep_picks:
        return lines

    lines.append("🔬 <b>심층 분석 (TOP " + str(len(deep_picks)) + ")</b>")
    for idx, p in enumerate(deep_picks, 1):
        da = p["deep_analysis"]
        pat = da.get("similar_pattern") or {}
        flow = da.get("capital_flow") or {}
        sync = da.get("sector_sync") or {}
        news = da.get("news_mapping") or []
        verd = da.get("verdict") or {}

        name = p.get("name") or p.get("ticker") or "?"
        ticker = p.get("ticker") or ""
        lines.append(f"  📊 <b>{idx}. {name}({ticker})</b>")

        # 패턴
        if pat.get("data_available"):
            wr = pat.get("win_rate") or 0.0
            mc = pat.get("matched_cases") or 0
            nda = pat.get("next_day_avg")
            lines.append(
                f"     • 패턴: {mc}건 매칭, 적중 {wr*100:.0f}%, "
                f"익일 평균 {nda:+.2f}%"
            )
        else:
            lines.append(f"     • 패턴: 사례 부족 (lookback {_PATTERN_LOOKBACK_DAYS}일)")

        # 수급
        if flow.get("days_in_data", 0) > 0:
            interp = flow.get("interpretation") or ""
            lines.append(f"     • 수급: {interp[:120]}")
        else:
            lines.append("     • 수급: 데이터 부족")

        # 동조성
        if sync.get("peers_count", 0) > 0:
            avg = sync.get("peers_avg_chg")
            n = sync.get("peers_count")
            strength = sync.get("sync_strength")
            avg_s = f"{avg:+.2f}%" if avg is not None else "N/A"
            lines.append(
                f"     • 동조: {sync.get('sector')} {n}종목 평균 {avg_s} ({strength})"
            )
        else:
            lines.append(f"     • 동조: peers 없음")

        # 뉴스 (상위 1~2건만 메시지 크기 컨트롤)
        if news:
            top = news[0]
            stars = "★" * int(top.get("importance") or 3)
            hl = (top.get("headline") or "")[:60]
            lines.append(f"     • 뉴스: {stars} {hl}")

        # Claude 통합
        if verd:
            v = verd.get("verdict") or "-"
            summary = (verd.get("summary") or "").replace("\n", " ")[:120]
            timing = (verd.get("entry_timing") or "")[:60]
            lines.append(f"     • 판정 <b>{v}</b> — {summary}")
            if timing:
                lines.append(f"     • 진입: {timing}")
            scen = verd.get("scenarios") or {}
            bull = scen.get("bull") or ""
            bear = scen.get("bear") or ""
            if bull or bear:
                lines.append(f"     • 시나리오: ↑ {bull[:40]} / ↓ {bear[:40]}")
        else:
            lines.append("     • 판정: Claude 분석 실패")
    lines.append("")
    # 한글 후처리 — Claude가 영어 단어 남긴 경우 자동 치환
    return [_kr_postprocess_deep(ln) for ln in lines]


# ── CLI (테스트) ──────────────────────────────────────────────

if __name__ == "__main__":
    # 어제 picks 로드해서 모의 실행
    from src.infra.database import fetch_one as _f
    row = _f("SELECT picks FROM morning_briefing WHERE date='2026-05-13'")
    if not row:
        print("morning_briefing 2026-05-13 없음")
    else:
        picks = json.loads(row["picks"] or "[]")
        out = deep_analyze_picks(picks, "2026-05-13")
        for p in out:
            if "deep_analysis" in p:
                print("===", p["name"], p["ticker"], "===")
                print(json.dumps(p["deep_analysis"], ensure_ascii=False, indent=2)[:2000])
