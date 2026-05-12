"""⚠️ DEPRECATED (2026-05-12) — 자동 매매 모델에서 사용. 어시스턴트 모델 전환으로 호출되지 않음.
대체: src/teams/research/morning_brief.py (08:45 아침 브리핑).

[원본 docstring]
morning_picker.py — 아침 시초가 매수 종목 최종 결정 (오버나이트 미국장 반영)

실행 시각: 평일 08:50 (장 시작 10분 전 — 시황 엔진 선기동 후, 매매팀 기동 전)
역할:
    - 어제 16:30 evening_selector가 저장한 tomorrow_pick은 미국장 반영 전이라 위험.
    - 본 모듈은 다음 입력을 통합해 오늘 시초가에 적합한 종목 최대 10개를 재선정한다.
        1) daily_market_journal 최근 7거래일 (시계열 트렌드, 거래대금 누적)
        2) global_condition 최신 1행 (오버나이트 미국장: S&P/NASDAQ, USD/KRW, 10Y, key_risks)
        3) 어제 evening_selector 결과 (검증용 대조)

핵심 우선순위:
    - 최근 5일 연속 등장한 종목(누적 수급 강세) 우선
    - NASDAQ/반도체 흐름이 한국 IT/반도체에 미칠 영향
    - USD/KRW 급등 시 수출주 우호 / 급락 시 내수·은행 우호
    - 갭업 +5%↑ 예상 종목은 추격 위험 → 후순위 또는 제외

출력:
    - tomorrow_pick 테이블에 오늘 날짜로 INSERT OR REPLACE (rank 1~10)
    - 텔레그램 알림
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_MAX_PICKS = 10

_SYSTEM_PROMPT = """당신은 한국 주식 단타·스윙 트레이딩 전문가입니다.
최근 7거래일의 한국 시장 데이터(거래대금·섹터·테마 시계열)와 오버나이트 미국장 마감,
그리고 어제 저녁에 잠정 선정된 후보 리스트를 받아 오늘 09:00 시초가 매수에 적합한
한국 종목을 최대 10개까지 우선순위(rank 1~10) 순으로 다시 선정합니다.

## 결정 원칙
1. 최근 5일 연속 거래대금 상위에 등장한 종목 = 누적 수급 강세 → 우선.
2. 미국 NASDAQ/반도체(NVDA·TSM 등) 강세 → 한국 반도체/AI 우대.
3. USD/KRW 급등(+0.8%↑) → 수출주(반도체·자동차·조선) 우대 / 급락(-0.8%↓) → 내수·은행 우대.
4. 미국 10년물 금리 급등 → 성장주 후순위 / 가치주 우대.
5. 어제 evening_selector의 tomorrow_pick과 동일한 종목은 유지 가능.
   다르게 정할 경우 reason에 변경 사유를 명시할 것.
6. 갭업 +5% 이상 예상되는 종목(미국 ADR/관련주 +10%↑ 또는 강한 야간 호재)은
   추격 위험 → 후순위 또는 제외. expected_open_gap_pct에 명시.
7. 글로벌 리스크 점수 7↑ 또는 한국 시장 전망 negative → picks 수 줄이거나 빈 배열.

## 응답 형식 (STRICT JSON만 — 코드 펜스/주석/설명문/trailing comma 금지)
{
  "picks": [
    {
      "rank": 1,
      "ticker": "<6자리 종목코드>",
      "name": "<종목명>",
      "reason": "<선정 이유 1~2줄 한국어>",
      "ref_price": <숫자 또는 null>,
      "expected_open_gap_pct": <예상 갭 %, 숫자>
    }
  ],
  "macro_view": "<오늘 한국 시장에 대한 종합 시각 2~4문장 한국어>"
}

규칙:
- picks 길이: 0~10. 후보 부족하거나 매크로 부정적이면 짧게.
- rank는 1부터 연속 정수.
- ticker는 한국 종목코드 6자리 문자열.
- 첫 글자 `{` 마지막 글자 `}` — 그 외 문자 없음."""


# ─────────────────────────────────────────────
# JSON 추출 헬퍼
# ─────────────────────────────────────────────

def _extract_json(raw: str) -> str:
    text = raw.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if m:
        text = m.group(1)
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]
    text = re.sub(r",(\s*[\]}])", r"\1", text)
    return text


# ─────────────────────────────────────────────
# 데이터 수집
# ─────────────────────────────────────────────

def _fetch_recent_journals(days: int = 7) -> list[dict]:
    """daily_market_journal 최근 N거래일."""
    rows = fetch_all(
        """
        SELECT date, kospi_chg_pct, kosdaq_chg_pct,
               foreign_net_buy, inst_net_buy,
               top30_by_value, sector_scores, notable_themes, summary
        FROM daily_market_journal
        ORDER BY date DESC
        LIMIT ?
        """,
        (days,),
    )
    return [dict(r) for r in rows] if rows else []


def _fetch_latest_global() -> dict | None:
    """global_condition 최신 1행 (오버나이트 미국장)."""
    row = fetch_one(
        """
        SELECT global_risk_score, vix, sp500_change, nasdaq_change,
               usd_krw, wti_oil, us_10y_yield,
               korea_market_outlook, key_events, created_at
        FROM global_condition
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    return dict(row) if row else None


def _fetch_yesterday_picks() -> list[dict]:
    """어제 evening_selector 결과 (오늘 pick_date 인 행)."""
    today = date.today().isoformat()
    rows = fetch_all(
        """
        SELECT rank, ticker, name, reason, ref_price
        FROM tomorrow_pick
        WHERE pick_date = ?
        ORDER BY rank
        """,
        (today,),
    )
    return [dict(r) for r in rows] if rows else []


def _accumulate_ticker_appearances(journals: list[dict]) -> list[dict]:
    """최근 N일 거래대금 TOP30에 등장한 빈도·평균 등락률 집계."""
    counter: dict[str, dict] = {}
    for j in journals:
        try:
            top = json.loads(j.get("top30_by_value") or "[]")
        except Exception:
            top = []
        if not isinstance(top, list):
            continue
        for t in top:
            tk = str(t.get("ticker") or "")
            if not tk:
                continue
            slot = counter.setdefault(
                tk,
                {
                    "ticker": tk,
                    "name": t.get("name") or "",
                    "sector": t.get("sector") or "",
                    "count": 0,
                    "chg_sum": 0.0,
                    "value_sum": 0,
                },
            )
            slot["count"] += 1
            slot["chg_sum"] += float(t.get("chg_pct") or 0)
            slot["value_sum"] += int(t.get("value_krw") or 0)

    out: list[dict] = []
    for s in counter.values():
        n = max(1, s["count"])
        out.append(
            {
                "ticker": s["ticker"],
                "name": s["name"],
                "sector": s["sector"],
                "appearances": s["count"],
                "avg_chg_pct": round(s["chg_sum"] / n, 2),
                "avg_value_eok": round(s["value_sum"] / n / 1e8, 0),
            }
        )
    # 누적 등장 횟수 → 평균 거래대금 → 평균 등락 순
    out.sort(
        key=lambda x: (-x["appearances"], -x["avg_value_eok"], -x["avg_chg_pct"])
    )
    return out[:30]


# ─────────────────────────────────────────────
# Claude 호출
# ─────────────────────────────────────────────

def _ask_claude(
    journals: list[dict],
    cumulative: list[dict],
    global_row: dict | None,
    yesterday_picks: list[dict],
) -> dict:
    """Claude(main) 호출 → picks + macro_view."""

    # 최근 7일 시계열 요약
    journal_lines = []
    for j in journals:
        try:
            themes = json.loads(j.get("notable_themes") or "[]")
        except Exception:
            themes = []
        if not isinstance(themes, list):
            themes = []
        kospi = j.get("kospi_chg_pct")
        kosdaq = j.get("kosdaq_chg_pct")
        kospi_txt = f"KOSPI {kospi:+.2f}%" if kospi is not None else "KOSPI N/A"
        kosdaq_txt = f"KOSDAQ {kosdaq:+.2f}%" if kosdaq is not None else "KOSDAQ N/A"
        fnb = j.get("foreign_net_buy")
        inb = j.get("inst_net_buy")
        fnb_txt = f"외인{fnb:+.0f}억" if fnb is not None else "외인N/A"
        inb_txt = f"기관{inb:+.0f}억" if inb is not None else "기관N/A"
        summary = (j.get("summary") or "").strip()
        if len(summary) > 160:
            summary = summary[:160] + "..."
        journal_lines.append(
            f"[{j['date']}] {kospi_txt} {kosdaq_txt} {fnb_txt} {inb_txt} | "
            f"테마={','.join(themes[:5]) or '-'}\n    요약: {summary or '-'}"
        )

    # 누적 등장 종목 (수급 시계열)
    cum_lines = []
    for i, c in enumerate(cumulative[:25], 1):
        cum_lines.append(
            f"{i}. {c['name']}({c['ticker']}) 등장={c['appearances']}/7일 "
            f"평균등락={c['avg_chg_pct']:+.2f}% 평균거래대금={c['avg_value_eok']:,.0f}억 "
            f"섹터={c['sector'] or '-'}"
        )

    # 오버나이트 미국장
    if global_row:
        try:
            key_events = json.loads(global_row.get("key_events") or "[]")
        except Exception:
            key_events = []
        global_block = (
            f"- 수집 시각: {global_row.get('created_at')}\n"
            f"- 글로벌 리스크 점수: {global_row.get('global_risk_score')}\n"
            f"- 한국 시장 전망: {global_row.get('korea_market_outlook')}\n"
            f"- VIX: {global_row.get('vix')}\n"
            f"- S&P 500: {global_row.get('sp500_change'):+.2f}%\n"
            f"- NASDAQ: {global_row.get('nasdaq_change'):+.2f}%\n"
            f"- USD/KRW: {global_row.get('usd_krw')}\n"
            f"- US 10Y: {global_row.get('us_10y_yield')}%\n"
            f"- WTI: {global_row.get('wti_oil')}\n"
            f"- 주요 리스크/이벤트: {'; '.join(str(e) for e in (key_events or [])[:5]) or '-'}"
        )
    else:
        global_block = "- 데이터 없음"

    # 어제 잠정 선정
    if yesterday_picks:
        yp_lines = [
            f"{p['rank']}. {p['name'] or p['ticker']}({p['ticker']}) "
            f"기준가={p.get('ref_price') or 'N/A'} 이유={p.get('reason') or '-'}"
            for p in yesterday_picks
        ]
        yp_block = "\n".join(yp_lines)
    else:
        yp_block = "(어제 선정 없음 — 신규 선정)"

    user_content = f"""## 최근 7거래일 시장 저널 (오래된→최신은 아래에서 위 방향)
{chr(10).join(journal_lines) if journal_lines else '데이터 없음'}

## 최근 7일 거래대금 누적 등장 종목 (수급 지속성 시그널)
{chr(10).join(cum_lines) if cum_lines else '데이터 없음'}

## 오버나이트 미국장 마감
{global_block}

## 어제 16:30 잠정 선정 (미국장 반영 전 — 참고용)
{yp_block}

시스템 프롬프트 규칙에 따라 오늘 09:00 시초가에 매수 적합한 한국 종목을 최대 10개,
우선순위 순으로 JSON만 출력하세요."""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=2048,
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
        result = json.loads(cleaned)
        if not isinstance(result, dict):
            return {"picks": [], "macro_view": ""}
        picks = result.get("picks") or []
        if not isinstance(picks, list):
            picks = []
        cleaned_picks: list[dict] = []
        for p in picks:
            if not isinstance(p, dict):
                continue
            tk = str(p.get("ticker") or "").strip()
            rk = p.get("rank")
            if not tk or rk is None:
                continue
            try:
                rk_i = int(rk)
            except Exception:
                continue
            cleaned_picks.append(
                {
                    "rank": rk_i,
                    "ticker": tk,
                    "name": str(p.get("name") or ""),
                    "reason": str(p.get("reason") or ""),
                    "ref_price": (
                        float(p["ref_price"])
                        if p.get("ref_price") not in (None, "")
                        else None
                    ),
                    "expected_open_gap_pct": (
                        float(p["expected_open_gap_pct"])
                        if p.get("expected_open_gap_pct") not in (None, "")
                        else None
                    ),
                }
            )
        cleaned_picks.sort(key=lambda x: x["rank"])
        # 중복 ticker 제거 (rank 낮은 것 유지)
        seen: set[str] = set()
        deduped: list[dict] = []
        for p in cleaned_picks:
            if p["ticker"] in seen:
                continue
            seen.add(p["ticker"])
            deduped.append(p)
        # rank 재정렬 (1..N)
        for i, p in enumerate(deduped[:_MAX_PICKS], 1):
            p["rank"] = i
        return {
            "picks": deduped[:_MAX_PICKS],
            "macro_view": str(result.get("macro_view") or "").strip(),
        }
    except Exception as e:
        logger.error(f"[아침 선정] Claude 호출 실패: {type(e).__name__}: {e}")
        return {"picks": [], "macro_view": ""}


# ─────────────────────────────────────────────
# DB 반영 + 알림
# ─────────────────────────────────────────────

def _save_picks(today: str, picks: list[dict]) -> None:
    """tomorrow_pick 테이블에 INSERT OR REPLACE.

    UNIQUE(pick_date, rank) 제약이 있으므로
    오늘 pick_date 의 기존 행은 모두 제거한 뒤 새로 삽입한다.
    """
    execute("DELETE FROM tomorrow_pick WHERE pick_date = ?", (today,))
    for p in picks:
        execute(
            """
            INSERT INTO tomorrow_pick
                (pick_date, rank, ticker, name, reason, ref_price, status, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
            """,
            (
                today,
                int(p["rank"]),
                p["ticker"],
                p.get("name") or "",
                p.get("reason") or "",
                p.get("ref_price"),
            ),
        )


def _notify_result(today: str, picks: list[dict], macro: str) -> None:
    if not picks:
        notify(
            f"🌅 <b>[아침 선정]</b> {today}\n"
            f"오늘 매수 후보 없음 — 매크로 부정 또는 데이터 부족"
            + (f"\n📝 {macro}" if macro else "")
        )
        return

    lines = [f"🌅 <b>[아침 선정 완료]</b> {today} / {len(picks)}종목"]
    for p in picks:
        gap = p.get("expected_open_gap_pct")
        gap_txt = f" 예상갭={gap:+.1f}%" if gap is not None else ""
        lines.append(
            f"  {p['rank']}. {p.get('name') or p['ticker']}({p['ticker']}){gap_txt}"
        )
        if p.get("reason"):
            lines.append(f"     └ {p['reason']}")
    if macro:
        lines.append(f"\n📝 매크로 시각: {macro}")
    notify("\n".join(lines))


# ─────────────────────────────────────────────
# 메인 진입점
# ─────────────────────────────────────────────

def run_morning_pick() -> None:
    """08:50 — 오버나이트 미국장 + 최근 7일 누적 데이터로 오늘 종목 최대 10개 재선정."""
    today = date.today().isoformat()
    logger.info(f"[아침 선정] 시작 — {today}")

    journals = _fetch_recent_journals(days=7)
    if not journals:
        logger.warning("[아침 선정] daily_market_journal 비어있음 — 어제 evening_selector 결과 유지")
        notify(
            "🌅 <b>[아침 선정]</b> 일일 시장 저널 데이터 없음 — "
            "어제 저녁 선정 종목을 그대로 사용합니다."
        )
        return

    cumulative = _accumulate_ticker_appearances(journals)
    global_row = _fetch_latest_global()
    yesterday_picks = _fetch_yesterday_picks()

    result = _ask_claude(journals, cumulative, global_row, yesterday_picks)
    picks = result.get("picks", [])
    macro = result.get("macro_view", "")

    if not picks:
        logger.info("[아침 선정] Claude 선정 0건 — 어제 picks 유지")
        _notify_result(today, [], macro)
        return

    _save_picks(today, picks)
    logger.info(f"[아침 선정] {len(picks)}종목 저장 완료")
    _notify_result(today, picks, macro)
