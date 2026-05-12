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

## 분석 원칙
1. 적중 정의:
   - 추천 적중 = (confidence>=3 AND chg_pct>0) OR (confidence<=2 AND chg_pct<1.0%)
   - 회피 적중 = chg_pct<0
2. 새 lessons는 구체적·반복 가능해야 함. "외인 -2000억+ 4일 연속 → 다음날 KOSPI 약세 70%" 같은 식.
3. 일반론·시황 단어("주의 필요" 등) 금지.
4. 기존 lessons 중 오늘 시장에서 검증된 것은 validated_ids에, 실패한 것은 failed_ids에 정확한 ID 명시.
5. 카테고리: pattern | sector | macro | avoid | entry_timing | risk

## 응답 형식 (STRICT JSON만 — 코드 펜스/주석/설명문/trailing comma 금지)
{
  "new_lessons": [
    {
      "category": "pattern|sector|macro|avoid|entry_timing|risk",
      "content": "<구체 교훈 1~2줄 한국어>",
      "evidence": "<오늘 어떤 케이스에서 도출됐는지>"
    }
  ],
  "lessons_validated_ids": [<int id>, ...],
  "lessons_failed_ids":    [<int id>, ...],
  "tomorrow_outlook": "<내일 전망 2~4문장>",
  "headline": "<한 줄 요약 — 30자 이내>"
}

규칙:
- 첫 글자 `{` 마지막 글자 `}` — 그 외 문자 없음.
- new_lessons 0~5개. validated/failed 0~10개 각각."""


_TELEGRAM_LIMIT = 4000


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

def _evaluate_picks(picks: list[dict], today_top: list[dict]) -> tuple[list[dict], float]:
    """추천 종목 평가.
    적중 = (conf>=3 AND chg>0) OR (conf<=2 AND chg<1.0)
    """
    chg_map = {r["ticker"]: float(r.get("chg_pct") or 0) for r in today_top}
    results: list[dict] = []
    hits = 0
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
                "hit": None, "note": "no_data",
            })
            continue
        hit = (conf >= 3 and actual > 0) or (conf <= 2 and actual < 1.0)
        if hit:
            hits += 1
        results.append({
            "ticker": tk, "name": p.get("name") or tk,
            "predicted_conf": conf, "actual_chg_pct": actual,
            "hit": hit, "entry": p.get("entry"),
        })
    n = len([r for r in results if r["hit"] is not None])
    acc = round(hits / n * 100, 1) if n else 0.0
    return results, acc


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


def _top10_with_rank_delta(
    today_top: list[dict], prev_top: list[dict]
) -> list[dict]:
    """오늘 TOP 10 + 어제 대비 순위 변화."""
    yesterday_rank: dict[str, int] = {}
    if prev_top:
        latest_date = prev_top[0]["date"] if prev_top else None
        for r in prev_top:
            if r["date"] == latest_date:
                yesterday_rank[r["ticker"]] = r["rank"]
    out = []
    for r in today_top[:10]:
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
    accuracy: float,
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
    for r in picks_results:
        actual = r.get("actual_chg_pct")
        actual_s = f"{actual:+.2f}%" if actual is not None else "N/A"
        hit_s = "적중" if r.get("hit") else ("실패" if r.get("hit") is False else "데이터없음")
        pr_lines.append(
            f"  - {r['name']}({r['ticker']}) conf={r.get('predicted_conf')} "
            f"실제={actual_s} → {hit_s}"
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

## 추천 종목 결과 (적중률 {accuracy:.1f}%)
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
    # 1) 신규 INSERT
    for ls in new_lessons or []:
        cat = (ls.get("category") or "").strip()
        content = (ls.get("content") or "").strip()
        evidence = ls.get("evidence") or ""
        if not cat or not content:
            continue
        try:
            execute(
                """
                INSERT INTO learnings (
                    discovered_at, category, content, evidence,
                    confidence, times_validated, times_failed, status
                ) VALUES (?, ?, ?, ?, 0.5, 0, 0, 'active')
                """,
                (today, cat, content,
                 json.dumps([{"date": today, "observation": evidence}],
                            ensure_ascii=False)),
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

def _fmt_lessons_ids(ids: list[int], learnings_map: dict[int, dict]) -> list[str]:
    """ID 리스트 → "#9 \"앞 35자 미리보기...\"" 형식 문자열 리스트."""
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
                preview = content[:38] + ("…" if len(content) > 38 else "")
        if preview:
            out.append(f"#{iid} \"{preview}\"")
        else:
            out.append(f"#{iid}")
    return out


def _format_message(
    today: str,
    review: dict,
    picks_results: list[dict],
    avoids_results: list[dict],
    accuracy: float,
    accuracy_avoid: float,
    sector_strong: list[dict],
    sector_weak: list[dict],
    top10: list[dict],
    market_row: dict | None,
    kosdaq_row: dict | None,
    learnings: list[dict] | None = None,
) -> str:
    lines: list[str] = []
    lines.append(f"🌆 <b>저녁 회고 — {today}</b>")
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
            lines.append(
                f"  방향: {market_row.get('market_direction') or '-'} "
                f"(score {_fmt(market_row.get('market_score'), '.2f')})"
            )
        if kosdaq_row:
            f_v = kosdaq_row.get('foreign_net_buy')
            i_v = kosdaq_row.get('inst_net_buy')
            f_s = f"{float(f_v):+.0f}억" if (f_v is not None and float(f_v) != 0) else "데이터 없음"
            i_s = f"{float(i_v):+.0f}억" if (i_v is not None and float(i_v) != 0) else "데이터 없음"
            lines.append(f"  KOSDAQ 외인 {f_s} | 기관 {i_s}")
        lines.append("")

    # 추천 결과
    if picks_results:
        lines.append(
            f"⭐ <b>추천 결과 (적중률 {accuracy:.1f}%)</b>"
        )
        for r in picks_results[:5]:
            actual = r.get("actual_chg_pct")
            actual_s = f"{actual:+.2f}%" if actual is not None else "N/A"
            hit_emoji = "✅" if r.get("hit") else ("❌" if r.get("hit") is False else "❓")
            lines.append(
                f"  {hit_emoji} {r['name']}({r['ticker']}) "
                f"conf={r.get('predicted_conf')}  실제 {actual_s}"
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

    msg = "\n".join(lines)
    if len(msg) > _TELEGRAM_LIMIT:
        msg = msg[:_TELEGRAM_LIMIT] + "\n...[truncated]"
    return msg


# ── DB 저장 ──────────────────────────────────────────────────

def _save_review(
    today: str,
    review: dict,
    picks_results: list[dict],
    avoids_results: list[dict],
    accuracy: float,
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
                accuracy,
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

    picks_results, accuracy = _evaluate_picks(picks, today_top)
    avoids_results, accuracy_avoid = _evaluate_avoids(avoids, today_top)

    sector_strong, sector_weak = _sector_strength(today_top)
    top10 = _top10_with_rank_delta(today_top, prev_top)

    # Claude 분석
    review = _ask_claude(
        today=today,
        briefing=briefing,
        picks_results=picks_results,
        avoids_results=avoids_results,
        accuracy=accuracy,
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
        accuracy, accuracy_avoid,
        sector_strong, sector_weak, top10,
        market_row, kosdaq_row,
        learnings=learnings,
    )

    sent = notify(msg)
    if not sent:
        logger.warning("[evening_review] 텔레그램 발송 실패 — DB 저장은 진행")

    _save_review(
        today, review,
        picks_results, avoids_results,
        accuracy, accuracy_avoid,
        sector_strong, sector_weak, top10,
        market_row, kosdaq_row,
        msg, sent,
    )

    logger.info(
        f"[evening_review] 완료 — accuracy={accuracy:.1f}% "
        f"new_lessons={new_lesson_count} sent={sent}"
    )
    return {
        "date": today,
        "accuracy": accuracy,
        "new_lessons": new_lesson_count,
        "sent": sent,
    }


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    r = run_evening_review()
    print(r)
