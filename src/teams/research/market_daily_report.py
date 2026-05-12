"""⚠️ DEPRECATED (2026-05-12) — 자동 매매 모델에서 사용. 어시스턴트 모델 전환으로 호출되지 않음.
대체: src/teams/research/daily_eod_loader.py (Phase 4).

[원본 docstring]
market_daily_report.py — 일일 시장 저널 작성 (장 마감 직후 데이터 축적)

실행 시각: 평일 15:35 (장 마감 직후 — 매매팀/시황팀 데이터 fresh)
역할:
    오늘 한국 시장의 핵심 데이터를 daily_market_journal 테이블에 적재.
    다음 날 08:50 morning_picker가 최근 7거래일을 시계열 컨텍스트로 활용한다.

수집 항목:
    1. hot_list 기반 거래대금 TOP 30 (오늘 자정 이후)
    2. market_condition 최신 1행 (KOSPI/KOSDAQ 종가·등락률·외인/기관 순매수)
    3. 섹터별 평균 모멘텀 점수 (sector_strength 우선, 없으면 hot_list 평균)
    4. Claude(haiku) 1회 호출 → notable_themes + 문장형 summary 생성

DB: daily_market_journal (date 기준 UPSERT)
"""

from __future__ import annotations

import json
import re
from datetime import date

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_SYSTEM_PROMPT = """당신은 한국 주식시장의 일일 시황 애널리스트입니다.
오늘 거래대금 TOP 30 종목과 섹터별 점수, KOSPI/KOSDAQ 지수 및 외인·기관 수급을 받아
오늘 시장에서 두드러진 테마를 추출하고 문장형 요약을 작성합니다.

## 응답 형식 (STRICT JSON만 — 코드 펜스/주석/설명문/trailing comma 금지)
{
  "notable_themes": ["<테마1>", "<테마2>", "<테마3>"],
  "summary": "<오늘 시장 흐름 한국어 요약 2~4문장>"
}

규칙:
- notable_themes: 최대 5개. 거래대금 TOP 30과 강세 섹터에 등장한 산업 테마 위주.
  (예: "AI반도체", "조선", "바이오", "원전", "이차전지", "방산", "엔터")
- summary: 지수 흐름·수급·주도 테마를 객관적으로 기술. 매수 추천 금지.
- 첫 글자 `{` 마지막 글자 `}` — 그 외 문자 없음."""


# ─────────────────────────────────────────────
# JSON 추출 헬퍼 (param_tuner._extract_json 패턴)
# ─────────────────────────────────────────────

def _extract_json(raw: str) -> str:
    """Claude 응답에서 JSON 본문만 추출."""
    text = raw.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if m:
        text = m.group(1)
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]
    # trailing comma 제거
    text = re.sub(r",(\s*[\]}])", r"\1", text)
    return text


# ─────────────────────────────────────────────
# 데이터 수집
# ─────────────────────────────────────────────

def _fetch_top30() -> list[dict]:
    """오늘 hot_list 기준 거래대금 TOP 30."""
    rows = fetch_all(
        """
        SELECT ticker, name, sector, trading_value, price_change_pct, momentum_score
        FROM hot_list
        WHERE date(created_at) = date('now', 'localtime')
        ORDER BY trading_value DESC
        LIMIT 30
        """
    )
    out = []
    seen: set[str] = set()
    for r in rows:
        t = r["ticker"]
        if t in seen:
            continue
        seen.add(t)
        out.append(
            {
                "ticker": t,
                "name": r["name"] or "",
                "value_krw": int(r["trading_value"] or 0),
                "chg_pct": float(r["price_change_pct"] or 0),
                "sector": r["sector"] or "",
            }
        )
    return out


def _fetch_market_condition() -> dict:
    """market_condition 최신 1행 → {kospi_*, kosdaq_*, foreign_net_buy, inst_net_buy}.

    주의: market_condition 스키마에는 KOSPI/KOSDAQ 지수 종가·등락률 컬럼이 없다.
    summary 텍스트에 점수·방향만 있으므로 지수 종가는 NULL로 두고
    수급 지표(외인/기관 억원) 만 채운다. (지수 종가 컬럼이 향후 추가되면 여기서 매핑)
    """
    row = fetch_one(
        """
        SELECT market_score, market_direction, foreign_net_buy_bn, institutional_net_buy_bn,
               summary
        FROM market_condition
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    if not row:
        return {
            "kospi_close": None,
            "kospi_chg_pct": None,
            "kosdaq_close": None,
            "kosdaq_chg_pct": None,
            "foreign_net_buy": None,
            "inst_net_buy": None,
            "market_score": None,
            "market_direction": None,
            "market_summary": None,
        }
    return {
        "kospi_close": None,
        "kospi_chg_pct": None,
        "kosdaq_close": None,
        "kosdaq_chg_pct": None,
        "foreign_net_buy": float(row["foreign_net_buy_bn"] or 0) or None,
        "inst_net_buy": float(row["institutional_net_buy_bn"] or 0) or None,
        "market_score": float(row["market_score"]) if row["market_score"] is not None else None,
        "market_direction": row["market_direction"],
        "market_summary": row["summary"],
    }


def _fetch_sector_scores() -> dict[str, float]:
    """섹터별 강세 점수. sector_strength 우선, 없으면 오늘 hot_list 모멘텀 평균."""
    rows = fetch_all(
        """
        SELECT sector, vs_kospi, avg_ret_1d, stock_count
        FROM sector_strength
        ORDER BY vs_kospi DESC
        """
    )
    if rows:
        # vs_kospi 를 1차 점수로 사용
        return {
            (r["sector"] or "기타"): round(float(r["vs_kospi"] or 0), 2)
            for r in rows
        }

    # 폴백: hot_list sector 평균 momentum_score
    rows = fetch_all(
        """
        SELECT sector, AVG(momentum_score) AS avg_mom
        FROM hot_list
        WHERE date(created_at) = date('now','localtime')
          AND sector IS NOT NULL AND sector != ''
        GROUP BY sector
        ORDER BY avg_mom DESC
        """
    )
    return {
        (r["sector"] or "기타"): round(float(r["avg_mom"] or 0), 2)
        for r in rows
    }


# ─────────────────────────────────────────────
# Claude 호출
# ─────────────────────────────────────────────

def _ask_claude(
    top30: list[dict],
    sector_scores: dict[str, float],
    mc: dict,
) -> dict:
    """Claude haiku 호출 → notable_themes + summary."""
    if not top30:
        return {"notable_themes": [], "summary": "오늘 거래대금 데이터 없음 — 시장 요약 불가."}

    # TOP 30 요약 라인 (토큰 절감)
    top_lines = []
    for i, t in enumerate(top30, 1):
        val_eok = (t["value_krw"] or 0) / 1e8
        top_lines.append(
            f"{i}. {t['name']}({t['ticker']}) "
            f"{t['chg_pct']:+.2f}% 거래대금={val_eok:,.0f}억 섹터={t['sector'] or '-'}"
        )

    sector_lines = [
        f"  - {s}: {v:+.2f}" for s, v in list(sector_scores.items())[:15]
    ]

    score_txt = (
        f"{mc.get('market_score'):+.2f}" if mc.get("market_score") is not None else "N/A"
    )
    dir_txt = mc.get("market_direction") or "N/A"
    fnb = mc.get("foreign_net_buy")
    inb = mc.get("inst_net_buy")
    fnb_txt = f"{fnb:+.0f}억" if fnb is not None else "N/A"
    inb_txt = f"{inb:+.0f}억" if inb is not None else "N/A"

    user_content = f"""## 오늘 한국 시장 핵심 데이터
- 시장 점수: {score_txt} ({dir_txt})
- 외국인 순매수: {fnb_txt}
- 기관 순매수: {inb_txt}

## 거래대금 TOP 30
{chr(10).join(top_lines)}

## 섹터별 강도 (상위 15개, vs KOSPI %)
{chr(10).join(sector_lines) if sector_lines else "  - 데이터 없음"}

위 데이터를 보고 시스템 프롬프트 규칙에 따라 JSON만 출력하세요."""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=600,
            temperature=0,
            timeout=30.0,
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
            return {"notable_themes": [], "summary": "Claude 응답 형식 오류"}
        themes = result.get("notable_themes") or []
        if not isinstance(themes, list):
            themes = []
        return {
            "notable_themes": [str(x) for x in themes][:5],
            "summary": str(result.get("summary") or "").strip(),
        }
    except Exception as e:
        logger.error(f"[일일 시장 저널] Claude 호출 실패: {type(e).__name__}: {e}")
        return {"notable_themes": [], "summary": ""}


# ─────────────────────────────────────────────
# 메인 진입점
# ─────────────────────────────────────────────

def run_daily_journal() -> None:
    """15:35 — 오늘의 시장 저널을 daily_market_journal 테이블에 UPSERT."""
    today = date.today().isoformat()
    logger.info(f"[일일 시장 저널] 시작 — {today}")

    top30 = _fetch_top30()
    if not top30:
        logger.warning("[일일 시장 저널] 오늘 hot_list 비어있음 — 저장 스킵")
        return

    mc = _fetch_market_condition()
    sector_scores = _fetch_sector_scores()

    claude_out = _ask_claude(top30, sector_scores, mc)

    execute(
        """
        INSERT INTO daily_market_journal (
            date, kospi_close, kospi_chg_pct, kosdaq_close, kosdaq_chg_pct,
            foreign_net_buy, inst_net_buy,
            top30_by_value, sector_scores, notable_themes, summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            kospi_close     = excluded.kospi_close,
            kospi_chg_pct   = excluded.kospi_chg_pct,
            kosdaq_close    = excluded.kosdaq_close,
            kosdaq_chg_pct  = excluded.kosdaq_chg_pct,
            foreign_net_buy = excluded.foreign_net_buy,
            inst_net_buy    = excluded.inst_net_buy,
            top30_by_value  = excluded.top30_by_value,
            sector_scores   = excluded.sector_scores,
            notable_themes  = excluded.notable_themes,
            summary         = excluded.summary,
            created_at      = CURRENT_TIMESTAMP
        """,
        (
            today,
            mc.get("kospi_close"),
            mc.get("kospi_chg_pct"),
            mc.get("kosdaq_close"),
            mc.get("kosdaq_chg_pct"),
            mc.get("foreign_net_buy"),
            mc.get("inst_net_buy"),
            json.dumps(top30, ensure_ascii=False),
            json.dumps(sector_scores, ensure_ascii=False),
            json.dumps(claude_out.get("notable_themes", []), ensure_ascii=False),
            claude_out.get("summary", ""),
        ),
    )

    themes_str = ", ".join(claude_out.get("notable_themes", [])) or "(없음)"
    logger.info(
        f"[일일 시장 저널] 저장 완료 — TOP30={len(top30)}종목 / "
        f"섹터={len(sector_scores)}개 / 테마=[{themes_str}]"
    )
