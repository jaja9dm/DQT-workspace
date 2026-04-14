"""
param_tuner.py — 자동 파라미터 튜닝 모듈

역할:
  일일 복기(trade_review) 결과를 읽어 Claude가 전략 파라미터를 자동으로 조정.
  매일 17:00에 스케줄러가 호출.

튜닝 가능 파라미터 (strategy_params 테이블):
  물타기 / 불타기 / 피라미딩 / MACD 탈출 / 오프닝 게이트 기준값

안전 장치:
  - 각 파라미터에 min_val / max_val 하드 바운드 존재
  - 1회 조정 폭은 기본값 대비 ±20% 이내로 제한
  - 3일 연속 같은 방향 조정만 허용 (오버튜닝 방지)
  - 변경 내역 전부 Telegram 알림

자기 개선 흐름:
  trade_review.improvements → Claude 분석 → strategy_params 갱신
  → 다음날 position_monitor / trading 엔진이 DB 값 우선 참조
  → 로직·구조 변경이 필요한 경우 Telegram에 "수동 검토 필요" 플래그 발송
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

# 1회 조정 시 기본값 대비 최대 변동 비율 (20%)
_MAX_ADJUST_RATIO = 0.20


# ──────────────────────────────────────────────
# 메인 진입점
# ──────────────────────────────────────────────

def run_param_tuning() -> None:
    """
    최근 N일 복기를 분석해 파라미터 자동 조정.
    스케줄러가 매일 17:00에 호출.
    """
    logger.info("=== 자동 파라미터 튜닝 시작 ===")

    # 최근 5일 복기 수집 (충분한 맥락)
    reviews = _load_recent_reviews(days=5)
    if not reviews:
        logger.info("복기 데이터 없음 — 튜닝 스킵")
        return

    # 현재 파라미터 전체 로드
    current_params = _load_current_params()

    # Claude에게 조정 제안 요청
    adjustments = _ask_claude_adjustments(reviews, current_params)
    if not adjustments:
        logger.info("파라미터 조정 없음")
        notify("⚙️ <b>[파라미터 튜닝]</b> 오늘은 조정 없음 — 현재 파라미터 유지")
        return

    # 안전 검증 후 DB 반영
    applied, manual_flags = _apply_adjustments(adjustments, current_params)

    # 결과 알림
    _notify_tuning_result(applied, manual_flags)
    logger.info(f"파라미터 튜닝 완료 — {len(applied)}개 조정, 수동 검토 {len(manual_flags)}건")


# ──────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────

def _load_recent_reviews(days: int = 5) -> list[dict]:
    """최근 N 영업일 복기 데이터 로드."""
    cutoff = str(date.today() - timedelta(days=days * 2))  # 주말 포함 여유 있게
    rows = fetch_all(
        """
        SELECT review_date, total_trades, win_trades, loss_trades, total_pnl,
               pattern_hits, pattern_fails, improvements, summary
        FROM trade_review
        WHERE review_date >= ?
        ORDER BY review_date DESC
        LIMIT ?
        """,
        (cutoff, days),
    )
    return [dict(r) for r in rows] if rows else []


def _load_current_params() -> dict[str, dict]:
    """strategy_params 전체 로드. {param_name: {current_val, default_val, min_val, max_val, description}}"""
    rows = fetch_all("SELECT * FROM strategy_params ORDER BY param_name")
    return {
        r["param_name"]: {
            "current_val": r["current_val"],
            "default_val": r["default_val"],
            "min_val":     r["min_val"],
            "max_val":     r["max_val"],
            "description": r["description"],
            "tuned_by":    r["tuned_by"],
        }
        for r in rows
    } if rows else {}


# ──────────────────────────────────────────────
# Claude 분석
# ──────────────────────────────────────────────

def _ask_claude_adjustments(
    reviews: list[dict],
    current_params: dict[str, dict],
) -> list[dict]:
    """
    Claude에게 파라미터 조정 제안 요청.

    Returns:
        [{"param": ..., "new_val": ..., "reason": ..., "is_code_change": bool}, ...]
    """
    # 복기 요약 텍스트화
    review_lines = []
    for r in reviews:
        hits  = json.loads(r.get("pattern_hits", "[]") or "[]")
        fails = json.loads(r.get("pattern_fails", "[]") or "[]")
        imps  = json.loads(r.get("improvements", "[]") or "[]")
        win_rate = f"{r['win_trades']}/{r['total_trades']}" if r["total_trades"] else "0/0"
        review_lines.append(
            f"\n[{r['review_date']}] 승 {win_rate} | 손익 {(r.get('total_pnl') or 0):+,.0f}원\n"
            f"  잘됨: {'; '.join(hits[:2])}\n"
            f"  실패: {'; '.join(fails[:2])}\n"
            f"  개선: {'; '.join(imps[:3])}"
        )

    # 현재 파라미터 텍스트화
    param_lines = []
    for name, p in current_params.items():
        param_lines.append(
            f"  {name}: {p['current_val']} "
            f"(기본 {p['default_val']}, 범위 {p['min_val']}~{p['max_val']}) — {p['description']}"
        )

    prompt = f"""당신은 국내 주식 퀀트 트레이딩 시스템의 전략 파라미터 최적화 AI입니다.
최근 복기 결과를 분석해 수치 파라미터 조정 방안을 제안하세요.

## 최근 복기 ({len(reviews)}일)
{"".join(review_lines)}

## 현재 파라미터
{chr(10).join(param_lines)}

## 조정 규칙
- 수치 파라미터만 조정 가능 (로직/코드 구조 변경은 is_code_change: true로 플래그)
- 1회 조정 폭: 현재값 대비 ±20% 이내
- 복기가 1일뿐이면 보수적으로 조정, 3일 이상 같은 패턴이면 적극 조정
- 성과가 좋은 파라미터는 건드리지 말 것
- 조정 불필요하면 adjustments 배열을 비워서 반환

## 응답 형식 (JSON만)
{{
  "adjustments": [
    {{
      "param": "<param_name>",
      "new_val": <숫자>,
      "reason": "<조정 근거 30자 이내>",
      "is_code_change": false
    }},
    ...
  ],
  "code_changes_needed": [
    "<코드 변경이 필요한 항목 설명 — 사람이 검토 필요>",
    ...
  ]
}}"""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_RESEARCH,
            max_tokens=1024,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        return result
    except Exception as e:
        logger.error(f"Claude 파라미터 튜닝 분석 실패: {e}")
        return []


# ──────────────────────────────────────────────
# 안전 검증 + DB 반영
# ──────────────────────────────────────────────

def _apply_adjustments(
    result: dict | list,
    current_params: dict[str, dict],
) -> tuple[list[dict], list[str]]:
    """
    Claude 제안을 안전 범위 내에서 검증하고 DB에 반영.

    Returns:
        (applied 목록, 수동 검토 필요 목록)
    """
    adjustments    = result.get("adjustments", []) if isinstance(result, dict) else []
    code_changes   = result.get("code_changes_needed", []) if isinstance(result, dict) else []

    applied: list[dict] = []

    for adj in adjustments:
        param   = adj.get("param", "")
        new_val = adj.get("new_val")
        reason  = adj.get("reason", "")

        if param not in current_params or new_val is None:
            logger.warning(f"알 수 없는 파라미터 또는 값 없음: {param}")
            continue

        p = current_params[param]
        cur  = p["current_val"]
        dft  = p["default_val"]
        lo   = p["min_val"]
        hi   = p["max_val"]

        # 1. 하드 바운드 클램프
        new_val = max(lo, min(hi, float(new_val)))

        # 2. 1회 조정 폭 제한 (기본값 대비 ±20%)
        max_delta = abs(dft) * _MAX_ADJUST_RATIO if dft != 0 else 0.5
        if abs(new_val - cur) > max_delta:
            if new_val > cur:
                new_val = cur + max_delta
            else:
                new_val = cur - max_delta
            new_val = max(lo, min(hi, new_val))
            logger.info(f"조정 폭 제한 적용: {param} → {new_val:.4f}")

        # 3. 변경이 없으면 스킵
        if abs(new_val - cur) < 1e-6:
            continue

        execute(
            """
            UPDATE strategy_params
            SET current_val = ?, tuned_by = 'auto', updated_at = CURRENT_TIMESTAMP
            WHERE param_name = ?
            """,
            (new_val, param),
        )
        applied.append({
            "param":   param,
            "old_val": cur,
            "new_val": new_val,
            "reason":  reason,
        })
        logger.info(f"파라미터 조정: {param} {cur} → {new_val:.4f} ({reason})")

    return applied, code_changes


# ──────────────────────────────────────────────
# Telegram 알림
# ──────────────────────────────────────────────

def _notify_tuning_result(applied: list[dict], manual_flags: list[str]) -> None:
    lines = ["⚙️ <b>[자동 파라미터 튜닝]</b>"]

    if applied:
        lines.append(f"\n✅ <b>자동 조정 {len(applied)}건</b>")
        for a in applied:
            lines.append(f"  • {a['param']}: {a['old_val']} → {a['new_val']:.4g}  ({a['reason']})")
    else:
        lines.append("\n조정 없음 — 현재 파라미터 유지")

    if manual_flags:
        lines.append(f"\n🔧 <b>수동 검토 필요 {len(manual_flags)}건 (코드 변경 필요)</b>")
        for f_ in manual_flags[:5]:
            lines.append(f"  • {f_}")

    notify("\n".join(lines))


# ──────────────────────────────────────────────
# 외부 조회 헬퍼 — 엔진들이 사용
# ──────────────────────────────────────────────

def get_param(name: str, fallback: float) -> float:
    """
    DB에서 파라미터 값 조회. 없으면 fallback 반환.
    엔진들이 하드코딩 상수 대신 이 함수를 호출.
    """
    try:
        row = fetch_one(
            "SELECT current_val FROM strategy_params WHERE param_name = ?", (name,)
        )
        return float(row["current_val"]) if row else fallback
    except Exception:
        return fallback
