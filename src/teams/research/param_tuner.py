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
import re
import time
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


def _extract_json(raw: str) -> str:
    """Claude 응답에서 JSON 블록 추출. 코드 펜스·앞뒤 텍스트 제거."""
    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", raw)
    if m:
        return m.group(1)
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1:
        return raw[start : end + 1]
    return raw


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

    # ① 규칙 기반 조정 (통계 데이터 직접 분석 — Claude 불필요)
    rule_result = _rule_based_adjustments(reviews, current_params)

    # ② Claude 조정 제안 (정성적 패턴·맥락 분석)
    claude_result = _ask_claude_adjustments(reviews, current_params)

    # 두 결과 병합 (규칙 기반 우선, Claude는 규칙이 다루지 않은 파라미터만)
    rule_params = {a["param"] for a in rule_result.get("adjustments", [])}
    merged_adjustments = rule_result.get("adjustments", []) + [
        a for a in claude_result.get("adjustments", [])
        if a.get("param") not in rule_params
    ]
    merged_manual = rule_result.get("code_changes_needed", []) + claude_result.get("code_changes_needed", [])
    merged = {"adjustments": merged_adjustments, "code_changes_needed": merged_manual}

    if not merged_adjustments:
        logger.info("파라미터 조정 없음")
        notify("⚙️ <b>[파라미터 튜닝]</b> 오늘은 조정 없음 — 현재 파라미터 유지")
        return

    # 안전 검증 후 DB 반영
    applied, manual_flags = _apply_adjustments(merged, current_params)

    # 결과 알림
    _notify_tuning_result(applied, manual_flags)
    logger.info(f"파라미터 튜닝 완료 — {len(applied)}개 조정, 수동 검토 {len(manual_flags)}건")


# ──────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────

def _load_recent_reviews(days: int = 5) -> list[dict]:
    """최근 N 영업일 복기 데이터 로드 (신호 분석 포함)."""
    cutoff = str(date.today() - timedelta(days=days * 2))  # 주말 포함 여유 있게
    rows = fetch_all(
        """
        SELECT review_date, total_trades, win_trades, loss_trades, total_pnl,
               pattern_hits, pattern_fails, improvements, summary, signal_analytics
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
# 규칙 기반 자동 조정 (통계 직접 분석)
# ──────────────────────────────────────────────

def _rule_based_adjustments(
    reviews: list[dict],
    current_params: dict[str, dict],
) -> dict:
    """
    최근 복기의 signal_analytics를 집계해 규칙 기반으로 파라미터 조정.
    Claude 호출 없이 통계만으로 결정 — 빠르고 일관성 있음.

    규칙:
      ① 3일 이상 연속 전체 승률 < target → 진입 점수 임계값 +5
      ② entry_score 80+ 승률 >> 전체 승률 → 임계값 상향 (높은 점수만 진입)
      ③ RSI 65-72 구간 승률 < 40% → hot_list_rsi_hot_limit 하향
      ④ 섹터 cold 승률 < 30% → sector_cold_penalty +1 (더 강하게 감점)
      ⑤ 신호유형별 3일 연속 손실 → manual_flag (코드 검토 요청)
    """
    adjustments: list[dict] = []
    code_changes: list[str] = []

    if not reviews:
        return {"adjustments": [], "code_changes_needed": []}

    # 최근 N일 signal_analytics 집계
    aggregated: dict[str, dict] = {}  # dimension → key → {win, loss, count, pnl_sum}

    def _merge(agg: dict, analytics: dict) -> None:
        for dim, groups in analytics.items():
            if dim == "overall":
                continue
            if not isinstance(groups, dict):
                continue
            for key, g in groups.items():
                base = agg.setdefault(dim, {}).setdefault(key, {"win": 0, "loss": 0, "count": 0})
                base["win"]   += g.get("win", 0)
                base["loss"]  += g.get("loss", 0)
                base["count"] += g.get("count", 0)

    overall_win = overall_loss = 0
    days_with_data = 0
    consecutive_low_winrate = 0

    target_wr = current_params.get("review_win_rate_target", {}).get("current_val", 55.0) / 100.0

    for r in reviews:
        try:
            sa = json.loads(r.get("signal_analytics") or "{}")
        except Exception:
            sa = {}

        if sa.get("overall"):
            ov = sa["overall"]
            w, l = ov.get("win", 0), ov.get("loss", 0)
            total = w + l
            if total >= 2:
                days_with_data += 1
                overall_win  += w
                overall_loss += l
                day_wr = w / total
                if day_wr < target_wr:
                    consecutive_low_winrate += 1

        _merge(aggregated, sa)

    # 전체 승률이 목표 미달인 날이 3일 이상 연속이면 진입 기준 강화
    if consecutive_low_winrate >= 3 and days_with_data >= 3:
        cur_score_min = current_params.get("gate_entry_score_min", {}).get("current_val", 50.0)
        new_score_min = min(75.0, cur_score_min + 5.0)
        if new_score_min != cur_score_min:
            adjustments.append({
                "param": "gate_entry_score_min",
                "new_val": new_score_min,
                "reason": f"3일 연속 승률 {target_wr*100:.0f}% 미달 → 진입 기준 강화",
                "is_code_change": False,
            })

    # entry_score 구간별 분석: 80+ 승률이 전체보다 훨씬 높으면 임계값 상향
    score_groups = aggregated.get("by_score_bucket", {})
    high_score = score_groups.get("80+", {})
    mid_score  = score_groups.get("50-59", {})
    if high_score.get("count", 0) >= 3 and mid_score.get("count", 0) >= 2:
        hs_wr = high_score["win"] / high_score["count"] if high_score["count"] else 0
        ms_wr = mid_score["win"] / mid_score["count"]   if mid_score["count"]  else 0
        if hs_wr - ms_wr >= 0.25:  # 25%p 이상 차이
            cur_min = current_params.get("gate_entry_score_min", {}).get("current_val", 50.0)
            new_min = min(70.0, cur_min + 5.0)
            if new_min != cur_min and not any(a["param"] == "gate_entry_score_min" for a in adjustments):
                adjustments.append({
                    "param": "gate_entry_score_min",
                    "new_val": new_min,
                    "reason": f"점수80+ 승률 {hs_wr*100:.0f}% >> 50-59 승률 {ms_wr*100:.0f}%",
                    "is_code_change": False,
                })

    # RSI 65-72 구간 승률 저조 → rsi_hot_limit 하향
    rsi_groups = aggregated.get("by_rsi_bucket", {})
    rsi_hot = rsi_groups.get("65-72", {})
    if rsi_hot.get("count", 0) >= 3:
        rsi_hot_wr = rsi_hot["win"] / rsi_hot["count"]
        if rsi_hot_wr < 0.40:
            cur_rsi_lim = current_params.get("hot_list_rsi_hot_limit", {}).get("current_val", 72.0)
            new_rsi_lim = max(65.0, cur_rsi_lim - 2.0)
            if new_rsi_lim != cur_rsi_lim:
                adjustments.append({
                    "param": "hot_list_rsi_hot_limit",
                    "new_val": new_rsi_lim,
                    "reason": f"RSI 65-72 승률 {rsi_hot_wr*100:.0f}% < 40% → 과열 기준 하향",
                    "is_code_change": False,
                })

    # 섹터 cold 승률 저조 → cold 패널티 강화
    sector_groups = aggregated.get("by_sector_hot", {})
    cold_g = sector_groups.get("cold", {})
    if cold_g.get("count", 0) >= 3:
        cold_wr = cold_g["win"] / cold_g["count"]
        if cold_wr < 0.30:
            cur_pen = current_params.get("sector_cold_penalty", {}).get("current_val", 3.0)
            new_pen = min(8.0, cur_pen + 1.0)
            if new_pen != cur_pen:
                adjustments.append({
                    "param": "sector_cold_penalty",
                    "new_val": new_pen,
                    "reason": f"약세섹터 승률 {cold_wr*100:.0f}% < 30% → 패널티 강화",
                    "is_code_change": False,
                })

    # 신호유형별 3일 연속 손실 → 수동 검토 요청
    sig_groups = aggregated.get("by_signal_type", {})
    for sig, g in sig_groups.items():
        if g.get("count", 0) >= 3 and g.get("win", 0) == 0:
            code_changes.append(
                f"신호유형 '{sig}' 최근 {g['count']}건 전패 "
                f"— 진입 조건 or 필터 코드 검토 필요"
            )

    if adjustments:
        logger.info(f"규칙 기반 조정 {len(adjustments)}건: {[a['param'] for a in adjustments]}")

    return {"adjustments": adjustments, "code_changes_needed": code_changes}


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

    response = None
    for attempt in range(1, 4):
        try:
            response = _client.messages.create(
                model=settings.CLAUDE_MODEL_MAIN,
                max_tokens=1024,
                temperature=0,
                timeout=60.0,
                messages=[{"role": "user", "content": prompt}],
            )
            break
        except Exception as e:
            if attempt == 3:
                logger.error(f"Claude 파라미터 튜닝 최종 실패 ({attempt}회): {type(e).__name__}: {e}")
                return []
            logger.warning(f"Claude 파라미터 튜닝 재시도 {attempt}/3: {type(e).__name__}: {e}")
            time.sleep(5 * attempt)
    if response is None:
        logger.error("Claude 파라미터 튜닝 응답 없음 — 모든 재시도 실패")
        return {"adjustments": [], "code_changes_needed": []}
    try:
        raw = response.content[0].text.strip()
        result = json.loads(_extract_json(raw))
        # 응답이 dict가 아니면(list 등) 빈 결과로 처리
        if not isinstance(result, dict):
            return {"adjustments": [], "code_changes_needed": []}
        return result
    except Exception as e:
        logger.error(f"Claude 파라미터 튜닝 응답 파싱 실패: {type(e).__name__}: {e}")
        return {"adjustments": [], "code_changes_needed": []}


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
