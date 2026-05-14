"""
learnings_consolidator.py — 누적 학습 자동 정리 (2026-05-14)

역할:
  매일 evening_review 발송 직후(17:05) 실행되어 learnings 테이블의 노이즈를 줄인다.

전략:
  1-A. 유사 교훈 자동 통합
       - 같은 카테고리 내 pair에 대해 2-gram char shingle + 4자 이상 단어로
         Jaccard 토큰 유사도를 계산 (news_collector와 동일 알고리즘)
       - 임계값 0.6+ 이면 통합 후보로 보고 Claude Haiku에게 진짜 같은 의미인지
         판단 받는다. 다르면 통합하지 않는다.
       - 통합 결정 시:
         · 새 learning INSERT — category 동일, 평균 confidence,
           times_validated/failed 합산, evidence에 "merged from #X, #Y"
         · 기존 둘 status='merged' archive

  1-B. 저신뢰도 자동 archive
       - times_failed > times_validated × 2 AND times_failed >= 5 → 실패 누적
       - confidence < 0.3 AND created_at < today - 30일 → 오래된 + 저신뢰도
       - status='deprecated'

설계 원칙:
  - 의미 변형 X — Claude가 "확실히 같은 의미"라고 판단하지 않으면 통합 안 함
  - evidence 보존 — 통합 시 양쪽 evidence를 모두 신규 learning에 누적
  - 비용 최소 — Haiku 사용, 후보 pair 0~5건 평균 (5/13 기준)
  - 실패 안전 — 통합 실패해도 원본은 손상되지 않음

함수:
  consolidate_learnings() -> dict
    Returns: {
        "merged": int,            # 신규 통합된 learning 개수
        "archived_pairs": int,    # 통합으로 인해 archive된 교훈 개수 (보통 merged*2)
        "archived_low_conf": int, # 저신뢰도 자동 archive 개수
        "total_active_before": int,
        "total_active_after": int,
        "haiku_calls": int,
        "errors": list[str],
    }
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import check_claude_error

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

# 통합 후보 임계값 (news_collector dedup은 0.5 사용 — learnings는 더 보수적)
_SIM_THRESHOLD = 0.6

# 저신뢰도 archive 기준
_OLD_DAYS = 30           # 30일 경과
_LOW_CONF = 0.3          # 0.3 미만
_FAIL_MIN = 5            # 최소 실패 횟수
_FAIL_RATIO = 2.0        # times_failed > times_validated × 2

_NORM_PATTERN = re.compile(r"[\s\W_]+")


# ── 유사도 계산 (news_collector와 동일 알고리즘) ────────────────


def _normalize_tokens(text: str) -> set[str]:
    """2-gram char shingle + 4자 이상 단어 토큰. 한글 강건."""
    if not text:
        return set()
    t = (text or "").lower().strip()
    cleaned = _NORM_PATTERN.sub("", t)
    shingles: set[str] = set()
    if len(cleaned) >= 2:
        for i in range(len(cleaned) - 1):
            shingles.add(cleaned[i:i + 2])
    for w in re.findall(r"[a-z0-9가-힣]{3,}", t):
        shingles.add(w)
    return shingles


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / len(a | b)


# ── Claude 통합 판단 ────────────────────────────────────────


_MERGE_SYSTEM_PROMPT = """\
당신은 트레이딩 학습 DB 정리자입니다.
두 누적 학습 교훈(같은 카테고리)이 주어졌을 때, 진짜로 같은 의미인지 판단합니다.

판단 기준:
- 핵심 시나리오와 결론이 거의 동일하면 통합 (merge=true)
- 일부 단어만 비슷하고 시나리오·조건·결론이 다르면 유지 (merge=false)
- 한 쪽이 다른 쪽의 일반화/세분화일 때만 통합. 보완 관계(전후 단계, 다른 조건)면 유지

응답은 반드시 JSON 한 줄:
{"merge": true|false, "new_content": "통합 시 새 교훈 한 문장 (한글, 더 명확·일반적). merge=false면 빈 문자열"}

규칙:
- new_content는 두 교훈의 공통 본질을 정확히 표현해야 한다 (의미 왜곡 금지)
- 한 문장으로 작성, 가능하면 200자 이내
- 트레이딩 룰처럼 행동 지침이 명확해야 함"""


def _ask_claude_merge(content_a: str, content_b: str) -> tuple[bool, str]:
    """Claude Haiku에 두 교훈 통합 여부 + 새 본문 받는다.
    Returns: (merge, new_content)
    """
    payload = {"a": content_a, "b": content_b}
    user_msg = (
        "다음 두 교훈을 비교하라.\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    try:
        resp = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=400,
            temperature=0,
            timeout=30.0,
            system=[
                {
                    "type": "text",
                    "text": _MERGE_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        # JSON 추출 — 본문에 ``` 등 부가 텍스트 있으면 첫 { ~ 마지막 } 잘라낸다
        if "{" in raw and "}" in raw:
            raw = raw[raw.index("{"):raw.rindex("}") + 1]
        data = json.loads(raw)
        merge = bool(data.get("merge"))
        new_content = (data.get("new_content") or "").strip()
        if merge and not new_content:
            # 안전장치 — 통합 결정인데 본문 빠지면 거부
            return False, ""
        return merge, new_content
    except Exception as e:
        logger.warning(f"[consolidator] Haiku 통합 판단 실패: {type(e).__name__}: {e}")
        check_claude_error(e, "learnings_consolidator")
        return False, ""


# ── 통합 처리 ───────────────────────────────────────────────


def _build_merged_evidence(
    a: dict, b: dict, sim: float, new_content: str
) -> str:
    """두 learning evidence를 합쳐 신규 JSON 배열로."""
    ev_entries: list[dict] = []
    for src in (a, b):
        src_ev = src.get("evidence") or ""
        try:
            parsed = json.loads(src_ev) if src_ev else []
            if isinstance(parsed, list):
                ev_entries.extend(parsed)
            elif isinstance(parsed, dict):
                ev_entries.append(parsed)
            else:
                ev_entries.append({"raw": str(parsed)})
        except Exception:
            if src_ev:
                ev_entries.append({"raw": str(src_ev)[:300]})
    today_iso = date.today().isoformat()
    ev_entries.append({
        "date": today_iso,
        "observation": (
            f"merged from #{a['id']} + #{b['id']} (jaccard={sim:.2f}); "
            f"new_content: {new_content[:120]}"
        ),
    })
    return json.dumps(ev_entries, ensure_ascii=False)


def _merge_pair(a: dict, b: dict, sim: float, new_content: str) -> int | None:
    """두 learning을 통합한다. Returns: 신규 learning id 또는 None."""
    try:
        cat = a["category"]
        ca = float(a.get("confidence") or 0.5)
        cb = float(b.get("confidence") or 0.5)
        avg_conf = round((ca + cb) / 2.0, 4)
        v_sum = int(a.get("times_validated") or 0) + int(b.get("times_validated") or 0)
        f_sum = int(a.get("times_failed") or 0) + int(b.get("times_failed") or 0)
        evidence_json = _build_merged_evidence(a, b, sim, new_content)
        # applicable_regime — 둘 중 하나라도 있으면 합집합 보존
        ar_set: set[str] = set()
        for src in (a, b):
            ar = src.get("applicable_regime")
            if not ar:
                continue
            try:
                lst = json.loads(ar) if isinstance(ar, str) else ar
                if isinstance(lst, list):
                    ar_set.update(str(x) for x in lst)
            except Exception:
                pass
        ar_json = json.dumps(sorted(ar_set), ensure_ascii=False) if ar_set else None

        today_iso = date.today().isoformat()
        new_id = execute(
            """
            INSERT INTO learnings (
                discovered_at, category, content, evidence,
                confidence, times_validated, times_failed, status,
                applicable_regime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)
            """,
            (today_iso, cat, new_content, evidence_json,
             avg_conf, v_sum, f_sum, ar_json),
        )
        # 기존 둘 status='merged' 처리
        for src in (a, b):
            try:
                execute(
                    """
                    UPDATE learnings
                    SET status = 'merged',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (int(src["id"]),),
                )
            except Exception as e:
                logger.warning(
                    f"[consolidator] merged status UPDATE 실패 [{src['id']}]: {e}"
                )
        logger.info(
            f"[consolidator] merged #{a['id']} + #{b['id']} → #{new_id} "
            f"(category={cat}, conf={avg_conf}, jaccard={sim:.2f})"
        )
        return new_id
    except Exception as e:
        logger.warning(f"[consolidator] merge_pair 실패: {e}")
        return None


# ── 저신뢰도 archive ────────────────────────────────────────


def _archive_low_confidence(today: str) -> int:
    """저신뢰도/실패누적 learnings를 deprecated 처리.
    Returns: archive된 건수.
    """
    archived = 0
    rows = fetch_all(
        """
        SELECT id, content, confidence, times_validated, times_failed,
               COALESCE(discovered_at, created_at) AS born_at
        FROM learnings
        WHERE status = 'active'
        """
    )
    cutoff_old = (datetime.now() - timedelta(days=_OLD_DAYS)).date().isoformat()
    for r in rows:
        lid = int(r["id"])
        v = int(r["times_validated"] or 0)
        f = int(r["times_failed"] or 0)
        conf = float(r["confidence"] or 0.5)
        born = str(r["born_at"] or "")
        should_archive = False
        reason = ""
        # 규칙 1: 실패 누적
        if f >= _FAIL_MIN and f > v * _FAIL_RATIO:
            should_archive = True
            reason = f"failed_累積 (f={f}, v={v})"
        # 규칙 2: 오래된 저신뢰도
        elif conf < _LOW_CONF and born and born < cutoff_old:
            should_archive = True
            reason = f"old_low_conf (conf={conf}, born={born})"

        if not should_archive:
            continue
        try:
            execute(
                """
                UPDATE learnings
                SET status = 'deprecated',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (lid,),
            )
            archived += 1
            logger.info(f"[consolidator] archived #{lid} — {reason}")
        except Exception as e:
            logger.warning(f"[consolidator] archive 실패 [{lid}]: {e}")
    return archived


# ── 메인 ────────────────────────────────────────────────────


def _fetch_active_learnings_full() -> list[dict]:
    rows = fetch_all(
        """
        SELECT id, category, content, evidence, confidence,
               times_validated, times_failed, applicable_regime
        FROM learnings
        WHERE status = 'active'
        ORDER BY category, id
        """
    )
    return [dict(r) for r in rows] if rows else []


def consolidate_learnings() -> dict:
    """누적 학습 자동 정리. 매일 evening_review 직후 호출.

    Returns: 통계 dict (merged, archived_pairs, archived_low_conf, ...)
    """
    today = date.today().isoformat()
    errors: list[str] = []

    before_n = (fetch_one(
        "SELECT COUNT(*) AS n FROM learnings WHERE status='active'"
    ) or {"n": 0})["n"]

    # 1) 유사 후보 pair 추출
    items = _fetch_active_learnings_full()
    if not items:
        return {
            "merged": 0,
            "archived_pairs": 0,
            "archived_low_conf": 0,
            "total_active_before": before_n,
            "total_active_after": before_n,
            "haiku_calls": 0,
            "errors": errors,
        }

    # 카테고리 그룹화
    by_cat: dict[str, list[dict]] = {}
    for it in items:
        cat = it["category"]
        it["_tokens"] = _normalize_tokens(it["content"])
        by_cat.setdefault(cat, []).append(it)

    candidates: list[tuple[dict, dict, float]] = []
    for cat, lst in by_cat.items():
        n = len(lst)
        for i in range(n):
            for j in range(i + 1, n):
                a, b = lst[i], lst[j]
                sim = _jaccard(a["_tokens"], b["_tokens"])
                if sim >= _SIM_THRESHOLD:
                    candidates.append((a, b, sim))

    logger.info(
        f"[consolidator] 통합 후보: {len(candidates)} pair "
        f"(active={before_n}, 카테고리={len(by_cat)})"
    )

    # 2) Claude로 통합 판단 → merge
    merged_new_ids: set[int] = set()
    archived_pairs = 0
    haiku_calls = 0
    used_ids: set[int] = set()    # 한 번 merged된 id는 추가 pair에서 제외

    # 유사도 높은 순으로 정렬 (강한 후보 우선)
    candidates.sort(key=lambda x: -x[2])

    for a, b, sim in candidates:
        if int(a["id"]) in used_ids or int(b["id"]) in used_ids:
            continue
        haiku_calls += 1
        merge, new_content = _ask_claude_merge(a["content"], b["content"])
        if not merge:
            logger.info(
                f"[consolidator] pair #{a['id']} + #{b['id']} (sim={sim:.2f}) → "
                f"Claude: 통합 안 함"
            )
            continue
        new_id = _merge_pair(a, b, sim, new_content)
        if new_id is not None:
            merged_new_ids.add(new_id)
            used_ids.update([int(a["id"]), int(b["id"])])
            archived_pairs += 2

    # 3) 저신뢰도 archive
    archived_low_conf = 0
    try:
        archived_low_conf = _archive_low_confidence(today)
    except Exception as e:
        msg = f"archive_low_confidence 실패: {e}"
        logger.error(f"[consolidator] {msg}")
        errors.append(msg)

    after_n = (fetch_one(
        "SELECT COUNT(*) AS n FROM learnings WHERE status='active'"
    ) or {"n": 0})["n"]

    result = {
        "merged": len(merged_new_ids),
        "archived_pairs": archived_pairs,
        "archived_low_conf": archived_low_conf,
        "total_active_before": before_n,
        "total_active_after": after_n,
        "haiku_calls": haiku_calls,
        "errors": errors,
    }
    logger.info(
        f"[consolidator] 완료 — merged={result['merged']} "
        f"archived_pairs={result['archived_pairs']} "
        f"archived_low_conf={result['archived_low_conf']} "
        f"{before_n}→{after_n} (haiku_calls={haiku_calls})"
    )
    return result


# ── 직접 실행 (수동/테스트) ────────────────────────────────────

if __name__ == "__main__":
    import pprint
    r = consolidate_learnings()
    pprint.pprint(r)
