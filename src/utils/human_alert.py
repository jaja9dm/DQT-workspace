"""
human_alert.py — 사람 개입 필요 알림 (비정기 텔레그램)

시스템이 스스로 처리하지 못하는 사안 발생 시 사용자(트레이더)에게 즉시 알림.
정기 메시지(아침 브리핑 07:30 / 저녁 회고 17:00)와는 구분되는 *비정기* 알림이다.

원칙:
1. 노이즈 최소화 — "진짜 사람이 봐야 하는 것"만 발송
2. 24시간 내 같은 dedup_key 알림은 1회만 발송 (스팸 방지)
3. severity prefix(🚨/💡/🔧)로 정기 메시지와 시각적 구분
4. DB(system_alerts)에 모든 발송/차단 이력 기록

사용 예:
    from src.utils.human_alert import send_human_alert, AlertSeverity

    send_human_alert(
        AlertSeverity.URGENT,
        "검토 요청 — 평가 기준 자기 모순",
        "evening_review가 새 lesson에서 'confidence=2 + 급등'을 실패로 분류하는 모순 감지.\\n"
        "→ _evaluate_picks() 함수 재검토 필요",
        category="self_contradiction",
        dedup_key="self_contradiction_2026-05-14",
    )

대표 시나리오:
    A. 자기 모순 감지 (evening_review에서 needs_human_review=true)
    B. 데이터 수집 N일 연속 실패 (daily_news/daily_top_value/us_market 등)
    E. 운영 안전망 (launchd 시작 실패, DB 크기 초과, caffeinate 종료 등)
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime
from pathlib import Path

from src.config.settings import settings
from src.infra.database import execute, fetch_one, fetch_all
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)


# ── Severity prefix (정기 메시지의 🌅/🌆와 시각적 구분) ──────
class AlertSeverity:
    URGENT = "URGENT"   # 즉시 검토 권장 — 코드 변경 필요할 수 있음
    INFO   = "INFO"     # 정보성 — 시간 날 때 확인
    OPS    = "OPS"      # 운영 점검 — 환경 이슈

# 이모지 prefix
_SEVERITY_EMOJI = {
    "URGENT": "🚨",
    "INFO":   "💡",
    "OPS":    "🔧",
}
# 라벨 (텔레그램 메시지 헤더용)
_SEVERITY_LABEL = {
    "URGENT": "검토 요청",
    "INFO":   "개선 제안",
    "OPS":    "운영 점검",
}


# ── 공개 API ──────────────────────────────────────────────────

def send_human_alert(
    severity: str,
    title: str,
    body: str,
    category: str,
    dedup_key: str | None = None,
) -> bool:
    """텔레그램 발송 + system_alerts DB 기록.

    Args:
        severity: AlertSeverity.URGENT | INFO | OPS
        title:    한 줄 제목 (≤80자 권장)
        body:     상세 내용 — 사용자가 바로 행동 가능하게 작성
        category: 'self_contradiction' | 'data_failure' | 'ops' | 자유 문자열
        dedup_key: 같은 키로 24h 내 발송 이력 있으면 차단. None=항상 발송.

    Returns:
        True = 발송 성공, False = dedup 차단 or 발송 실패
    """
    sev = severity.upper().strip()
    if sev not in _SEVERITY_EMOJI:
        logger.warning(f"[human_alert] 알 수 없는 severity={severity} → URGENT로 처리")
        sev = "URGENT"

    # 1) dedup 검사 — 24h 내 같은 dedup_key 발송 이력
    if dedup_key:
        try:
            row = fetch_one(
                """
                SELECT id, sent_at FROM system_alerts
                WHERE dedup_key = ?
                  AND sent_at >= datetime('now', '-1 day', 'localtime')
                ORDER BY sent_at DESC LIMIT 1
                """,
                (dedup_key,),
            )
            if row:
                logger.info(
                    f"[human_alert] dedup 차단 — key={dedup_key} 이전 발송 {row['sent_at']}"
                )
                return False
        except Exception as e:
            logger.warning(f"[human_alert] dedup 체크 실패 — 강제 발송 계속: {e}")

    # 2) 메시지 작성
    emoji = _SEVERITY_EMOJI[sev]
    label = _SEVERITY_LABEL[sev]
    now = datetime.now()
    weekday = ['월', '화', '수', '목', '금', '토', '일'][now.weekday()]
    header = f"{emoji} <b>[DQT {label}] {now.strftime('%Y-%m-%d')} ({weekday})</b>"
    separator = "─────────────────────"
    # body 컷 (텔레그램 4096자 한도 대비 안전 마진)
    body_clip = (body or "").strip()
    if len(body_clip) > 3500:
        body_clip = body_clip[:3500] + "\n…[중략]"
    msg = f"{header}\n{separator}\n<b>{title}</b>\n\n{body_clip}\n\n<i>{now.strftime('%H:%M:%S')}</i>"

    # 3) 발송
    sent = False
    try:
        sent = notify(msg)
    except Exception as e:
        logger.error(f"[human_alert] 텔레그램 발송 예외: {e}", exc_info=True)
        sent = False

    # 4) DB 기록 (발송 실패해도 기록 — 디버깅 용도)
    try:
        execute(
            """
            INSERT INTO system_alerts (severity, category, title, body, dedup_key, sent_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (sev, category, title[:200], body_clip, dedup_key, now.isoformat(timespec="seconds")),
        )
    except Exception as e:
        logger.warning(f"[human_alert] system_alerts INSERT 실패: {e}")

    if sent:
        logger.info(f"[human_alert] 발송 — {sev} {category} | {title[:60]}")
    else:
        logger.warning(f"[human_alert] 발송 실패 — {sev} {category} | {title[:60]}")
    return sent


# ════════════════════════════════════════════════════════════════════
# B. 데이터 수집 건강 점검
# ════════════════════════════════════════════════════════════════════

def _count_recent_failures(
    table: str,
    date_col: str,
    *,
    min_rows: int | None = None,
    null_col: str | None = None,
    lookback_days: int = 4,
) -> int:
    """최근 거래일에서 데이터가 "실패" 상태인 연속 일수.

    실패 정의:
      - min_rows 지정: 해당 날짜 row 수 < min_rows
      - null_col 지정: 해당 컬럼이 모두 NULL
      - 둘 다 None: row 수 == 0

    토·일은 카운트에서 제외 (휴장).
    """
    from datetime import date, timedelta
    today = date.today()
    consecutive = 0
    checked = 0
    d = today
    while checked < lookback_days * 2 and consecutive < lookback_days:
        if d.weekday() >= 5:  # 토·일 스킵
            d -= timedelta(days=1)
            continue
        # 오늘은 장중일 수 있으므로 스킵 — 어제부터 카운트
        if d == today:
            d -= timedelta(days=1)
            continue
        date_str = d.isoformat()
        try:
            if null_col:
                row = fetch_one(
                    f"SELECT COUNT(*) AS n, "
                    f"SUM(CASE WHEN {null_col} IS NULL THEN 1 ELSE 0 END) AS nulls "
                    f"FROM {table} WHERE {date_col} = ?",
                    (date_str,),
                )
                if row and row["n"] and row["n"] > 0 and row["nulls"] == row["n"]:
                    consecutive += 1
                else:
                    break
            else:
                row = fetch_one(
                    f"SELECT COUNT(*) AS n FROM {table} WHERE {date_col} = ?",
                    (date_str,),
                )
                n = (row["n"] if row else 0) or 0
                threshold = min_rows if min_rows is not None else 1
                if n < threshold:
                    consecutive += 1
                else:
                    break
        except Exception as e:
            logger.debug(f"[human_alert] {table} 점검 SQL 실패: {e}")
            break
        d -= timedelta(days=1)
        checked += 1
    return consecutive


def check_data_health() -> list[dict]:
    """데이터 수집 건강 점검 — 2일 이상 연속 실패 시 알림 리스트 반환.

    임계값:
      - 1일 실패: 무시 (네트워크 일시 장애 가능)
      - 2일 연속: URGENT
      - 3일 이상: 매일 알림 (dedup_key에 날짜 포함 안 함)
    """
    issues: list[dict] = []
    today_str = datetime.now().strftime("%Y-%m-%d")

    checks = [
        # (테이블, 날짜컬럼, 표시명, min_rows, null_col)
        ("daily_news",      "date", "뉴스 수집",       5,    None),
        ("daily_top_value", "date", "KIS 거래대금 적재", 50,   None),
        ("us_market_daily", "date", "미국 시장 스냅샷", None, None),  # row 0개면 실패
        ("kosdaq_condition", "date", "KOSDAQ 외인 수급", None, "foreign_net_buy"),
    ]
    for table, date_col, name, min_rows, null_col in checks:
        fails = _count_recent_failures(
            table, date_col, min_rows=min_rows, null_col=null_col, lookback_days=4
        )
        if fails >= 2:
            issues.append({
                "severity": AlertSeverity.URGENT,
                "title": f"{name} {fails}일 연속 실패",
                "body": (
                    f"테이블: {table}\n"
                    f"증상: 최근 {fails}거래일 동안 데이터가 임계값 미달.\n"
                    f"  - 기준: "
                    + (f"row 수 < {min_rows}" if min_rows else
                       (f"{null_col} 전부 NULL" if null_col else "row 0개"))
                    + "\n\n"
                    f"권장 조치:\n"
                    f"  1. logs/dqt.log에서 해당 수집 잡 에러 로그 확인\n"
                    f"  2. 외부 API/RSS 가용성 확인 (네트워크/키 만료 여부)\n"
                    f"  3. 필요 시 수동 백필 또는 잡 재실행"
                ),
                "category": "data_failure",
                "dedup_key": f"data_failure_{table}_{today_str}",
            })
        elif fails == 1:
            logger.info(f"[human_alert] {name} 1일 실패 — 일시 장애 가능, 알림 보류")

    return issues


# ════════════════════════════════════════════════════════════════════
# E. 운영 안전망 점검
# ════════════════════════════════════════════════════════════════════

def _dir_size_mb(path: Path) -> float:
    """디렉토리 누적 크기 (MB). 실패 시 -1."""
    try:
        total = 0
        for p in path.rglob("*"):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except OSError:
                    pass
        return total / 1024 / 1024
    except Exception:
        return -1.0


def _caffeinate_alive() -> bool:
    """sleep 방지 caffeinate 프로세스가 살아있는지."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "caffeinate"],
            capture_output=True, text=True, timeout=3,
        )
        return result.returncode == 0 and result.stdout.strip() != ""
    except Exception as e:
        logger.debug(f"[human_alert] caffeinate 점검 실패: {e}")
        return True  # 점검 자체가 실패하면 false-positive 피하기 위해 True 반환


def check_ops_health() -> list[dict]:
    """운영 안전망 점검 — 환경 이슈 알림 리스트 반환."""
    issues: list[dict] = []
    today_str = datetime.now().strftime("%Y-%m-%d")
    # workspace 루트 추정: settings.DB_PATH의 부모의 부모
    try:
        workspace = Path(settings.DB_PATH).resolve().parent.parent
    except Exception:
        workspace = Path(__file__).resolve().parent.parent.parent

    # 1) DB 크기
    try:
        db_size_mb = os.path.getsize(settings.DB_PATH) / 1024 / 1024
        if db_size_mb > 100:
            issues.append({
                "severity": AlertSeverity.OPS,
                "title": f"DB 크기 {db_size_mb:.1f}MB 초과 (한도 100MB)",
                "body": (
                    f"파일: {settings.DB_PATH}\n"
                    f"현재 크기: {db_size_mb:.1f}MB\n\n"
                    f"권장 조치:\n"
                    f"  - 오래된 intraday_candles/fetch_checkpoint VACUUM\n"
                    f"  - 백업 후 일부 테이블 정리"
                ),
                "category": "ops",
                "dedup_key": f"db_size_warning_{today_str}",
            })
    except Exception as e:
        logger.debug(f"[human_alert] DB 크기 점검 실패: {e}")

    # 2) logs 디렉토리 크기
    logs_dir = workspace / "logs"
    if logs_dir.exists():
        logs_mb = _dir_size_mb(logs_dir)
        if logs_mb > 500:
            issues.append({
                "severity": AlertSeverity.OPS,
                "title": f"logs 디렉토리 {logs_mb:.0f}MB 초과 (한도 500MB)",
                "body": (
                    f"경로: {logs_dir}\n"
                    f"현재 크기: {logs_mb:.0f}MB\n\n"
                    f"권장 조치:\n"
                    f"  - 오래된 dqt_YYYYMMDD_*.log / main_*.log 압축 또는 삭제\n"
                    f"  - logrotate 또는 manual cleanup 스크립트 실행"
                ),
                "category": "ops",
                "dedup_key": f"logs_size_warning_{today_str}",
            })

    # 3) caffeinate 프로세스 점검
    if not _caffeinate_alive():
        issues.append({
            "severity": AlertSeverity.OPS,
            "title": "caffeinate 프로세스 종료 감지",
            "body": (
                "Mac sleep 방지 caffeinate가 죽었습니다.\n"
                "장시간 미사용 시 시스템이 sleep에 진입해 스케줄 잡이 멈출 수 있음.\n\n"
                "권장 조치:\n"
                "  - 터미널에서 `caffeinate -i -s &` 실행\n"
                "  - 또는 시스템 재시작 시 자동 실행되도록 launchd 등록 확인"
            ),
            "category": "ops",
            "dedup_key": f"caffeinate_dead_{today_str}",
        })

    # 4) launchd vs 수동 시작 감지 — log에서 오늘 시작 라인 추출
    #    어시스턴트 모드는 17:15 자동 종료 + 다음 영업일 아침 자동 시작이 정상.
    #    실제로 점검 가능한 신호:
    #      - run.log 최근 줄의 시작 시각이 거래일 07:00~08:30 사이면 정상
    #      - 08:30 이후 시작이면 수동/지연 시작 가능성 → 정보성 알림
    try:
        run_log = workspace / "logs" / "run.log"
        if run_log.exists():
            # 마지막 'DQT start' 라인 찾기
            content = run_log.read_text(encoding="utf-8", errors="ignore")
            lines = [ln for ln in content.splitlines() if "DQT start" in ln]
            if lines:
                last = lines[-1]
                # 오늘 시작 여부만 가볍게 체크 — 자세한 시각 파싱은 생략
                # (사용자가 직접 보면 충분히 진단 가능)
                today_token = datetime.now().strftime("%b %e")  # "May 14"
                today_token_alt = datetime.now().strftime("%b %d")
                # 가벼운 검사: 오늘 시작 흔적이 없으면 알림 — 단, 잡이 도는 중이면 의미 X
                # (이 함수는 morning_brief 안에서 호출되므로 시스템 동작 중)
                # → 시작 시각이 09:00 이후이면 "지연 시작"으로 간주
                # 단순히 토큰만 있는지 확인 (false-positive 회피)
                if today_token not in last and today_token_alt not in last:
                    issues.append({
                        "severity": AlertSeverity.OPS,
                        "title": "오늘 시작 로그 없음 — 수동 시작 가능성",
                        "body": (
                            f"logs/run.log 마지막 시작: {last.strip()}\n\n"
                            f"오늘({today_str}) 'DQT start' 라인을 찾지 못했습니다.\n"
                            f"launchd 자동 시작이 실패하고 수동 시작했을 가능성.\n\n"
                            f"권장 조치:\n"
                            f"  - launchctl list | grep dqt 로 등록 상태 확인\n"
                            f"  - 또는 ~/Library/LaunchAgents/ 의 plist 확인"
                        ),
                        "category": "ops",
                        "dedup_key": f"launchd_start_missing_{today_str}",
                    })
    except Exception as e:
        logger.debug(f"[human_alert] launchd 시작 점검 실패: {e}")

    return issues


# ════════════════════════════════════════════════════════════════════
# 통합 진입점 — morning_brief에서 호출
# ════════════════════════════════════════════════════════════════════

def run_health_checks() -> dict:
    """데이터 + 운영 점검을 모두 실행하고 결과 요약 반환.

    morning_brief.run_morning_brief() 시작 부분에서 1회 호출 권장.

    Returns:
        {
            "data_issues": int,
            "ops_issues": int,
            "alerts_sent": int,
            "alerts_blocked": int,
        }
    """
    sent = 0
    blocked = 0

    try:
        data_issues = check_data_health()
    except Exception as e:
        logger.error(f"[human_alert] check_data_health 실패: {e}", exc_info=True)
        data_issues = []

    try:
        ops_issues = check_ops_health()
    except Exception as e:
        logger.error(f"[human_alert] check_ops_health 실패: {e}", exc_info=True)
        ops_issues = []

    for issue in data_issues + ops_issues:
        try:
            ok = send_human_alert(**issue)
            if ok:
                sent += 1
            else:
                blocked += 1
        except Exception as e:
            logger.error(f"[human_alert] 알림 발송 실패: {e}", exc_info=True)

    summary = {
        "data_issues": len(data_issues),
        "ops_issues": len(ops_issues),
        "alerts_sent": sent,
        "alerts_blocked": blocked,
    }
    logger.info(f"[human_alert] 점검 완료 — {summary}")
    return summary
