"""
seed_themes.py — KRX 공식 업종 + 주요 테마 시드 1회 INSERT.

어시스턴트 모델 전환 (2026-05-12) — Phase 3-2 보강 (이슈 #3).

ticker_themes 테이블의 source='news'만 채워지고 'krx'/'manual'은 비어있던 문제 해결.
주요 테마 (반도체/2차전지/조선/방산/바이오/AI/원자력 등)에 대해 대표 종목을 코드 내
상수로 매핑하여 INSERT. 이미 동일 (ticker, theme, source)가 있으면 ON CONFLICT 갱신.

실행:
    venv/bin/python -m scripts.seed_themes
또는
    venv/bin/python scripts/seed_themes.py

idempotent: 여러 번 실행해도 안전 (weight만 갱신).

KRX 공식 업종은 daily_eod_loader가 종목별 sector 컬럼으로 별도 적재하므로
이 스크립트는 '테마'(투자 키워드) 매핑에 집중한다.
"""

from __future__ import annotations

import sys
from pathlib import Path

# 프로젝트 루트 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.infra.database import execute, fetch_one  # noqa: E402
from src.infra.sector_mapper import upsert_theme  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


# ── 시드 테마 매핑 (2026-05-12 기준 시총 상위·인기 테마) ─────────
# 주의: 종목 코드만 정확하면 OK — 이름은 daily_top_value/universe에서 동적으로 결합됨.

_THEME_SEED: dict[str, list[str]] = {
    # 반도체 — 메모리·파운드리·소부장
    "반도체": [
        "005930",  # 삼성전자
        "000660",  # SK하이닉스
        "042700",  # 한미반도체
        "240810",  # 원익IPS
        "140860",  # 파크시스템스
        "058470",  # 리노공업
        "095610",  # 테스
        "036930",  # 주성엔지니어링
        "131970",  # 두산테스나
        "403870",  # HPSP
        "095340",  # ISC
        "108860",  # 셀바스AI (AI반도체 관련)
    ],
    "AI반도체": [
        "005930",  # 삼성전자 (HBM)
        "000660",  # SK하이닉스 (HBM)
        "042700",  # 한미반도체 (TC본더)
        "058470",  # 리노공업
        "403870",  # HPSP
    ],
    # 2차전지 — 셀·소재·장비
    "2차전지": [
        "373220",  # LG에너지솔루션
        "006400",  # 삼성SDI
        "247540",  # 에코프로비엠
        "086520",  # 에코프로
        "066970",  # 엘앤에프
        "003670",  # 포스코퓨처엠
        "121600",  # 나노신소재
        "112610",  # 씨에스윈드 (풍력이지만 2차전지 연결도 있음)
        "058610",  # 에스피지
    ],
    "리튬": [
        "086520",  # 에코프로
        "003670",  # 포스코퓨처엠
    ],
    # 조선 — 슈퍼사이클
    "조선": [
        "009540",  # HD한국조선해양
        "010140",  # 삼성중공업
        "042660",  # 한화오션
        "329180",  # HD현대중공업
        "010620",  # 현대미포조선
        "267250",  # HD현대
    ],
    # 방산
    "방산": [
        "047810",  # 한국항공우주
        "012450",  # 한화에어로스페이스
        "272210",  # 한화시스템
        "079550",  # LIG넥스원
        "064350",  # 현대로템
    ],
    # 원자력 (SMR 포함)
    "원자력": [
        "034020",  # 두산에너빌리티
        "100090",  # 삼강엠앤티
        "267260",  # HD현대일렉트릭
        "010060",  # OCI
    ],
    # 바이오 · 신약
    "바이오": [
        "207940",  # 삼성바이오로직스
        "068270",  # 셀트리온
        "326030",  # SK바이오팜
        "141080",  # 리가켐바이오
        "950160",  # 코오롱티슈진
        "048410",  # 현대바이오
        "196170",  # 알테오젠
        "298380",  # 에이비엘바이오
    ],
    # 로봇 · 자동화
    "로봇": [
        "454910",  # 두산로보틱스
        "277810",  # 레인보우로보틱스
        "056080",  # 유진로봇
        "090710",  # 휴림로봇
        "388720",  # 유일로보틱스
        "319400",  # 현대무벡스
    ],
    # AI · 소프트웨어
    "AI소프트웨어": [
        "035420",  # NAVER
        "035720",  # 카카오
        "018260",  # 삼성에스디에스
        "064400",  # LG씨엔에스
        "108860",  # 셀바스AI
    ],
    # 자동차
    "자동차": [
        "005380",  # 현대차
        "000270",  # 기아
        "012330",  # 현대모비스
        "204320",  # HL만도
        "086280",  # 현대글로비스
        "011210",  # 현대위아
    ],
    # 화장품 · K뷰티
    "화장품": [
        "090430",  # 아모레퍼시픽
        "051900",  # LG생활건강
        "241710",  # 코스메카코리아
        "483650",  # 달바글로벌
    ],
    # 게임
    "게임": [
        "036570",  # 엔씨소프트
        "112040",  # 위메이드
        "251270",  # 넷마블
        "194480",  # 데브시스터즈
        "263750",  # 펄어비스
    ],
    # 금융 (은행·증권)
    "은행": [
        "105560",  # KB금융
        "055550",  # 신한지주
        "086790",  # 하나금융지주
        "316140",  # 우리금융지주
        "138930",  # BNK금융지주
    ],
    "증권": [
        "030210",  # KTB투자증권
        "008560",  # 메리츠금융지주
        "006800",  # 미래에셋증권
        "016360",  # 삼성증권
        "039490",  # 키움증권
    ],
    # 통신 · 미디어
    "통신": [
        "017670",  # SK텔레콤
        "030200",  # KT
        "032640",  # LG유플러스
    ],
    # 에너지 · 화학
    "정유화학": [
        "096770",  # SK이노베이션
        "010950",  # S-Oil
        "011170",  # 롯데케미칼
        "011780",  # 금호석유
        "009830",  # 한화솔루션
    ],
    # 철강 · 건자재
    "철강": [
        "005490",  # POSCO홀딩스
        "004020",  # 현대제철
    ],
}


def seed_themes() -> tuple[int, int]:
    """ticker_themes 테이블에 시드 테마 적재.
    Returns:
        (inserted_count, theme_count)
    """
    inserted = 0
    for theme, tickers in _THEME_SEED.items():
        for tk in tickers:
            tk = str(tk).zfill(6)
            if len(tk) != 6 or not tk.isdigit():
                logger.warning(f"[seed_themes] 잘못된 ticker 형식: {tk}")
                continue
            # source='manual' — krx 공식이 아닌 큐레이션. weight 1.0
            upsert_theme(tk, theme, source="manual", weight=1.0)
            inserted += 1
    return inserted, len(_THEME_SEED)


if __name__ == "__main__":
    n, t = seed_themes()
    total = fetch_one("SELECT COUNT(*) AS cnt FROM ticker_themes")["cnt"]
    print(f"INSERT/UPDATE: {n}건 ({t}개 테마) / 전체 ticker_themes: {total}건")
