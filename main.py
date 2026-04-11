"""
main.py
DQT-workspace 시스템 진입점.

스케줄러 기동 → KIS 게이트웨이 초기화 → DB 초기화 → 스케줄러 루프.
모든 팀 엔진의 기동·정지 타이밍은 DQTScheduler가 관리한다.

직접 실행 시 즉시 엔진을 기동하려면 --now 플래그 사용:
  python main.py --now
"""

import sys

from src.config.settings import settings
from src.infra.database import init_db
from src.infra.kis_gateway import KISGateway
from src.utils.logger import get_logger

logger = get_logger("main")


def main() -> None:
    logger.info("=" * 60)
    logger.info("DQT-workspace 시스템 시작")
    logger.info(f"KIS 모드: {settings.KIS_MODE.upper()}")
    logger.info("=" * 60)

    # 1. 환경 변수 검증
    settings.validate()

    # 2. DB 초기화 (스키마 적용)
    init_db()

    # 3. KIS 게이트웨이 기동 (싱글턴 — 모든 팀이 공유)
    KISGateway()
    logger.info("KIS 게이트웨이 준비 완료")

    # 4. 연구소 전략 초기화 (DB에 기본 전략 없으면 삽입)
    from src.teams.research.engine import _init_strategies
    _init_strategies()

    # 5. 스케줄러 기동
    from src.scheduler.scheduler import DQTScheduler
    scheduler = DQTScheduler()
    scheduler.start()

    # --now 플래그: 장 시간 무관하게 즉시 엔진 기동 (개발·테스트용)
    if "--now" in sys.argv:
        logger.info("--now 플래그: 즉시 엔진 기동")
        scheduler._pre_market_setup()
        scheduler._start_realtime_engines()

    logger.info("스케줄러 대기 중 (Ctrl+C로 종료)")
    scheduler.run_forever()


if __name__ == "__main__":
    main()
