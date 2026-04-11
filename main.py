"""
main.py
DQT-workspace 시스템 진입점.
KIS 게이트웨이 기동 → DB 초기화 → 각 팀 엔진 순차 시작.
"""

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

    # 3. KIS 게이트웨이 기동 (싱글턴 — 가장 먼저)
    gateway = KISGateway()
    logger.info("KIS 게이트웨이 준비 완료")

    # 4. 팀 엔진 순차 기동

    # 4-1. 종목 유니버스 확정 (가장 먼저 — 국내 팀 전체가 의존)
    from src.infra.universe import UniverseManager
    universe = UniverseManager()
    universe.rebuild()
    universe.start_disclosure_watcher()
    logger.info(f"유니버스 확정: {universe.get_today_count()}종목")

    # 4-2. 글로벌 시황팀
    from src.teams.global_market.engine import GlobalMarketEngine
    global_market = GlobalMarketEngine()
    global_market.start()

    # 4-3. 감성 분석 캐시 초기화 (만료 항목 정리)
    from src.infra.sentiment_cache import SentimentCache
    sentiment_cache = SentimentCache()
    sentiment_cache.purge_expired()

    # 4-4. 국내 시황팀
    from src.teams.domestic_market.engine import DomesticMarketEngine
    domestic_market = DomesticMarketEngine()
    domestic_market.start()

    # 4-5. 국내 주식팀
    from src.teams.domestic_stock.engine import DomesticStockEngine
    domestic_stock = DomesticStockEngine()
    domestic_stock.start()

    # TODO: 구현 완료 시 순서대로 추가
    # from src.teams.risk.engine import RiskEngine
    # from src.teams.position_monitor.engine import PositionMonitorEngine
    # from src.teams.trading.engine import TradingEngine
    # from src.teams.report.engine import ReportEngine
    # from src.teams.research.engine import ResearchEngine

    logger.info("시스템 가동 중 — 글로벌·국내 시황팀·국내 주식팀 활성")

    # 메인 스레드 유지 (엔진들은 daemon 스레드로 실행 중)
    try:
        import time
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("시스템 종료 신호 수신")
        global_market.stop()
        domestic_market.stop()
        domestic_stock.stop()


if __name__ == "__main__":
    main()
