"""
scheduler.py — DQT 통합 스케줄러

역할:
  모든 팀 엔진의 기동·정지·배치 실행을 중앙에서 관리한다.
  장 시간 여부를 판단하여 실시간 엔진을 켜고 끄고,
  장 마감 후 배치(리포트팀·연구소)를 자동 실행한다.

APScheduler 기반. 모든 엔진은 이 스케줄러를 통해 기동한다.
main.py는 스케줄러만 시작하면 된다.

스케줄 요약:
  [장 전]
  - 08:50  유니버스 재구성
  - 08:55  감성 캐시 만료 정리
  - 09:00  실시간 엔진 전체 기동 (글로벌·국내 시황·주식·위기·포지션·매매)

  [장 중]
  - 글로벌 시황팀: 1시간 주기 (자체 루프)
  - 국내 시황팀:   30분 주기 (자체 루프)
  - 국내 주식팀:   5분 주기  (자체 루프)
  - 위기 관리팀:   15분 주기 (자체 루프)
  - 포지션 감시:   90초 주기 (자체 루프)
  - 매매팀:        5분 주기  (자체 루프)

  [장 마감]
  - 15:35  실시간 엔진 정지
  - 15:40  리포트팀 실행
  - 16:00  연구소 일일 분석 실행
  - 일요일 16:30  연구소 심층 백테스트 (deep=True)
"""

from __future__ import annotations

import signal
import sys
import threading
import time
from datetime import datetime

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    _HAS_APSCHEDULER = True
except ImportError:
    _HAS_APSCHEDULER = False

from src.config.settings import settings
from src.infra.database import init_db
from src.infra.kis_gateway import KISGateway
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)


class DQTScheduler:
    """DQT 통합 스케줄러."""

    def __init__(self) -> None:
        if not _HAS_APSCHEDULER:
            raise ImportError(
                "APScheduler 미설치 — 'pip install apscheduler' 실행 필요"
            )

        self._scheduler = BackgroundScheduler(timezone="Asia/Seoul")
        self._stop_event = threading.Event()

        # 실시간 엔진 참조 (기동 후 저장)
        self._global_market = None
        self._domestic_market = None
        self._domestic_stock = None
        self._risk = None
        self._position_monitor = None
        self._trading = None
        self._intraday_macd = None

    # ──────────────────────────────────────────
    # 스케줄러 기동
    # ──────────────────────────────────────────

    def start(self) -> None:
        """스케줄 등록 후 스케줄러 기동."""
        logger.info("DQT 스케줄러 초기화")
        self._register_jobs()
        self._scheduler.start()
        logger.info("DQT 스케줄러 시작 완료")
        notify("🚀 <b>DQT 시스템 시작</b>\n스케줄러 기동 완료")

    def stop(self) -> None:
        self._stop_event.set()
        self._stop_realtime_engines()
        self._scheduler.shutdown(wait=False)
        logger.info("DQT 스케줄러 종료")
        notify("🛑 <b>DQT 시스템 종료</b>")

    def run_forever(self) -> None:
        """메인 스레드 블로킹 (Ctrl+C 시 종료)."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        logger.info("메인 루프 대기 중 (Ctrl+C로 종료)")
        try:
            while not self._stop_event.is_set():
                time.sleep(10)
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def _signal_handler(self, signum, frame) -> None:
        logger.info(f"종료 신호 수신 ({signum})")
        self.stop()
        sys.exit(0)

    # ──────────────────────────────────────────
    # 잡 등록
    # ──────────────────────────────────────────

    def _register_jobs(self) -> None:
        s = self._scheduler

        # 장 전 준비 (평일만)
        s.add_job(self._pre_market_setup, CronTrigger(
            day_of_week="mon-fri", hour=8, minute=50, timezone="Asia/Seoul"
        ), id="pre_market_setup", name="장 전 유니버스 재구성")

        # 장 시작 — 실시간 엔진 전체 기동
        s.add_job(self._start_realtime_engines, CronTrigger(
            day_of_week="mon-fri", hour=9, minute=0, timezone="Asia/Seoul"
        ), id="start_engines", name="실시간 엔진 기동")

        # 장 마감 — 실시간 엔진 정지
        s.add_job(self._stop_realtime_engines, CronTrigger(
            day_of_week="mon-fri", hour=15, minute=35, timezone="Asia/Seoul"
        ), id="stop_engines", name="실시간 엔진 정지")

        # 장 마감 후 배치: 리포트팀
        s.add_job(self._run_report, CronTrigger(
            day_of_week="mon-fri", hour=15, minute=40, timezone="Asia/Seoul"
        ), id="daily_report", name="일일 리포트 생성")

        # 장 마감 후 배치: 연구소 일일 분석
        s.add_job(self._run_research_daily, CronTrigger(
            day_of_week="mon-fri", hour=16, minute=0, timezone="Asia/Seoul"
        ), id="research_daily", name="연구소 일일 분석")

        # 연구소 심층 백테스트 (일요일 주 1회)
        s.add_job(self._run_research_deep, CronTrigger(
            day_of_week="sun", hour=16, minute=30, timezone="Asia/Seoul"
        ), id="research_deep", name="연구소 심층 백테스트")

        # 매일 자정: 감성 캐시 만료 정리
        s.add_job(self._purge_sentiment_cache, CronTrigger(
            hour=0, minute=5, timezone="Asia/Seoul"
        ), id="purge_cache", name="감성 캐시 만료 정리")

        logger.info(f"총 {len(s.get_jobs())}개 잡 등록 완료")

    # ──────────────────────────────────────────
    # 잡 콜백
    # ──────────────────────────────────────────

    def _pre_market_setup(self) -> None:
        """08:50 — 유니버스 재구성 + 감성 캐시 정리."""
        logger.info("장 전 준비 시작")
        try:
            from src.infra.universe import UniverseManager
            um = UniverseManager()
            count = um.rebuild()
            um.start_disclosure_watcher()
            logger.info(f"유니버스 재구성 완료: {count}종목")

            from src.infra.sentiment_cache import SentimentCache
            deleted = SentimentCache().purge_expired()
            logger.info(f"감성 캐시 정리: {deleted}건 삭제")
        except Exception as e:
            logger.error(f"장 전 준비 오류: {e}", exc_info=True)

    def _start_realtime_engines(self) -> None:
        """09:00 — 실시간 엔진 전체 기동."""
        logger.info("실시간 엔진 기동 시작")
        try:
            from src.teams.global_market.engine import GlobalMarketEngine
            from src.teams.domestic_market.engine import DomesticMarketEngine
            from src.teams.domestic_stock.engine import DomesticStockEngine
            from src.teams.risk.engine import RiskEngine
            from src.teams.position_monitor.engine import PositionMonitorEngine
            from src.teams.trading.engine import TradingEngine
            from src.teams.intraday_macd.engine import IntradayMACDEngine

            self._global_market = GlobalMarketEngine()
            self._domestic_market = DomesticMarketEngine()
            self._domestic_stock = DomesticStockEngine()
            self._risk = RiskEngine()
            self._position_monitor = PositionMonitorEngine()
            self._trading = TradingEngine()
            self._intraday_macd = IntradayMACDEngine()

            self._global_market.start()
            self._domestic_market.start()
            self._domestic_stock.start()
            self._risk.start()
            self._position_monitor.start()
            self._trading.start()
            self._intraday_macd.start()

            logger.info("전체 실시간 엔진 기동 완료")
            notify("📈 <b>장 시작</b> — 전체 엔진 활성화")
        except Exception as e:
            logger.error(f"실시간 엔진 기동 오류: {e}", exc_info=True)

    def _stop_realtime_engines(self) -> None:
        """15:35 — 실시간 엔진 정지 (역순)."""
        logger.info("실시간 엔진 정지 시작")
        for engine, name in [
            (self._intraday_macd, "장중 MACD 모니터"),
            (self._trading, "매매팀"),
            (self._position_monitor, "포지션 감시"),
            (self._domestic_stock, "국내 주식팀"),
            (self._risk, "위기 관리팀"),
            (self._domestic_market, "국내 시황팀"),
            (self._global_market, "글로벌 시황팀"),
        ]:
            if engine is not None:
                try:
                    engine.stop()
                    logger.info(f"{name} 정지 완료")
                except Exception as e:
                    logger.warning(f"{name} 정지 오류: {e}")

        # 참조 해제
        self._global_market = self._domestic_market = self._domestic_stock = None
        self._risk = self._position_monitor = self._trading = self._intraday_macd = None

        notify("📉 <b>장 마감</b> — 실시간 엔진 정지")

    def _run_report(self) -> None:
        """15:40 — 일일 리포트 생성."""
        logger.info("일일 리포트 실행")
        try:
            from src.teams.report.engine import ReportEngine
            ReportEngine().run()
        except Exception as e:
            logger.error(f"리포트 실행 오류: {e}", exc_info=True)

    def _run_research_daily(self) -> None:
        """16:00 — 연구소 일일 분석."""
        logger.info("연구소 일일 분석 실행")
        try:
            from src.teams.research.engine import ResearchEngine
            ResearchEngine().run(deep=False)
        except Exception as e:
            logger.error(f"연구소 분석 오류: {e}", exc_info=True)

    def _run_research_deep(self) -> None:
        """일요일 16:30 — 연구소 심층 백테스트."""
        logger.info("연구소 심층 백테스트 실행")
        try:
            from src.teams.research.engine import ResearchEngine
            ResearchEngine().run(deep=True)
        except Exception as e:
            logger.error(f"연구소 백테스트 오류: {e}", exc_info=True)

    def _purge_sentiment_cache(self) -> None:
        """자정 — 감성 캐시 만료 정리."""
        try:
            from src.infra.sentiment_cache import SentimentCache
            SentimentCache().purge_expired()
        except Exception as e:
            logger.warning(f"캐시 정리 오류: {e}")

    # ──────────────────────────────────────────
    # 수동 실행 헬퍼 (개발/테스트용)
    # ──────────────────────────────────────────

    def trigger_now(self, job_id: str) -> None:
        """특정 잡을 즉시 실행 (개발/테스트용)."""
        job = self._scheduler.get_job(job_id)
        if job:
            job.func()
            logger.info(f"수동 실행 완료: {job_id}")
        else:
            logger.warning(f"잡 없음: {job_id}")

    def status(self) -> list[dict]:
        """등록된 잡 목록과 다음 실행 시각 반환."""
        return [
            {
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time),
            }
            for job in self._scheduler.get_jobs()
        ]
