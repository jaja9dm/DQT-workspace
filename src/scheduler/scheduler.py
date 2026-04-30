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

# ── KRX 공휴일 (주말 제외, 대체공휴일 포함) ───────────────────
_KRX_HOLIDAYS: frozenset[str] = frozenset({
    # 2026
    "2026-01-01",  # 신정
    "2026-02-16",  # 설날 연휴
    "2026-02-17",  # 설날
    "2026-02-18",  # 설날 연휴
    "2026-03-02",  # 3·1절 대체공휴일 (3/1이 일요일)
    "2026-05-01",  # 근로자의 날
    "2026-05-05",  # 어린이날
    "2026-05-25",  # 부처님오신날
    "2026-08-17",  # 광복절 대체공휴일 (8/15가 토요일)
    "2026-09-24",  # 추석 연휴
    "2026-09-25",  # 추석
    "2026-10-05",  # 개천절 대체공휴일 (10/3이 토요일)
    "2026-10-09",  # 한글날
    "2026-12-25",  # 성탄절
    # 2025 (과거 데이터·테스트용)
    "2025-01-01",  # 신정
    "2025-01-28",  # 설날 연휴
    "2025-01-29",  # 설날
    "2025-01-30",  # 설날 연휴
    "2025-03-03",  # 3·1절 대체공휴일
    "2025-05-01",  # 근로자의 날
    "2025-05-05",  # 어린이날
    "2025-05-06",  # 어린이날 대체공휴일
    "2025-06-06",  # 현충일
    "2025-08-15",  # 광복절
    "2025-10-03",  # 개천절
    "2025-10-05",  # 추석 연휴
    "2025-10-06",  # 추석
    "2025-10-07",  # 추석 연휴
    "2025-10-08",  # 추석 대체공휴일
    "2025-10-09",  # 한글날
    "2025-12-25",  # 성탄절
})


def is_trading_day(dt: datetime | None = None) -> bool:
    """주말·공휴일이면 False, 거래일이면 True."""
    import pytz
    if dt is None:
        dt = datetime.now(pytz.timezone("Asia/Seoul"))
    if dt.weekday() >= 5:   # 토(5)·일(6)
        return False
    return dt.strftime("%Y-%m-%d") not in _KRX_HOLIDAYS


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

        # 장중/장전 재시작 감지 — 이미 시황 엔진 시간(08:35~15:35)이면 즉시 기동
        import pytz
        _kst = pytz.timezone("Asia/Seoul")
        _now = datetime.now(_kst)
        _hm = _now.hour * 100 + _now.minute
        if is_trading_day(_now) and 835 <= _hm < 1535:
            if _hm < 900:
                # 08:35~08:59: 시황 엔진만 기동
                logger.info(f"장 전 재시작 감지 ({_now.strftime('%H:%M')}) — 시황 엔진 즉시 기동")
                self._start_market_engines()
            else:
                # 09:00~15:35: 전체 엔진 기동
                logger.info(f"장중 재시작 감지 ({_now.strftime('%H:%M')}) — 전체 엔진 즉시 기동")
                # 유니버스가 오늘 것이 없으면 즉시 재구성 (08:50 스케줄 놓친 경우)
                try:
                    from src.infra.universe import UniverseManager
                    _um = UniverseManager()
                    if _um.get_today_count() == 0:
                        logger.info("장중 재시작: 오늘 유니버스 없음 → _pre_market_setup 즉시 실행")
                        self._pre_market_setup()
                except Exception as _e:
                    logger.warning(f"유니버스 확인 오류: {_e}")
                self._start_realtime_engines()

    def stop(self) -> None:
        if getattr(self, "_stopping", False):
            return
        self._stopping = True
        self._stop_event.set()
        self._stop_realtime_engines(notify_market_close=False)  # 장 마감 알림 없이 엔진만 정지
        try:
            self._scheduler.shutdown(wait=False)
        except Exception:
            pass
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

        # 08:35 — 글로벌·국내 시황 엔진 선기동 (오버나이트 요약)
        s.add_job(self._start_market_engines, CronTrigger(
            day_of_week="mon-fri", hour=8, minute=35, timezone="Asia/Seoul"
        ), id="start_market_engines", name="시황 엔진 선기동 (오버나이트 요약)")

        # 장 전 준비 (평일만)
        s.add_job(self._pre_market_setup, CronTrigger(
            day_of_week="mon-fri", hour=8, minute=50, timezone="Asia/Seoul"
        ), id="pre_market_setup", name="장 전 유니버스 재구성")

        # 장 시작 — 거래 엔진 기동 (시황 엔진은 08:35에 이미 기동됨)
        s.add_job(self._start_realtime_engines, CronTrigger(
            day_of_week="mon-fri", hour=9, minute=0, timezone="Asia/Seoul"
        ), id="start_engines", name="실시간 엔진 기동")

        # 9:10 — 장 시작 10분 재점검 (Hot List 강제 트리거 + 매매 재개)
        s.add_job(self._market_open_recheck, CronTrigger(
            day_of_week="mon-fri", hour=9, minute=10, timezone="Asia/Seoul"
        ), id="open_recheck", name="9:10 장 시작 재점검")

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

        # 매매 일지 생성 — docs/trading_journal/journal.md (16:05)
        s.add_job(self._run_trading_journal, CronTrigger(
            day_of_week="mon-fri", hour=16, minute=5, timezone="Asia/Seoul"
        ), id="trading_journal", name="매매 일지 생성")

        # 일일 복기 — 오늘 매매 분석 + 개선점 도출 + Telegram 리포트 (16:15)
        s.add_job(self._run_daily_review, CronTrigger(
            day_of_week="mon-fri", hour=16, minute=15, timezone="Asia/Seoul"
        ), id="daily_review_debrief", name="일일 매매 복기")

        # 자동 파라미터 튜닝 — 복기 결과 기반 수치 자동 조정 (16:25)
        s.add_job(self._run_param_tuning, CronTrigger(
            day_of_week="mon-fri", hour=16, minute=25, timezone="Asia/Seoul"
        ), id="param_tuning", name="자동 파라미터 튜닝")

        # 자동 종료 — 모든 배치 완료 후 프로세스 종료 (16:35)
        s.add_job(self._auto_shutdown, CronTrigger(
            day_of_week="mon-fri", hour=16, minute=35, timezone="Asia/Seoul"
        ), id="auto_shutdown", name="자동 종료")

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
        if not is_trading_day():
            logger.info("장 전 준비 스킵 — 휴장일")
            return
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

            # KOSPI + KRX 섹터 매핑 선제 로드 (장 시작 시 blocking fetch 방지)
            from src.infra.sector_rotation import prefetch as _sector_prefetch
            _sector_prefetch()
            logger.info("섹터 로테이션 캐시 프리페치 시작 (백그라운드)")

            notify(f"🌅 <b>장 전 준비 완료</b>\n유니버스 {count}종목 | 감성 캐시 {deleted}건 정리")
        except Exception as e:
            logger.error(f"장 전 준비 오류: {e}", exc_info=True)

    def _start_market_engines(self) -> None:
        """08:35 — 글로벌·국내 시황 엔진 선기동 (오버나이트 요약 포함)."""
        if not is_trading_day():
            logger.info("시황 엔진 선기동 스킵 — 휴장일")
            return
        logger.info("시황 엔진 선기동 시작 (오버나이트 요약 모드)")
        try:
            from src.teams.global_market.engine import GlobalMarketEngine
            from src.teams.domestic_market.engine import DomesticMarketEngine

            self._global_market = GlobalMarketEngine()
            self._domestic_market = DomesticMarketEngine()

            self._global_market.start(morning_summary=True)
            self._domestic_market.start(morning_summary=True)

            logger.info("시황 엔진 선기동 완료")
        except Exception as e:
            logger.error(f"시황 엔진 선기동 오류: {e}", exc_info=True)

    def _start_realtime_engines(self) -> None:
        """09:00 — 거래 엔진 기동 (시황 엔진은 08:35에 이미 기동됨)."""
        if not is_trading_day():
            logger.info("실시간 엔진 기동 스킵 — 휴장일")
            return
        logger.info("실시간 엔진 기동 시작")
        try:
            from src.teams.domestic_stock.engine import DomesticStockEngine
            from src.teams.risk.engine import RiskEngine
            from src.teams.position_monitor.engine import PositionMonitorEngine
            from src.teams.trading.engine import TradingEngine
            from src.teams.intraday_macd.engine import IntradayMACDEngine

            # 시황 엔진이 아직 없으면 (장중 재시작 등) 여기서도 기동
            if self._global_market is None:
                from src.teams.global_market.engine import GlobalMarketEngine
                self._global_market = GlobalMarketEngine()
                self._global_market.start(morning_summary=True)
            if self._domestic_market is None:
                from src.teams.domestic_market.engine import DomesticMarketEngine
                self._domestic_market = DomesticMarketEngine()
                self._domestic_market.start(morning_summary=True)

            self._domestic_stock = DomesticStockEngine()
            self._risk = RiskEngine()
            self._position_monitor = PositionMonitorEngine()
            self._trading = TradingEngine()
            self._intraday_macd = IntradayMACDEngine()

            self._domestic_stock.start()
            self._risk.start()
            self._position_monitor.start()
            self._trading.start()
            self._intraday_macd.start()

            logger.info("전체 실시간 엔진 기동 완료")
            notify("📈 <b>장 시작</b> — 전체 엔진 활성화")
        except Exception as e:
            logger.error(f"실시간 엔진 기동 오류: {e}", exc_info=True)

    def _market_open_recheck(self) -> None:
        """09:10 — 장 시작 10분 재점검. 오프닝 관망 해제 + Hot List 재스캔."""
        if not is_trading_day():
            return
        logger.info("09:10 장 시작 재점검 실행")
        try:
            # 오프닝 게이트 해제 (이제 무조건 매수 허용)
            if self._trading is not None:
                self._trading.reset_opening_gate()

            # 국내 주식팀 즉시 스캔 (Hot List 갱신)
            if self._domestic_stock is not None:
                self._domestic_stock.run_once()
                logger.info("09:10 Hot List 재스캔 완료")

            # 매매팀 즉시 1회 실행 (갱신된 Hot List 기반 매수 판단)
            if self._trading is not None:
                self._trading.run_once()
                logger.info("09:10 매매팀 재실행 완료")

            # 헬스체크 리포트 — 시스템 상태 텔레그램 요약
            _send_morning_healthcheck(self)

            notify("🔄 <b>[09:10 재점검]</b> Hot List 재스캔 + 매수 재개")
        except Exception as e:
            logger.error(f"09:10 재점검 오류: {e}", exc_info=True)

    def _stop_realtime_engines(self, notify_market_close: bool = True) -> None:
        """실시간 엔진 정지 (역순). 휴장일엔 스킵.

        notify_market_close=True  → 15:35 스케줄 정지 (장 마감 알림 발송)
        notify_market_close=False → 시스템 종료 시 호출 (알림 없이 엔진만 정지)
        """
        if notify_market_close and not is_trading_day():
            return  # 휴장일엔 15:35 스케줄 발동 자체를 무시
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

        if notify_market_close:
            notify("📉 <b>장 마감</b> — 실시간 엔진 정지")

    def _run_report(self) -> None:
        """15:40 — 일일 리포트 생성."""
        if not is_trading_day():
            return
        logger.info("일일 리포트 실행")
        try:
            from src.teams.report.engine import ReportEngine
            ReportEngine().run()
        except Exception as e:
            logger.error(f"리포트 실행 오류: {e}", exc_info=True)

    def _run_trading_journal(self) -> None:
        """16:05 — 매매 일지 생성 (docs/trading_journal/journal.md)."""
        if not is_trading_day():
            return
        logger.info("매매 일지 생성 실행")
        try:
            from scripts.generate_trading_journal import generate
            path = generate()
            logger.info(f"매매 일지 저장: {path}")
        except Exception as e:
            logger.error(f"매매 일지 생성 오류: {e}", exc_info=True)

    def _auto_shutdown(self) -> None:
        """16:35 — 장 마감 후 자동 프로세스 종료."""
        logger.info("자동 종료 시작 (16:35 스케줄)")
        self.stop()

    def _run_param_tuning(self) -> None:
        """17:00 — 자동 파라미터 튜닝 (복기 결과 기반)."""
        if not is_trading_day():
            return
        logger.info("자동 파라미터 튜닝 실행")
        try:
            from src.teams.research.param_tuner import run_param_tuning
            run_param_tuning()
        except Exception as e:
            logger.error(f"파라미터 튜닝 오류: {e}", exc_info=True)

    def _run_daily_review(self) -> None:
        """16:30 — 일일 매매 복기 (오늘 매매 분석 + 개선점 도출)."""
        if not is_trading_day():
            return
        logger.info("일일 복기 실행")
        try:
            from src.teams.review.engine import run_daily_review
            run_daily_review()
        except Exception as e:
            logger.error(f"일일 복기 오류: {e}", exc_info=True)

    def _run_research_daily(self) -> None:
        """16:00 — 연구소 일일 분석."""
        if not is_trading_day():
            return
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


# ──────────────────────────────────────────────────────────────
# 09:10 헬스체크 — 시스템 상태 텔레그램 요약
# ──────────────────────────────────────────────────────────────

def _send_morning_healthcheck(scheduler: "DQTScheduler") -> None:
    """
    매일 09:10에 시스템 정상 동작 여부를 텔레그램으로 요약.
    - 엔진 활성 여부
    - 오늘 Hot List 종목 수
    - MACD 신호 수집 현황
    - 보유 포지션
    """
    from src.infra.database import fetch_one, fetch_all
    from src.utils.notifier import notify
    from datetime import date

    today = str(date.today())
    lines = ["🩺 <b>[09:10 헬스체크]</b>"]

    # 1. 엔진 상태
    engines = {
        "시황": scheduler._global_market,
        "국내주식": scheduler._domestic_stock,
        "매매팀": scheduler._trading,
        "포지션감시": scheduler._position_monitor,
        "MACD": scheduler._intraday_macd,
    }
    alive = [name for name, e in engines.items() if e is not None]
    dead  = [name for name, e in engines.items() if e is None]
    lines.append(f"✅ 엔진: {' · '.join(alive)}" if not dead else
                 f"⚠️ 엔진: {' · '.join(alive)} | 미기동: {' · '.join(dead)}")

    # 2. 오늘 Hot List
    try:
        hl = fetch_all(
            "SELECT DISTINCT ticker FROM hot_list WHERE date(created_at)=? ORDER BY created_at DESC",
            (today,),
        )
        lines.append(f"🔥 Hot List: {len(hl)}종목" + (f" ({', '.join(r['ticker'] for r in hl[:5])})" if hl else ""))
    except Exception:
        lines.append("🔥 Hot List: 조회 실패")

    # 3. MACD 신호 수집 현황
    try:
        macd_row = fetch_one(
            "SELECT COUNT(*) as cnt FROM intraday_macd_signal WHERE date(created_at)=?",
            (today,),
        )
        cnt = macd_row["cnt"] if macd_row else 0
        lines.append(f"📊 MACD 신호: {'정상 수집 중 ' + str(cnt) + '건' if cnt > 0 else '⚠️ 아직 0건 (3분봉 대기 중)'}")
    except Exception:
        lines.append("📊 MACD 신호: 조회 실패")

    # 4. 보유 포지션
    try:
        from src.infra.kis_gateway import KISGateway
        gw = KISGateway()
        bal = gw.get_balance()
        positions = bal.get("positions", [])
        if positions:
            pos_str = " · ".join(
                f"{p.get('name', p['ticker'])}({p['pnl_pct']:+.1f}%)"
                for p in positions[:5]
            )
            lines.append(f"💼 보유: {len(positions)}종목 — {pos_str}")
        else:
            lines.append("💼 보유: 없음 (매수 대기)")
    except Exception:
        lines.append("💼 보유: 조회 실패")

    notify("\n".join(lines))
