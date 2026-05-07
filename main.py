"""
main.py
DQT-workspace 시스템 진입점.

스케줄러 기동 → KIS 게이트웨이 초기화 → DB 초기화 → 스케줄러 루프.
모든 팀 엔진의 기동·정지 타이밍은 DQTScheduler가 관리한다.

직접 실행 시 즉시 엔진을 기동하려면 --now 플래그 사용:
  python main.py --now
"""

import atexit
import faulthandler
import os
import sys
import traceback
from datetime import datetime

from src.config.settings import settings
from src.infra.database import init_db
from src.infra.kis_gateway import KISGateway
from src.utils.logger import get_logger

logger = get_logger("main")

_PID_FILE = os.path.join(os.path.dirname(__file__), "dqt.pid")
_CRASH_LOG = os.path.join(os.path.dirname(__file__), "logs", "crash.log")


def _setup_crash_logger() -> None:
    """처리되지 않은 예외와 segfault를 crash.log에 기록."""
    os.makedirs(os.path.dirname(_CRASH_LOG), exist_ok=True)

    # segfault / abort 시 스택 트레이스 기록
    with open(_CRASH_LOG, "a") as f:
        faulthandler.enable(f)

    def _excepthook(exc_type, exc_value, exc_tb):
        msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        with open(_CRASH_LOG, "a") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"CRASH {datetime.now().isoformat()}  PID={os.getpid()}\n")
            f.write(msg)
        logger.critical(f"비정상 종료: {exc_value}")
        sys.__excepthook__(exc_type, exc_value, exc_tb)

    sys.excepthook = _excepthook


def _acquire_pid_lock() -> None:
    """이미 실행 중인 main.py 인스턴스를 모두 종료하고 PID 파일 획득."""
    import signal
    import subprocess

    my_pid = os.getpid()

    # dqt.pid 파일에 등록된 기존 프로세스 종료
    if os.path.exists(_PID_FILE):
        try:
            pid = int(open(_PID_FILE).read().strip())
            if pid != my_pid:
                os.kill(pid, signal.SIGTERM)
                logger.info(f"기존 인스턴스 종료 요청 (PID {pid})")
        except (ProcessLookupError, PermissionError, ValueError):
            pass

    # pgrep으로 이름이 같은 다른 main.py 프로세스도 모두 종료
    try:
        result = subprocess.run(
            ["pgrep", "-f", "python.*main\\.py"],
            capture_output=True, text=True,
        )
        for pid_str in result.stdout.strip().splitlines():
            pid = int(pid_str.strip())
            if pid != my_pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                    logger.info(f"중복 main.py 인스턴스 종료 (PID {pid})")
                except (ProcessLookupError, PermissionError):
                    pass
    except Exception:
        pass

    with open(_PID_FILE, "w") as f:
        f.write(str(my_pid))
    atexit.register(lambda: os.path.exists(_PID_FILE) and os.remove(_PID_FILE))


def main() -> None:
    _setup_crash_logger()
    _acquire_pid_lock()

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

    # 6. 텔레그램 AI 파트너 봇 기동
    chat_bot = None
    try:
        from src.utils.telegram_chat import TelegramChatBot
        chat_bot = TelegramChatBot()
        chat_bot.start()
        logger.info("텔레그램 AI 파트너 봇 기동 완료")
    except Exception as e:
        logger.warning(f"텔레그램 AI 파트너 봇 기동 실패 (무시하고 계속): {e}")

    # --now 플래그: 장 시간 무관하게 즉시 엔진 기동 (개발·테스트용)
    if "--now" in sys.argv:
        logger.info("--now 플래그: 즉시 엔진 기동")
        scheduler._pre_market_setup()
        scheduler._start_realtime_engines()

    logger.info("스케줄러 대기 중 (Ctrl+C로 종료)")
    scheduler.run_forever()

    # run_forever 반환 = 정상 종료 신호 수신
    if chat_bot is not None:
        chat_bot.stop()
    if os.path.exists(_PID_FILE):
        os.remove(_PID_FILE)
    os._exit(0)


if __name__ == "__main__":
    main()
