"""
logger.py
전 팀 공통 로거.
콘솔 + 파일(logs/dqt.log) 동시 출력.
"""

import logging
import sys
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

_FMT = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str) -> logging.Logger:
    """팀·모듈별 로거 반환. 같은 name으로 호출 시 동일 인스턴스 반환."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # 콘솔 핸들러
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(_FMT, _DATE_FMT))

    # 파일 핸들러 (DEBUG 포함 전체 기록)
    fh = logging.FileHandler(LOG_DIR / "dqt.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(_FMT, _DATE_FMT))

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.propagate = False  # 루트 로거로 전파 차단 (중복 출력 방지)
    return logger
