"""
database.py
공유 DB (SQLite) 연결 및 초기화 모듈.
모든 팀이 이 모듈을 통해 DB에 접근한다.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from src.config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_DB_PATH = Path(settings.DB_PATH)
_SCHEMA_PATH = Path(__file__).parent.parent.parent / "db" / "schema.sql"


def init_db() -> None:
    """DB 파일 생성 및 스키마 적용. 시스템 시작 시 1회 호출."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    with get_conn() as conn:
        conn.executescript(schema_sql)
    logger.info(f"DB 초기화 완료: {_DB_PATH}")


@contextmanager
def get_conn():
    """
    SQLite 연결 컨텍스트 매니저.

    Usage:
        with get_conn() as conn:
            conn.execute("SELECT ...")
    """
    conn = sqlite3.connect(
        str(_DB_PATH),
        timeout=10,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.row_factory = sqlite3.Row  # 딕셔너리처럼 컬럼명으로 접근 가능
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_one(sql: str, params: tuple = ()) -> sqlite3.Row | None:
    """단일 행 조회."""
    with get_conn() as conn:
        return conn.execute(sql, params).fetchone()


def fetch_all(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    """복수 행 조회."""
    with get_conn() as conn:
        return conn.execute(sql, params).fetchall()


def execute(sql: str, params: tuple = ()) -> int:
    """INSERT / UPDATE / DELETE 실행. 마지막 삽입 rowid 반환."""
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        return cur.lastrowid
