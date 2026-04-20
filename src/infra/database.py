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
        # strategy_params 초기 시드 (존재하면 건너뜀)
        _seed_params = [
            # param_name                cur    default  min    max    description
            ("initial_stop_pct",        2.0,   2.0,   1.0,   4.0,  "초기 손절선 기준 (%)"),
            ("initial_stop_min_pct",    1.5,   1.5,   0.8,   2.5,  "초기 손절선 하한 (%)"),
            ("initial_stop_max_pct",    3.5,   3.5,   2.0,   5.0,  "초기 손절선 상한 (%)"),
            ("hot_list_min_vol_ratio",  2.0,   2.0,   1.2,   5.0,  "Hot List 최소 거래량 비율"),
            ("hot_list_max_rsi",       82.0,  82.0,  75.0,  90.0,  "Hot List RSI 완전차단 상한 (82↑=극과열)"),
            ("hot_list_rsi_hot_limit", 72.0,  72.0,  65.0,  80.0,  "Hot List RSI 과열 포지션50% 시작선"),
            ("hot_list_min_rsi",       35.0,  35.0,  20.0,  45.0,  "Hot List RSI 붕괴 하한 (35↓=과매도 차단)"),
            ("hot_list_min_obv_slope",  0.0,   0.0,  -1.0,   1.0,  "Hot List OBV기울기 최소값 (0=방향무관)"),
        ]
        for row in _seed_params:
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO strategy_params
                        (param_name, current_val, default_val, min_val, max_val, description, tuned_by)
                    VALUES (?, ?, ?, ?, ?, ?, 'default')
                    """,
                    row,
                )
            except Exception:
                pass

        # 기존 DB 마이그레이션: trailing_stop 동적 파라미터 컬럼 추가
        for col, default in [("trigger_pct", "3.0"), ("floor_pct", "2.5")]:
            try:
                conn.execute(
                    f"ALTER TABLE trailing_stop ADD COLUMN {col} REAL NOT NULL DEFAULT {default}"
                )
            except Exception:
                pass  # 이미 존재하면 무시
        # 기존 DB 마이그레이션: intraday_macd_signal 타임프레임별 개별 신호 컬럼 추가
        for col in ["sig_3m", "sig_5m"]:
            try:
                conn.execute(
                    f"ALTER TABLE intraday_macd_signal ADD COLUMN {col} TEXT NOT NULL DEFAULT 'hold'"
                )
            except Exception:
                pass  # 이미 존재하면 무시
        # 기존 DB 마이그레이션: intraday_candles 테이블 생성
        conn.execute("""
            CREATE TABLE IF NOT EXISTS intraday_candles (
                ticker   TEXT NOT NULL,
                bar_time TEXT NOT NULL,
                open     REAL NOT NULL,
                high     REAL NOT NULL,
                low      REAL NOT NULL,
                close    REAL NOT NULL,
                volume   INTEGER NOT NULL,
                saved_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, bar_time)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_intraday_candles ON intraday_candles(ticker, bar_time DESC)"
        )
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
