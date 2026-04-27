"""
sector_rotation.py — 섹터 로테이션 감지 & KOSPI 상대강도 기반 데이터 제공

2단계 시스템:
  1단계 — 상대강도(RS): 종목 등락률 vs KOSPI 등락률 차이
  2단계 — 섹터 로테이션: 어느 업종으로 수급이 이동 중인지 감지

제공 함수:
    get_kospi_daily_chg()          -> float   # KOSPI 당일 등락률(%)
    get_kospi_5d_ret()             -> float   # KOSPI 5일 수익률(%)
    get_sector(ticker)             -> str     # 종목 업종명
    inject_scan_results(snapshots) -> None    # 스캔 후 섹터 강도 갱신
    get_sector_vs_kospi(sector)    -> float   # 섹터 vs KOSPI 초과수익(%)
    get_hot_sectors(n)             -> list    # 강세 섹터 상위 n개
    get_cold_sectors(n)            -> list    # 약세 섹터 하위 n개
    prefetch()                     -> None    # 장 시작 전 선제 로드
"""

from __future__ import annotations

import threading
from datetime import date, datetime, timedelta
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


class SectorRotationCache:
    """섹터 로테이션 & 상대강도 싱글턴 캐시."""

    _instance: Optional["SectorRotationCache"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "SectorRotationCache":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._data_lock = threading.RLock()
        self._fetched_date: Optional[date] = None

        # 섹터 매핑: ticker → sector 이름
        self._sector_map: dict[str, str] = {}

        # KOSPI 최근 데이터
        self._kospi_daily_chg: float = 0.0   # 당일 등락률 (%)
        self._kospi_5d_ret:    float = 0.0   # 5일 수익률 (%)

        # 섹터별 강도: sector → (avg_ret_1d, vs_kospi)
        self._sector_strength: dict[str, dict] = {}

    # ── 공개 API ──────────────────────────────────────────────

    def get_kospi_daily_chg(self) -> float:
        """KOSPI 당일 등락률 (%). 미로드 시 0.0."""
        self._ensure_fresh()
        with self._data_lock:
            return self._kospi_daily_chg

    def get_kospi_5d_ret(self) -> float:
        """KOSPI 5일 누적 수익률 (%)."""
        self._ensure_fresh()
        with self._data_lock:
            return self._kospi_5d_ret

    def get_sector(self, ticker: str) -> str:
        """종목 업종명. 미매핑 종목은 '기타' 반환."""
        self._ensure_fresh()
        with self._data_lock:
            return self._sector_map.get(ticker, "기타")

    def inject_scan_results(self, snapshots: list) -> None:
        """
        유니버스 스캔 완료 후 섹터별 평균 등락률 계산.
        domestic_stock/engine.py의 run_once() 종료 직전에 호출.

        Args:
            snapshots: StockSnapshot 리스트 (change_pct, sector 포함)
        """
        from collections import defaultdict
        sector_returns: dict[str, list[float]] = defaultdict(list)

        for snap in snapshots:
            if getattr(snap, "error", False):
                continue
            chg    = getattr(snap, "change_pct", 0.0) or 0.0
            sector = self.get_sector(snap.ticker) or "기타"
            sector_returns[sector].append(chg)

        kospi_chg = self.get_kospi_daily_chg()
        new_strength: dict[str, dict] = {}
        for sector, returns in sector_returns.items():
            if len(returns) < 2:   # 샘플 부족이면 노이즈
                continue
            avg_ret = sum(returns) / len(returns)
            vs_kospi = avg_ret - kospi_chg
            new_strength[sector] = {
                "avg_ret_1d": round(avg_ret, 3),
                "vs_kospi":   round(vs_kospi, 3),
                "count":      len(returns),
            }

        with self._data_lock:
            self._sector_strength = new_strength

        # DB에도 저장 (trading engine이 읽음)
        self._save_sector_strength(new_strength)

        hot = self.get_hot_sectors(3)
        cold = self.get_cold_sectors(3)
        logger.info(
            f"섹터 로테이션 갱신 — 강세: {hot} | 약세: {cold}"
        )

    def get_sector_vs_kospi(self, sector: str) -> float:
        """섹터의 KOSPI 대비 초과수익률 (%). 없으면 0.0."""
        with self._data_lock:
            return self._sector_strength.get(sector, {}).get("vs_kospi", 0.0)

    def get_hot_sectors(self, n: int = 3) -> list[str]:
        """KOSPI 대비 초과수익 상위 n개 섹터."""
        with self._data_lock:
            ranked = sorted(
                self._sector_strength.items(),
                key=lambda x: x[1]["vs_kospi"],
                reverse=True,
            )
        return [s for s, _ in ranked[:n]]

    def get_cold_sectors(self, n: int = 3) -> list[str]:
        """KOSPI 대비 초과수익 하위 n개 섹터."""
        with self._data_lock:
            ranked = sorted(
                self._sector_strength.items(),
                key=lambda x: x[1]["vs_kospi"],
            )
        return [s for s, _ in ranked[:n]]

    def prefetch(self) -> None:
        """장 시작 전 백그라운드 선제 로드."""
        t = threading.Thread(target=self._fetch, daemon=True, name="sector-prefetch")
        t.start()

    # ── 내부 로직 ──────────────────────────────────────────────

    def _ensure_fresh(self) -> None:
        today = date.today()
        with self._data_lock:
            already = self._fetched_date == today and bool(self._sector_map)
        if not already:
            self._fetch()

    def _fetch(self) -> None:
        """KOSPI 일봉 + KRX 종목 업종 일괄 로드."""
        self._fetch_kospi()
        self._fetch_sector_map()
        with self._data_lock:
            self._fetched_date = date.today()

    def _fetch_kospi(self) -> None:
        """KOSPI 최근 10일 데이터로 당일 등락률 & 5일 수익률 계산."""
        try:
            import FinanceDataReader as fdr
            end   = datetime.now().date()
            start = end - timedelta(days=20)
            df = fdr.DataReader("KS11", start, end)
            if df is None or len(df) < 2:
                return

            close = df["Close"].astype(float)
            daily_chg = float((close.iloc[-1] / close.iloc[-2] - 1) * 100)
            ret_5d    = float((close.iloc[-1] / close.iloc[-min(6, len(close))] - 1) * 100)

            with self._data_lock:
                self._kospi_daily_chg = round(daily_chg, 3)
                self._kospi_5d_ret    = round(ret_5d, 3)

            logger.debug(f"KOSPI 로드 — 당일 {daily_chg:+.2f}% | 5일 {ret_5d:+.2f}%")
        except Exception as e:
            logger.warning(f"KOSPI 로드 실패: {e}")

    def _fetch_sector_map(self) -> None:
        """KRX 전종목 업종 정보 로드 (FDR StockListing)."""
        try:
            import FinanceDataReader as fdr
            df = fdr.StockListing("KRX")
            if df is None or df.empty:
                return

            # FDR StockListing 컬럼: Code/Symbol, Name, Sector, Industry 등 버전마다 다름
            code_col   = next((c for c in ["Code", "Symbol", "ISU_SRT_CD"] if c in df.columns), None)
            sector_col = next((c for c in ["Sector", "업종명", "Industry", "sector"] if c in df.columns), None)

            if code_col is None or sector_col is None:
                logger.warning(f"섹터 매핑 컬럼 없음. 사용 가능 컬럼: {list(df.columns)}")
                return

            new_map: dict[str, str] = {}
            for _, row in df.iterrows():
                ticker = str(row[code_col]).zfill(6)
                sector = str(row[sector_col]).strip() or "기타"
                new_map[ticker] = sector

            with self._data_lock:
                self._sector_map = new_map

            logger.info(f"섹터 매핑 로드 완료: {len(new_map)}종목")
        except Exception as e:
            logger.warning(f"섹터 매핑 로드 실패: {e}")

    def _save_sector_strength(self, strength: dict[str, dict]) -> None:
        """섹터 강도를 DB sector_strength 테이블에 저장."""
        try:
            from src.infra.database import execute
            for sector, data in strength.items():
                execute(
                    """
                    INSERT INTO sector_strength (sector, avg_ret_1d, vs_kospi, stock_count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(sector) DO UPDATE SET
                        avg_ret_1d = excluded.avg_ret_1d,
                        vs_kospi   = excluded.vs_kospi,
                        stock_count = excluded.stock_count,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (sector, data["avg_ret_1d"], data["vs_kospi"], data["count"]),
                )
        except Exception as e:
            logger.debug(f"섹터 강도 DB 저장 실패: {e}")


# ── 모듈 레벨 편의 함수 ────────────────────────────────────────

def get_kospi_daily_chg() -> float:
    return SectorRotationCache().get_kospi_daily_chg()

def get_kospi_5d_ret() -> float:
    return SectorRotationCache().get_kospi_5d_ret()

def get_sector(ticker: str) -> str:
    return SectorRotationCache().get_sector(ticker)

def get_sector_vs_kospi(sector: str) -> float:
    return SectorRotationCache().get_sector_vs_kospi(sector)

def get_hot_sectors(n: int = 3) -> list[str]:
    return SectorRotationCache().get_hot_sectors(n)

def get_cold_sectors(n: int = 3) -> list[str]:
    return SectorRotationCache().get_cold_sectors(n)

def inject_scan_results(snapshots: list) -> None:
    SectorRotationCache().inject_scan_results(snapshots)

def prefetch() -> None:
    SectorRotationCache().prefetch()
