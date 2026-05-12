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


# ── KSIC 세부 업종 → 투자자 관점의 큰 섹터 매핑 ──────────────────
# fdr.StockListing('KRX-DESC').Industry는 통계청 KSIC 기준이라 매우 세분화.
# 같은 업종이라도 표현이 달라 group_by 시 종목 수 1~2개 단위로 흩어진다.
# 투자자 관점에서 의미 있는 큰 섹터(반도체/자동차/2차전지/바이오 등)로 모은다.

_BROAD_SECTOR_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    # (broad_sector, keyword_tuples) — Industry 문자열에 키워드 중 하나라도 포함되면 매칭
    ("반도체",         ("반도체",)),
    ("디스플레이",     ("디스플레이",)),
    ("전자/IT부품",    ("전자부품", "통신 및 방송 장비", "광전송 장치", "광학", "측정",
                       "전동기, 발전기 및 전기 변환", "절연선 및 케이블", "전구",
                       "축전지", "1차전지", "전기 공급")),
    ("자동차",         ("자동차",)),
    ("2차전지",        ("2차전지", "이차전지", "축전지 제조", "전지 제조")),
    ("조선/해운",      ("선박", "해상 운송")),
    ("기계/설비",      ("일반 목적용 기계", "특수 목적용 기계", "기계장비",
                       "기계 제조", "공작기계")),
    ("철강/금속",      ("철강", "1차 비철금속", "금속 가공")),
    ("화학/소재",      ("화학", "플라스틱", "합성수지", "고무")),
    ("바이오/제약",    ("의약품", "기초 의약물질", "의료용품", "의료용 기기",
                       "자연과학 및 공학 연구개발")),
    ("소프트웨어/플랫폼", ("소프트웨어", "컴퓨터 프로그래밍", "포털 및 기타",
                          "온라인 정보", "데이터 처리", "정보서비스")),
    ("게임/콘텐츠",    ("게임 소프트웨어", "영화, 비디오", "방송", "오디오물",
                       "출판")),
    ("통신서비스",     ("전기통신", "이동 통신")),
    ("건설/건자재",    ("건설업", "건물 건설", "토목 건설", "전문직별 공사",
                       "시멘트", "벽돌", "유리")),
    ("음식료",         ("식품 제조", "음료 제조", "도축", "수산물", "곡물")),
    ("유통/소매",      ("도매", "소매", "백화점")),
    ("운송/물류",      ("육상 운송", "항공 운송", "보관 및 창고")),
    ("금융",           ("은행", "보험", "기타 금융", "금융 지원", "신용",
                       "투자", "자산운용")),
    ("부동산/리츠",    ("부동산",)),
    ("섬유/의류",      ("봉제의복", "섬유", "신발")),
    ("미디어/엔터",    ("연예", "기획")),
    ("에너지/유틸",    ("석유", "가스", "전기업", "발전업", "원유")),
    ("교육/서비스",    ("교육", "사업시설 관리", "사업 지원")),
    ("환경/리사이클",  ("폐기물", "환경 정화")),
)


def _industry_to_broad_sector(industry: str) -> str:
    """KSIC 세부 업종 문자열 → 투자자 관점의 큰 섹터.

    매칭 안 되면 KSIC 그대로 (단, 너무 긴 경우 30자로 절단). '기타'는 별도로 처리.
    """
    if not industry:
        return "기타"
    s = industry.strip()
    if not s or s.lower() == "nan":
        return "기타"
    for broad, keywords in _BROAD_SECTOR_RULES:
        for kw in keywords:
            if kw in s:
                return broad
    # 매칭 실패 시 원본 사용 (단 한도 30자) — 후속에서 _BROAD_SECTOR_RULES 보강할 단서
    return s if len(s) <= 30 else s[:27] + "..."


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
        """KRX 전종목 업종 정보 로드.

        2026-05-12 수정: fdr.StockListing('KRX')는 'Sector/Industry' 미제공이고
        'Dept'(소속부)만 있어 "우량기업부/중견기업부" 같은 값이 적재됐다.
        진짜 업종은 fdr.StockListing('KRX-DESC')의 'Industry' 컬럼에 존재.
        세부 KSIC 업종을 투자자 관점의 큰 섹터로 매핑하여 적재한다.
        """
        try:
            import FinanceDataReader as fdr
            df = fdr.StockListing("KRX-DESC")
            if df is None or df.empty:
                logger.warning("KRX-DESC 응답 비어있음 — 섹터 매핑 스킵")
                return

            # KRX-DESC 컬럼: Code, Name, Market, Sector(소속부, 대부분 NaN), Industry(KSIC), Products...
            code_col = next((c for c in ["Code", "Symbol", "ISU_SRT_CD"] if c in df.columns), None)
            if code_col is None or "Industry" not in df.columns:
                logger.warning(f"섹터 매핑 컬럼 없음. 사용 가능 컬럼: {list(df.columns)}")
                return

            new_map: dict[str, str] = {}
            unmapped_industries: dict[str, int] = {}
            for _, row in df.iterrows():
                ticker = str(row[code_col]).zfill(6)
                raw = row.get("Industry")
                # NaN / None / 빈 문자열 → '기타'
                if raw is None:
                    sector = "기타"
                else:
                    s = str(raw).strip()
                    sector = "기타" if (not s or s.lower() == "nan") else _industry_to_broad_sector(s)
                if sector == "기타" and raw is not None:
                    s = str(raw).strip()
                    if s and s.lower() != "nan":
                        unmapped_industries[s] = unmapped_industries.get(s, 0) + 1
                new_map[ticker] = sector

            with self._data_lock:
                self._sector_map = new_map

            mapped = sum(1 for v in new_map.values() if v != "기타")
            logger.info(
                f"섹터 매핑 로드 완료: 총 {len(new_map)}종목 / 분류 {mapped}종목"
            )
            if unmapped_industries:
                top_unmap = sorted(
                    unmapped_industries.items(), key=lambda x: x[1], reverse=True
                )[:5]
                logger.debug(f"섹터 매핑 누락 KSIC 상위: {top_unmap}")
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
