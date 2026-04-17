"""
settings.py
환경 변수 기반 전역 설정.
.env 파일에서 자동으로 로드한다.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# 프로젝트 루트 기준 .env 파일 로드
ROOT = Path(__file__).parent.parent.parent
load_dotenv(ROOT / ".env")


class Settings:
    # ── KIS API ──────────────────────────────────────
    KIS_APP_KEY: str = os.getenv("KIS_APP_KEY", "")
    KIS_APP_SECRET: str = os.getenv("KIS_APP_SECRET", "")
    KIS_ACCOUNT_NO: str = os.getenv("KIS_ACCOUNT_NO", "")   # 계좌번호 ex) 12345678-01
    KIS_MODE: str = os.getenv("KIS_MODE", "paper")           # paper | live

    # KIS REST Base URL (모드에 따라 자동 결정)
    @property
    def KIS_BASE_URL(self) -> str:
        if self.KIS_MODE == "live":
            return "https://openapi.koreainvestment.com:9443"
        return "https://openapivts.koreainvestment.com:29443"  # 모의

    # ── Claude API ───────────────────────────────────
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

    # Claude 모델 할당 (비용 최적화)
    CLAUDE_MODEL_FAST: str = "claude-haiku-4-5-20251001"    # 시황 분석, 감성 캐시, Hot List (78번/일)
    CLAUDE_MODEL_MAIN: str = "claude-sonnet-4-6"            # 매수 최종 판단(Gate5), 복기, 연구소
    CLAUDE_MODEL_RESEARCH: str = "claude-sonnet-4-6"        # (= MAIN, Opus 비용 절감)

    # Claude 공통 설정
    CLAUDE_TEMPERATURE: float = 0.0   # 거래 판단 — 결정론적
    CLAUDE_MAX_TOKENS: int = 2048

    # ── DB ───────────────────────────────────────────
    DB_PATH: str = os.getenv("DB_PATH", str(ROOT / "db" / "dqt.db"))

    # ── 알림 (텔레그램) ──────────────────────────────
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # ── 외부 API ──────────────────────────────────────
    FRED_API_KEY: str = os.getenv("FRED_API_KEY", "")

    # ── 리스크 파라미터 (concept.md 기준 기본값) ────────
    RISK_MAX_SINGLE_TRADE_PCT: float = float(os.getenv("RISK_MAX_SINGLE_TRADE_PCT", "10"))  # 1회 최대 비중 %
    STOP_LOSS_DEFAULT_PCT: float = float(os.getenv("STOP_LOSS_DEFAULT_PCT", "5.0"))
    STOP_LOSS_LEVEL2_PCT: float = float(os.getenv("STOP_LOSS_LEVEL2_PCT", "3.0"))
    STOP_LOSS_LEVEL4_PCT: float = float(os.getenv("STOP_LOSS_LEVEL4_PCT", "1.0"))
    TAKE_PROFIT_1_PCT: float = float(os.getenv("TAKE_PROFIT_1_PCT", "5.0"))
    TAKE_PROFIT_2_PCT: float = float(os.getenv("TAKE_PROFIT_2_PCT", "10.0"))
    POSITION_MAX_HOLD_DAYS: int = int(os.getenv("POSITION_MAX_HOLD_DAYS", "5"))

    # ── 트레일링 스톱 ─────────────────────────────
    # 초기 손절선: 매수가 대비 -N% (.env에서 조정 가능 — 권장 2~5%)
    TRAILING_INITIAL_STOP_PCT: float = float(os.getenv("TRAILING_INITIAL_STOP_PCT", "2.0"))
    # 손절선 올리기 시작 조건: 매수가 대비 +N% 이상 수익 시 (동적 계산 기본값)
    TRAILING_TRIGGER_PCT: float = float(os.getenv("TRAILING_TRIGGER_PCT", "3.0"))
    # 손절선 위치: 현재가 대비 -N% (트레일링 간격, 동적 계산 기본값)
    TRAILING_FLOOR_PCT: float = float(os.getenv("TRAILING_FLOOR_PCT", "2.5"))
    # 사다리 매수 발동 조건: 매수가 대비 -N% 하락 시
    LADDER_TRIGGER_PCT: float = float(os.getenv("LADDER_TRIGGER_PCT", "10.0"))
    # 사다리 매수 수량 비율 (기존 보유 수량의 N배)
    LADDER_QTY_RATIO: float = float(os.getenv("LADDER_QTY_RATIO", "1.0"))

    # ── MACD 전략 파라미터 ─────────────────────────
    # 일봉 MACD 필터: True이면 일봉 MACD 비강세 종목 Hot List 제외
    MACD_DAILY_FILTER: bool = os.getenv("MACD_DAILY_FILTER", "true").lower() == "true"
    # Pre-Cross 감지: 히스토그램이 N봉 연속 수렴 시 예측 신호 발생
    MACD_HIST_CONV_BARS: int = int(os.getenv("MACD_HIST_CONV_BARS", "2"))
    # MACD 조기 손절: True이면 포지션 진입 후 MACD 역행 시 즉시 청산
    MACD_EARLY_EXIT_ENABLED: bool = os.getenv("MACD_EARLY_EXIT_ENABLED", "true").lower() == "true"
    # MACD 조기 손절 발동 최소 손실률 (이 이상 손실 + MACD 역행 시 청산)
    MACD_EARLY_EXIT_MIN_LOSS_PCT: float = float(os.getenv("MACD_EARLY_EXIT_MIN_LOSS_PCT", "0.0"))

    def validate(self) -> None:
        """필수 환경 변수 누락 시 경고."""
        missing = []
        if not self.KIS_APP_KEY:
            missing.append("KIS_APP_KEY")
        if not self.KIS_APP_SECRET:
            missing.append("KIS_APP_SECRET")
        if not self.KIS_ACCOUNT_NO:
            missing.append("KIS_ACCOUNT_NO")
        if not self.ANTHROPIC_API_KEY:
            missing.append("ANTHROPIC_API_KEY")
        if missing:
            raise EnvironmentError(f"필수 환경 변수 누락: {', '.join(missing)}")


settings = Settings()
