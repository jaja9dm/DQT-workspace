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

    # Claude 모델 할당 (concept.md 8-3 기준)
    CLAUDE_MODEL_FAST: str = "claude-haiku-4-5-20251001"    # 감성 캐시, 빠른 스캔
    CLAUDE_MODEL_MAIN: str = "claude-sonnet-4-6"            # 매매팀, 위기관리팀
    CLAUDE_MODEL_RESEARCH: str = "claude-opus-4-6"          # 연구소, 전략 심층 분석

    # Claude 공통 설정
    CLAUDE_TEMPERATURE: float = 0.0   # 거래 판단 — 결정론적
    CLAUDE_MAX_TOKENS: int = 2048

    # ── DB ───────────────────────────────────────────
    DB_PATH: str = os.getenv("DB_PATH", str(ROOT / "db" / "dqt.db"))

    # ── 알림 ─────────────────────────────────────────
    SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")
    KAKAO_ACCESS_TOKEN: str = os.getenv("KAKAO_ACCESS_TOKEN", "")

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
