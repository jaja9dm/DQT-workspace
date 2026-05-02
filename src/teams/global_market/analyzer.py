"""
analyzer.py — 글로벌 시황팀 Claude 분석 모듈

수집된 데이터를 Claude API에 보내 글로벌 리스크 점수와
한국 시장 전망을 생성한다.

모델: claude-sonnet-4-6 (temperature=0)
시스템 프롬프트(정적 규칙·응답형식)를 cache_control=ephemeral로 캐시.
"""

from __future__ import annotations

import json

import anthropic

from src.config.settings import settings
from src.teams.global_market.collector import GlobalMarketData
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_SYSTEM_PROMPT = """당신은 글로벌 매크로 전문 퀀트 애널리스트입니다.
제공된 실시간 글로벌 시장 데이터를 분석하여 한국 주식시장에 대한 리스크 평가를 수행하세요.

## 분석 요청
1. **글로벌 리스크 점수** (0~10, 0=완전 안전, 10=극도 위험)
2. **한국 시장 전망** (positive / neutral / negative)
   - 스캘핑·단타 관점: 변동성이 있어도 추세가 있으면 positive로 판단
   - negative는 전쟁·금융위기·서킷브레이커급 실질적 위기 상황에만 사용
3. **VIX 기반 리스크** (18↓=낮음, 18~25=주의, 25~30=경계, 30↑=위험)
4. **주요 리스크 요인** (최대 3가지)
5. **한 줄 요약**

## 응답 형식 (반드시 JSON만 출력)
{
  "global_risk_score": <0~10 정수>,
  "korea_market_outlook": "<positive|neutral|negative>",
  "vix_risk": "<low|caution|warning|danger>",
  "key_risks": ["<리스크1>", "<리스크2>"],
  "risk_summary": "<한 줄 요약 (50자 이내)>"
}"""


def _build_user_content(data: GlobalMarketData, morning_summary: bool = False) -> str:
    tech_lines = "\n".join(
        f"  - {name}: {chg:+.2f}%" for name, chg in data.us_tech.items()
    )
    events_text = "\n".join(f"  - {e}" for e in data.upcoming_events) or "  - 없음"
    mode = (
        "오버나이트 요약 모드: 전날 15:30 이후 오늘 장 전까지의 글로벌 변동을 종합하여 오늘 한국 장에 미칠 영향을 평가하세요."
        if morning_summary else
        "현재 시점 글로벌 시장 상태를 평가하세요."
    )
    return f"""## 현재 글로벌 시장 데이터
- 수집 시각: {data.timestamp}
- 분석 모드: {mode}

### 미국 증시
- S&P 500: {data.sp500_price:,.2f} ({data.sp500_change:+.2f}%)
- NASDAQ: {data.nasdaq_price:,.2f} ({data.nasdaq_change:+.2f}%)
- Dow Jones: {data.dow_price:,.2f} ({data.dow_change:+.2f}%)

### 공포 지수
- VIX: {data.vix:.2f}

### 원자재
- WTI 원유: ${data.wti_oil:.2f}/배럴
- 금: ${data.gold:.2f}/온스

### 환율
- USD/KRW: {data.usd_krw:.2f}
- JPY/KRW: {data.jpy_krw:.4f}
- EUR/KRW: {data.eur_krw:.2f}

### 미국 10년물 국채 금리
- {data.us_10y_yield:.3f}%

### 주요 미국 기술주 등락률
{tech_lines}

### 예정된 경제지표 발표 (2일 이내)
{events_text}"""


def analyze(data: GlobalMarketData, morning_summary: bool = False) -> dict:
    """
    Claude에 글로벌 시황 분석 요청.

    Returns:
        {
            "global_risk_score": int,
            "korea_market_outlook": str,
            "vix_risk": str,
            "key_risks": list[str],
            "risk_summary": str,
        }
    """
    label = "오버나이트 요약" if morning_summary else "정기 분석"
    logger.info(f"Claude 글로벌 시황 분석 시작 ({label})")

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=512,
            temperature=settings.CLAUDE_TEMPERATURE,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {"role": "user", "content": _build_user_content(data, morning_summary)}
            ],
        )
        raw = response.content[0].text.strip()

        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        result = json.loads(raw)

        cache_read = getattr(response.usage, "cache_read_input_tokens", 0) or 0
        if cache_read:
            logger.debug(f"글로벌 시황 캐시 히트: {cache_read}토큰 절감")

        logger.info(
            f"Claude 분석 완료 — 리스크 점수: {result.get('global_risk_score')} "
            f"| 전망: {result.get('korea_market_outlook')}"
        )
        return result

    except json.JSONDecodeError as e:
        logger.error(f"Claude 응답 JSON 파싱 실패: {e}")
        return _fallback_from_vix(data.vix)
    except Exception as e:
        logger.error(f"Claude API 오류: {e}")
        from src.utils.notifier import check_claude_error
        check_claude_error(e, "글로벌 시황")
        return _fallback_from_vix(data.vix)


def _fallback_from_vix(vix: float) -> dict:
    """Claude 실패 시 VIX 값만으로 기본 리스크 산출."""
    if vix < 18:
        score, outlook = 1, "positive"
    elif vix < 25:
        score, outlook = 3, "neutral"
    elif vix < 30:
        score, outlook = 6, "neutral"
    else:
        score, outlook = 8, "negative"

    return {
        "global_risk_score": score,
        "korea_market_outlook": outlook,
        "vix_risk": "low" if vix < 18 else "caution" if vix < 25 else "warning" if vix < 30 else "danger",
        "key_risks": ["Claude API 응답 실패 — VIX 기반 기본값 적용"],
        "risk_summary": f"VIX {vix:.1f} 기반 자동 산출 (Claude 미사용)",
    }
