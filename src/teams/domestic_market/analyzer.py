"""
analyzer.py — 국내 시황팀 Claude 분석 모듈

수집된 국내 시황 데이터를 Claude API에 보내 시장 방향성과
투자 전략 권고를 생성한다.

모델: claude-sonnet-4-6 (temperature=0)
시스템 프롬프트(정적 규칙·응답형식)를 cache_control=ephemeral로 캐시.
"""

from __future__ import annotations

import json

import anthropic

from src.config.settings import settings
from src.teams.domestic_market.collector import DomesticMarketData
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_SYSTEM_PROMPT = """당신은 국내 주식시장 전문 퀀트 애널리스트입니다.
제공된 실시간 국내 시황 데이터를 분석하여 오늘의 시장 방향성을 평가하세요.

## 분석 요청
1. **시장 점수** (-1.0 약세 ~ +1.0 강세): 현재 시장의 방향성 수치화
   - 스캘핑·단타 관점: 변동성이 있어도 거래 가능하면 중립 이상으로 평가
   - bearish(-0.5 이하)는 진짜 큰 악재(전쟁·금융위기·서킷브레이커) 수준에만 사용
2. **시장 방향** (bullish / neutral / bearish)
3. **시장 판단 근거** (최대 3가지, 각 20자 이내)
4. **주도 주체** (foreign / institutional / individual / mixed): 오늘 시장 주도 세력
5. **한 줄 요약** (50자 이내)

## 응답 형식 (반드시 JSON만 출력)
{
  "market_score": <-1.0~1.0, 소수점 2자리>,
  "market_direction": "<bullish|neutral|bearish>",
  "key_reasons": ["<이유1>", "<이유2>"],
  "leading_force": "<foreign|institutional|individual|mixed>",
  "summary": "<한 줄 요약>"
}"""


def _build_user_content(
    data: DomesticMarketData,
    global_risk_score: int = 5,
    morning_summary: bool = False,
) -> str:
    news_lines = "\n".join(
        f"  - {n.title}" for n in data.news[:5]
    ) or "  - 수집된 뉴스 없음"

    kospi_trend  = data.kospi_trend
    kosdaq_trend = data.kosdaq_trend
    mode = (
        "장 전 오버나이트 요약 모드: 전날 장 마감 이후 글로벌 변화가 오늘 국내 장에 미칠 영향을 중심으로 평가하세요."
        if morning_summary else
        "현재 장중 상황을 평가하세요."
    )
    return f"""## 현재 국내 시장 데이터
- 수집 시각: {data.timestamp}
- 글로벌 리스크 점수 (참고): {global_risk_score}/10
- 분석 모드: {mode}

### KOSPI 지수
- 현재가: {data.kospi.current:,.2f}pt ({data.kospi.change_pct:+.2f}%)
- 5일 이평: {kospi_trend.ma5:,.2f} | 20일 이평: {kospi_trend.ma20:,.2f} | 60일 이평: {kospi_trend.ma60:,.2f}
- 20일선 상단 여부: {"예" if kospi_trend.above_ma20 else "아니오"} | 단기 추세: {kospi_trend.trend_direction}

### KOSDAQ 지수
- 현재가: {data.kosdaq.current:,.2f}pt ({data.kosdaq.change_pct:+.2f}%)
- 5일 이평: {kosdaq_trend.ma5:,.2f} | 20일 이평: {kosdaq_trend.ma20:,.2f} | 60일 이평: {kosdaq_trend.ma60:,.2f}
- 20일선 상단 여부: {"예" if kosdaq_trend.above_ma20 else "아니오"} | 단기 추세: {kosdaq_trend.trend_direction}

### 투자자별 매매동향 (KOSPI, 억원)
- 외국인 순매수: {data.kospi_flow.foreign_net:+.0f}억
- 기관 순매수: {data.kospi_flow.institutional_net:+.0f}억
- 개인 순매수: {data.kospi_flow.individual_net:+.0f}억

### 투자자별 매매동향 (KOSDAQ, 억원)
- 외국인 순매수: {data.kosdaq_flow.foreign_net:+.0f}억
- 기관 순매수: {data.kosdaq_flow.institutional_net:+.0f}억
- 개인 순매수: {data.kosdaq_flow.individual_net:+.0f}억

### 주요 뉴스 헤드라인
{news_lines}"""


def analyze(data: DomesticMarketData, global_risk_score: int = 5, morning_summary: bool = False) -> dict:
    """
    Claude에 국내 시황 분석 요청.

    Args:
        data: 국내 시황 수집 데이터
        global_risk_score: 글로벌 시황팀이 산출한 리스크 점수 (0~10)

    Returns:
        {
            "market_score": float,      # -1.0 ~ 1.0
            "market_direction": str,    # bullish | neutral | bearish
            "key_reasons": list[str],
            "leading_force": str,
            "summary": str,
        }
    """
    label = "오버나이트 요약" if morning_summary else "정기 분석"
    logger.info(f"Claude 국내 시황 분석 시작 ({label})")

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=256,
            temperature=settings.CLAUDE_TEMPERATURE,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {"role": "user", "content": _build_user_content(data, global_risk_score, morning_summary)}
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
            logger.debug(f"국내 시황 캐시 히트: {cache_read}토큰 절감")

        logger.info(
            f"Claude 분석 완료 — 시장점수: {result.get('market_score')} "
            f"| 방향: {result.get('market_direction')}"
        )
        return result

    except json.JSONDecodeError as e:
        logger.error(f"Claude 응답 JSON 파싱 실패: {e}")
        return _fallback_from_index(data)
    except Exception as e:
        logger.error(f"Claude API 오류: {e}")
        from src.utils.notifier import check_claude_error
        check_claude_error(e, "국내 시황")
        return _fallback_from_index(data)


def _fallback_from_index(data: DomesticMarketData) -> dict:
    """Claude 실패 시 지수 등락률만으로 기본값 산출."""
    avg_change = (data.kospi.change_pct + data.kosdaq.change_pct) / 2

    if avg_change > 0.5:
        score, direction = 0.5, "bullish"
    elif avg_change < -0.5:
        score, direction = -0.5, "bearish"
    else:
        score, direction = round(avg_change / 2, 2), "neutral"

    return {
        "market_score": score,
        "market_direction": direction,
        "key_reasons": ["Claude API 응답 실패 — 지수 등락률 기반 기본값"],
        "leading_force": "mixed",
        "summary": f"KOSPI {data.kospi.change_pct:+.2f}% KOSDAQ {data.kosdaq.change_pct:+.2f}% 기반 자동 산출",
    }
