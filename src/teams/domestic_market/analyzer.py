"""
analyzer.py — 국내 시황팀 Claude 분석 모듈

수집된 국내 시황 데이터를 Claude API에 보내 시장 방향성과
투자 전략 권고를 생성한다.

모델: claude-sonnet-4-6 (temperature=0)
"""

from __future__ import annotations

import json

import anthropic

from src.config.settings import settings
from src.teams.domestic_market.collector import DomesticMarketData
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)


def _build_prompt(data: DomesticMarketData, global_risk_score: int = 5) -> str:
    news_lines = "\n".join(
        f"  - {n.title}" for n in data.news[:5]
    ) or "  - 수집된 뉴스 없음"

    kospi_trend = data.kospi_trend
    kosdaq_trend = data.kosdaq_trend

    return f"""당신은 국내 주식시장 전문 퀀트 애널리스트입니다.
아래 실시간 국내 시황 데이터를 분석하여 오늘의 시장 방향성을 평가하세요.

## 현재 국내 시장 데이터
- 수집 시각: {data.timestamp}
- 글로벌 리스크 점수 (참고): {global_risk_score}/10

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
{news_lines}

## 분석 요청
1. **시장 점수** (-1.0 약세 ~ +1.0 강세): 현재 시장의 방향성 수치화
2. **시장 방향** (bullish / neutral / bearish)
3. **시장 판단 근거** (최대 3가지, 각 20자 이내)
4. **주도 주체** (foreign / institutional / individual / mixed): 오늘 시장 주도 세력
5. **한 줄 요약** (50자 이내)

## 응답 형식 (반드시 JSON만 출력)
{{
  "market_score": <-1.0~1.0, 소수점 2자리>,
  "market_direction": "<bullish|neutral|bearish>",
  "key_reasons": ["<이유1>", "<이유2>"],
  "leading_force": "<foreign|institutional|individual|mixed>",
  "summary": "<한 줄 요약>"
}}"""


def analyze(data: DomesticMarketData, global_risk_score: int = 5) -> dict:
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
    logger.info("Claude 국내 시황 분석 시작")

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=256,
            temperature=settings.CLAUDE_TEMPERATURE,
            messages=[
                {"role": "user", "content": _build_prompt(data, global_risk_score)}
            ],
        )
        raw = response.content[0].text.strip()

        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        result = json.loads(raw)
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
