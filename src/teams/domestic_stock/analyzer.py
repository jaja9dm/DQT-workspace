"""
analyzer.py — 국내 주식팀 Claude 분석 모듈

후보 종목 목록을 Claude에 보내 Hot List 여부를 판단한다.
후보가 많으면 배치로 분할하여 처리.

모델: claude-sonnet-4-6 (temperature=0)
출력: hot_list 테이블에 저장할 종목 목록
"""

from __future__ import annotations

import json

import anthropic

from src.config.settings import settings
from src.teams.domestic_stock.collector import StockSnapshot, UniverseScan
from src.utils.logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

# 1회 Claude 호출당 처리할 최대 종목 수
_BATCH_SIZE = 20

# Hot List 최대 종목 수
_MAX_HOT_LIST = 10


def _build_prompt(
    candidates: list[StockSnapshot],
    market_score: float,
    global_risk_score: int,
) -> str:
    lines = []
    for s in candidates:
        flags = []
        if s.is_volume_surge:
            flags.append(f"거래량{s.volume_ratio:.1f}배")
        if s.is_price_surge:
            flags.append(f"가격+{s.change_pct:.1f}%")
        if s.is_breakout:
            flags.append("BB돌파")
        flag_str = "/".join(flags) if flags else "조건미충족"

        lines.append(
            f"- {s.ticker}({s.name}): "
            f"등락{s.change_pct:+.1f}% | RSI {s.rsi:.0f} | "
            f"MACD히스토그램 {s.macd_hist:+.4f} | BB위치 {s.bb_position:.2f} | "
            f"MA20{'위' if s.above_ma20 else '아래'} | {flag_str}"
        )
    stock_block = "\n".join(lines)

    return f"""당신은 국내 주식 퀀트 전략가입니다.
아래 후보 종목들을 검토하여 당일 매매 관심 목록(Hot List)을 선정하세요.

## 시장 컨텍스트
- 국내 시장 점수: {market_score:+.2f} (-1.0 약세 ~ +1.0 강세)
- 글로벌 리스크 점수: {global_risk_score}/10 (10이 최대 위험)

## 후보 종목 ({len(candidates)}개)
{stock_block}

## 선정 기준
1. 거래량 급증 (평균 대비 3배↑) + 가격 상승 동반: 강력 매수 신호
2. 볼린저밴드 상단 돌파 + RSI 60↑: 모멘텀 돌파 신호
3. MACD 히스토그램 양전환: 추세 전환 신호
4. 글로벌 리스크 7 이상 시 보수적으로 선정
5. RSI 70 초과 + BB위치 0.9 이상이면 과열 — 제외

## 응답 형식 (반드시 JSON만, 최대 {_MAX_HOT_LIST}종목)
{{
  "hot_list": [
    {{
      "ticker": "<6자리>",
      "signal_type": "<volume_surge|breakout|momentum|sector_momentum>",
      "reason": "<선정 근거 20자 이내>"
    }}
  ]
}}

선정 기준 미충족 시 hot_list를 빈 배열로 반환."""


def analyze(
    scan: UniverseScan,
    market_score: float = 0.0,
    global_risk_score: int = 5,
) -> list[dict]:
    """
    후보 종목을 Claude에 보내 Hot List 판단.

    Args:
        scan: 유니버스 스캔 결과
        market_score: 국내 시황팀이 산출한 시장 점수 (-1.0~1.0)
        global_risk_score: 글로벌 시황팀이 산출한 리스크 점수 (0~10)

    Returns:
        Hot List 딕셔너리 리스트 [{"ticker", "signal_type", "reason"}]
    """
    candidates = scan.candidates
    if not candidates:
        logger.info("후보 종목 없음 — Hot List 비어있음")
        return []

    # 후보가 많으면 배치 분할 (신호 강도 우선: 거래량 급등 > 가격 급등 > BB돌파)
    candidates_sorted = sorted(
        candidates,
        key=lambda s: (
            s.is_volume_surge and s.is_price_surge,  # 복합 신호 최우선
            s.is_breakout,
            s.volume_ratio,
        ),
        reverse=True,
    )
    batch = candidates_sorted[:_BATCH_SIZE]

    logger.info(f"Claude Hot List 분석 시작 — 후보 {len(candidates)}개 중 {len(batch)}개 전송")

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=1024,
            temperature=settings.CLAUDE_TEMPERATURE,
            messages=[
                {
                    "role": "user",
                    "content": _build_prompt(batch, market_score, global_risk_score),
                }
            ],
        )
        raw = response.content[0].text.strip()

        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        result = json.loads(raw)
        hot_list = result.get("hot_list", [])
        logger.info(f"Claude Hot List 결정: {len(hot_list)}종목")
        return hot_list[:_MAX_HOT_LIST]

    except json.JSONDecodeError as e:
        logger.error(f"Claude 응답 파싱 실패: {e}")
        return _fallback_hot_list(candidates_sorted)
    except Exception as e:
        logger.error(f"Claude API 오류: {e}")
        from src.utils.notifier import check_claude_error
        check_claude_error(e, "국내 주식 Hot List")
        return _fallback_hot_list(candidates_sorted)


def _fallback_hot_list(candidates: list[StockSnapshot]) -> list[dict]:
    """Claude 실패 시 신호 강도 기준 상위 5종목 자동 선정."""
    top = [
        s for s in candidates
        if s.is_volume_surge and s.is_price_surge and not (s.rsi > 70 and s.bb_position > 0.9)
    ][:5]

    return [
        {
            "ticker": s.ticker,
            "signal_type": "volume_surge",
            "reason": f"거래량{s.volume_ratio:.1f}배·가격{s.change_pct:+.1f}% (자동선정)",
        }
        for s in top
    ]
