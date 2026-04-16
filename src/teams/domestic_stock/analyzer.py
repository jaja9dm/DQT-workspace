"""
analyzer.py — 국내 주식팀 Claude 분석 모듈

후보 종목 목록을 Claude에 보내 Hot List 여부를 판단한다.
후보가 많으면 배치로 분할하여 처리.

모델: claude-sonnet-4-6 (temperature=0)
출력: hot_list 테이블에 저장할 종목 목록

토큰 최적화:
  - 시스템 프롬프트(정적 선정 기준·규칙)를 cache_control=ephemeral로 캐시
  - 사이클마다 바뀌는 후보 데이터만 user 메시지로 전송
  - 5분 주기 반복 호출 시 캐시 히트 → 입력 토큰 ~70% 절감
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

# Hot List 최대 종목 수 (max_positions와 맞춤 — 5종목 집중)
_MAX_HOT_LIST = 5

# ─────────────────────────────────────────────────────────────────────
# 정적 시스템 프롬프트 — 캐시 대상 (1024 토큰 이상, 5분마다 재사용)
#
# 내용: 역할 정의 · 선정 기준 · 시장 해석 규칙 · 출력 형식 · 판단 가이드
# 변경 시: 캐시가 무효화되어 다음 호출에서 재캐싱됨
# ─────────────────────────────────────────────────────────────────────
_SYSTEM_PROMPT = """당신은 국내 주식 퀀트 전략가입니다.
5분 주기로 제공되는 후보 종목 배치를 분석하여 당일 매매 관심 목록(Hot List)을 선정합니다.
후보 종목들은 이미 일봉 MACD 필터(MACD 강세 또는 골든크로스 임박)를 통과한 상태입니다.

## 역할과 목표
- 당일 장중 모멘텀이 가장 강한 종목을 선별합니다.
- Hot List에 오른 종목은 매매팀이 실제 주문을 실행하므로 정확성이 중요합니다.
- 과열·리스크 신호가 있는 종목은 과감히 제외합니다.

## 입력 데이터 해석 가이드

### 각 종목 필드 설명
- 등락(%): 전일 종가 대비 현재가 변동률
- RSI: 상대강도지수 (0~100). 30↓=과매도, 70↑=과열
- MACD 히스토그램: 양수=강세, 음수=약세. 증가 중=모멘텀 축적
- BB위치(볼린저밴드): 0.0=하단, 0.5=중앙, 1.0=상단, 1.0↑=돌파
- MA20: 20일 이동평균선. 현재가가 위=상승 추세, 아래=하락 추세
- 거래량비율: 최근 20일 평균 대비 배수 (1.0=평균, 3.0=3배)
- 외인순매수: 당일 외국인 순매수량(주). 양수=외인 매수우위, 음수=외인 매도우위. 제공 시 수급 강도 판단에 활용
- 기관순매수: 당일 기관 순매수량(주). 양수=기관 매수우위. 외인+기관 동시 양수=가장 강한 수급 신호
- 조건 태그: 거래량N배 / 가격+N% / BB돌파 (동시 충족할수록 신호 강도 높음)

### 시장 컨텍스트 해석
- 국내 시장점수: -1.0(극약세) ~ +1.0(극강세). 0.0 이상=중립 이상
- 글로벌 리스크: 0(완전 안전) ~ 10(극도 위험). 7↑=보수적 선정 필요

## 선정 기준 (우선순위 순)

### 1순위: 복합 모멘텀 신호 (즉시 선정 고려)
- 거래량 3배↑ + 가격 상승 + MACD 히스토그램 양수: 강력 매수 신호
- 볼린저밴드 상단 돌파(BB위치 > 1.0) + RSI 60↑ + MA20 위: 모멘텀 돌파

### 2순위: 골든크로스 임박 (선점 진입)
- MACD 히스토그램이 음수이지만 2봉 이상 연속 증가 (수렴 중)
- RSI 50~65 구간 (과열 아닌 상승 초입)
- MA20 위에서 거래량 증가 동반

### 3순위: 거래량 집중 단독 신호
- 거래량 5배↑ + 가격 소폭 상승: 세력 집결 가능성
- 단, MACD 하락세 중이면 제외

### 4순위: 외인/기관 수급 (데이터 제공 시 가중 판단)
- 외인+기관 동시 순매수(+): 가장 강한 수급 신호 — 1~2순위 기준 충족 시 최우선 선정
- 외인 단독 순매수: 기술적 신호와 함께라면 강한 복합 신호
- 기관 단독 순매수: 보조 신호 — 외인 없이도 기술적 신호 강하면 선정 가중
- 외인+기관 동시 순매도(-): 기술적 신호와 무관하게 선정 보류 권고
- 외인 순매도 + 기관 순매수: 상충 신호 — 기술적 지표 우선 판단
- 순매수 규모가 거래량 대비 클수록 수급 신뢰도 높음

## 제외 기준 (하나라도 해당 시 선정 불가)
- RSI 75↑ AND BB위치 0.95↑: 단기 과열 — 진입 시 손실 위험
- 가격 하락 중 + MACD 히스토그램 하락: 명확한 하락 추세
- 거래량 없이 가격만 급등 (거래량비율 < 1.5): 유동성 부족
- 글로벌 리스크 8↑ 시: RSI 60↑ 종목만 선정 (보수 모드)
- 외인+기관 동시 순매도: 기술적 신호와 무관하게 선정 보류 (세력 이탈 가능성)
- 외인 단독 강한 순매도 (음수 규모가 거래량의 10%↑): 기술적 신호만으로 선정 불가

## 시장 상황별 선정 강도 조절
- 시장점수 +0.5↑: 적극 선정 (최대 10종목까지 허용)
- 시장점수 -0.1 ~ +0.5: 기준 준수 (5~8종목)
- 시장점수 -0.3 ~ -0.1: 보수적 선정 (3~5종목)
- 시장점수 -0.3↓: 최소화 (1~3종목, 매우 강한 신호만)

## 응답 규칙
- JSON만 출력합니다. 설명이나 주석을 추가하지 않습니다.
- hot_list가 비어있으면 빈 배열 []을 반환합니다.
- 선정 이유는 한국어로 20자 이내로 간결하게 작성합니다.
- signal_type은 반드시 다음 중 하나: volume_surge | breakout | momentum | sector_momentum

## 출력 형식
{
  "hot_list": [
    {
      "ticker": "<6자리 종목코드>",
      "signal_type": "<volume_surge|breakout|momentum|sector_momentum>",
      "reason": "<선정 근거 20자 이내>"
    }
  ]
}"""


def _build_user_message(
    candidates: list[StockSnapshot],
    market_score: float,
    global_risk_score: int,
    max_hot_list: int,
) -> str:
    """동적 부분만 담은 사용자 메시지 구성."""
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

        supply_parts = []
        if s.frgn_net_buy != 0:
            supply_parts.append(f"외인{s.frgn_net_buy:+,}")
        if s.inst_net_buy != 0:
            supply_parts.append(f"기관{s.inst_net_buy:+,}")
        supply_str = f" | 수급({'/'.join(supply_parts)}주)" if supply_parts else ""
        lines.append(
            f"- {s.ticker}({s.name}): "
            f"등락{s.change_pct:+.1f}% | RSI {s.rsi:.0f} | "
            f"MACD히스토그램 {s.macd_hist:+.4f} | BB위치 {s.bb_position:.2f} | "
            f"MA20{'위' if s.above_ma20 else '아래'}{supply_str} | {flag_str}"
        )
    stock_block = "\n".join(lines)

    return (
        f"## 현재 시장 컨텍스트\n"
        f"- 국내 시장점수: {market_score:+.2f}\n"
        f"- 글로벌 리스크: {global_risk_score}/10\n\n"
        f"## 후보 종목 ({len(candidates)}개)\n"
        f"{stock_block}\n\n"
        f"위 종목 중 Hot List를 선정하세요. 최대 {max_hot_list}종목."
    )


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

    # ── 일봉 MACD 필터 ──────────────────────────────────────
    if settings.MACD_DAILY_FILTER:
        before = len(candidates)
        candidates = [s for s in candidates if s.daily_macd_ok]
        filtered = before - len(candidates)
        if filtered:
            logger.info(f"일봉 MACD 필터: {filtered}종목 제외 (MACD 비강세) — 잔여 {len(candidates)}종목")
        if not candidates:
            logger.info("일봉 MACD 필터 후 후보 없음 — Hot List 비어있음")
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
            max_tokens=512,
            temperature=settings.CLAUDE_TEMPERATURE,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},  # 정적 기준 캐시 (5분 TTL)
                }
            ],
            messages=[
                {
                    "role": "user",
                    "content": _build_user_message(
                        batch, market_score, global_risk_score, _MAX_HOT_LIST
                    ),
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

        # 캐시 히트 여부 로깅 (usage 블록에서 확인)
        usage = response.usage
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
        cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
        if cache_read:
            logger.debug(f"프롬프트 캐시 히트: {cache_read}토큰 절감")
        elif cache_write:
            logger.debug(f"프롬프트 캐시 저장: {cache_write}토큰")

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
