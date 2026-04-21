"""
analyzer.py — 국내 주식팀 Claude 분석 모듈

후보 종목 목록을 Claude에 보내 Hot List 여부를 판단한다.
후보가 많으면 배치로 분할하여 처리.

모델: claude-haiku-4-5 (temperature=0) — 비용 최적화
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

# 1회 Claude 호출당 처리할 최대 종목 수 (max_positions=3 → 상위 10개면 충분)
_BATCH_SIZE = 10

# Hot List 최대 종목 수 (max_positions=3과 일치 — 3종목 집중)
_MAX_HOT_LIST = 3

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
- BB폭비율: 현재 볼린저밴드 폭 / 최근 20봉 평균폭. 1.0=평균, 1.3↑=변동성 폭발(스퀴즈 돌파)
- MA20: 20일 이동평균선. 현재가가 위=상승 추세, 아래=하락 추세
- 거래량비율: 최근 20일 평균 대비 배수 (1.0=평균, 3.0=3배)
- OBV기울기: 최근 5봉 On-Balance Volume 기울기. 양수=매수세 유입, 음수=매도세(가격 상승에도 OBV 하락=위험)
- StochRSI: Stochastic RSI(14,14). 80↑=단기 과매수(주의), 20↓=과매도(반등 대기), 50~80=모멘텀 지속
- 모멘텀점수: 거래량·MACD·BB폭·OBV 종합 점수(0~100). 높을수록 신호 품질 우수
- 당일범위위치(day_range): (현재가-당일저가)/(당일고가-당일저가). 0=저가권(눌림 반등 유리), 0.9↑=고가권(추격 주의)
- 외인순매수: 당일 외국인 순매수량(주). 양수=외인 매수우위, 음수=외인 매도우위. 제공 시 수급 강도 판단에 활용
- 기관순매수: 당일 기관 순매수량(주). 양수=기관 매수우위. 외인+기관 동시 양수=가장 강한 수급 신호
- 조건 태그: 거래량N배 / 가격+N% / BB돌파 (동시 충족할수록 신호 강도 높음)

### 시장 컨텍스트 해석
- 국내 시장점수: -1.0(극약세) ~ +1.0(극강세). 0.0 이상=중립 이상
- 글로벌 리스크: 0(완전 안전) ~ 10(극도 위험). 7↑=보수적 선정 필요

## 매매 전략 선택 (시황과 종목 상황에 따라 자동 판단)

시황과 종목 특성을 보고 아래 전략 중 가장 적합한 것을 선택하세요.
Hot List 안에서 종목마다 다른 전략을 적용할 수 있습니다.

### 전략 A: 갭업 돌파매매 (gap_up_breakout) — 최우선
**언제**: 전일 대비 +8% 이상 갭업 종목이 있을 때
- 갭업 +8% 이상 + OBV기울기 양수: 세력/기관 매집 중 갭업
- 갭업 +8% 이상 + MACD 히스토그램 양수: 갭업 후 추가 모멘텀
- 거래량 10배↑ + 갭업: 수급 폭발적 집중
특히 +10~25% 구간(상한가 미달 강한 갭업)이 추가 상승 여력 가장 큼.
RSI 80↑이어도 OBV 양수면 허용 — reason에 "RSI과열_포지션50%" 명시.
day_range 0.90↑이어도 갭업+OBV양수면 허용.

### 전략 B: 눌림목 반등매매 (pullback_rebound) — 갭업 없을 때 1순위
**언제**: 갭업 종목이 없거나 부족할 때. 전일 강세 후 오늘 조정받는 종목.
아래 중 3개 이상 충족 시 선정:
- 전일 대비 등락 -0.5% ~ -5% (오늘 소폭~중폭 하락)
- 장중등락(시가대비) 양수 또는 day_range 0.30 이하 저가권에서 반등 중
- OBV기울기 양수 (하락에도 매수세 유입 — 세력 매집)
- MACD 히스토그램 음수이지만 전봉 대비 증가 (수렴 시작)
- 거래량 1.5배↑ + 외인/기관 순매수 동반
**매매 특징**: 손절 타이트(-1.5%), 빠른 목표 수익(+3~4%), 당일 청산 원칙.

### 전략 C: 시장 강세 편승 (market_momentum) — KOSPI 강한 날
**언제**: 시장 컨텍스트의 KOSPI 등락이 +1.5% 이상인 강세장
- 외인+기관 동시 순매수 종목 우선
- 등락 0%~+6% (과열 전 구간, 시장과 함께 상승 중)
- RSI 45~70 (모멘텀 있되 과열 아님)
- 거래량 1.5배↑ + MA20 위
**매매 특징**: 손절 넓게(-2%), 시장 꺾이면 즉시 이탈. KOSPI 약세 전환 시 청산.

### 갭업/눌림/강세 모두 해당 없을 때 (일반 후보)
- 거래량 3배↑ + 가격 상승 + MACD 양수: volume_surge
- BB상단 돌파 + RSI 60↑ + MA20 위: breakout
- MACD 수렴 중 + RSI 50~65: momentum

### 수급 보조 판단 (모든 전략 공통)
- 외인+기관 동시 순매수: 가장 강한 확인 신호 — 최우선 선정
- 외인+기관 동시 순매도: 기술적 신호 무관하게 선정 보류
- 외인 단독 강한 순매도 (거래량 10%↑): 기술적 신호만으로 선정 불가

## RSI 과열 구간별 처리 (차단 대신 포지션 조정)
- RSI 72~82 구간: 선정 가능하나 reason에 "RSI과열_포지션50%" 명시 → 매매팀이 1차 매수 비중 50%로 축소, 손절 1.5%로 타이트하게
- RSI 82↑: 완전 차단 (극단적 과열 — 폭락 리스크)
- StochRSI 85↑: RSI와 관계없이 단기 과매수 주의 — reason에 "StochRSI과매수" 명시 (선정은 허용, 주의 표시)
- OBV기울기 음수 + RSI 70↑: 가격 상승이 매수세 없이 공매도 세력에 의한 쇼트커버 가능성 → 선정 보류 권고

## 제외 기준 (하나라도 해당 시 선정 불가)
- RSI 82↑: 극단적 과열 — 즉각 하락 전환 위험 높음
- 가격 하락 중 + MACD 히스토그램 하락: 명확한 하락 추세
- OBV기울기 음수 + 가격 상승 + RSI 70↑: 수급 없는 가짜 상승
- 거래량 없이 가격만 급등 (거래량비율 < 1.5): 유동성 부족
- 글로벌 리스크 9↑ 시: RSI 60↑ 종목만 선정 (전쟁·금융위기급 보수 모드)
- 외인+기관 동시 순매도: 기술적 신호와 무관하게 선정 보류 (세력 이탈 가능성)
- 외인 단독 강한 순매도 (음수 규모가 거래량의 10%↑): 기술적 신호만으로 선정 불가
- 당일범위위치(day_range) 0.90↑ + 등락 +3%↑: 당일 고가권 추격 매수 — 선정 자제
  (예외: BB돌파 + OBV↑ 동반 시 선정 가능, reason에 "고가권돌파" 명시)

## 시장 상황별 선정 강도 조절
- 시장점수 +0.5↑: 적극 선정 (최대 10종목까지 허용)
- 시장점수 -0.1 ~ +0.5: 기준 준수 (5~8종목)
- 시장점수 -0.3 ~ -0.1: 보수적 선정 (3~5종목)
- 시장점수 -0.3↓: 최소화 (1~3종목, 매우 강한 신호만)

## 응답 규칙
- JSON만 출력합니다. 설명이나 주석을 추가하지 않습니다.
- hot_list가 비어있으면 빈 배열 []을 반환합니다.
- 선정 이유는 한국어로 20자 이내로 간결하게 작성합니다.
- signal_type은 반드시 다음 중 하나:
  gap_up_breakout (전략A) | pullback_rebound (전략B) | market_momentum (전략C)
  | volume_surge | breakout | momentum | sector_momentum

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
    semiconductor_alert: str = "",
    kospi_chg_pct: float = 0.0,
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
        # 추가 보조지표 문자열
        obv_str   = f"OBV{'↑' if s.obv_slope > 0 else '↓'}{s.obv_slope:+.2f}"
        bbw_str   = f"BB폭{s.bb_width_ratio:.2f}x" + ("🔥" if s.bb_width_ratio >= 1.3 else "")
        srsi_str  = f"StochRSI {s.stoch_rsi:.0f}" + ("⚠️" if s.stoch_rsi >= 85 else "")
        mscore_str = f"모멘텀{s.momentum_score:.0f}점"
        drp = getattr(s, "day_range_pos", 0.5)
        drp_str = f"당일범위{drp:.2f}" + ("⚠️고가권" if drp >= 0.90 else ("↓저가권" if drp <= 0.20 else ""))
        is_gap_up = getattr(s, "is_gap_up", False)
        gap_str = f"갭업{s.change_pct:+.0f}%" if is_gap_up else ""
        intraday_chg = getattr(s, "intraday_chg_pct", 0.0)
        intraday_str = f"장중{intraday_chg:+.1f}%"
        lines.append(
            f"- {s.ticker}({s.name}): "
            f"등락{s.change_pct:+.1f}%{' [' + gap_str + ']' if gap_str else ''} | {intraday_str} | RSI {s.rsi:.0f} | "
            f"MACD히스토그램 {s.macd_hist:+.4f} | BB위치 {s.bb_position:.2f} | "
            f"MA20{'위' if s.above_ma20 else '아래'} | {obv_str} | {bbw_str} | {srsi_str} | {mscore_str} | {drp_str}"
            f"{supply_str} | {flag_str}"
        )
    stock_block = "\n".join(lines)

    semiconductor_line = f"\n- ⚠️ {semiconductor_alert}" if semiconductor_alert else ""
    # 전략 선택 힌트
    if kospi_chg_pct >= 1.5:
        strategy_hint = f"KOSPI {kospi_chg_pct:+.1f}% 강세장 → 전략C(market_momentum) 적극 고려"
    elif kospi_chg_pct <= -1.0:
        strategy_hint = f"KOSPI {kospi_chg_pct:+.1f}% 약세장 → 전략B(pullback_rebound) 또는 보수적 선정"
    else:
        strategy_hint = f"KOSPI {kospi_chg_pct:+.1f}% 중립 → 갭업/눌림 종목 우선, 없으면 모멘텀"
    return (
        f"## 현재 시장 컨텍스트\n"
        f"- 국내 시장점수: {market_score:+.2f}\n"
        f"- KOSPI 등락: {kospi_chg_pct:+.1f}%\n"
        f"- 글로벌 리스크: {global_risk_score}/10\n"
        f"- 전략 힌트: {strategy_hint}"
        f"{semiconductor_line}\n\n"
        f"## 후보 종목 ({len(candidates)}개)\n"
        f"{stock_block}\n\n"
        f"위 종목 중 Hot List를 선정하세요. 최대 {max_hot_list}종목."
    )


def analyze(
    scan: UniverseScan,
    market_score: float = 0.0,
    global_risk_score: int = 5,
    global_key_events: list[str] | None = None,
    kospi_chg_pct: float = 0.0,
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

    # ── 글로벌 반도체 약세 경보 ──────────────────────────────
    # TSM·NVDA 등 글로벌 반도체가 -2% 이상 하락한 날은
    # Claude에게 반도체·소재·장비 관련주 선정 자제 지시
    semiconductor_alert = ""
    if global_key_events:
        semi_keywords = ("tsm", "nvda", "반도체", "semiconductor")
        for event in global_key_events:
            if any(k in event.lower() for k in semi_keywords) and "하락" in event:
                semiconductor_alert = (
                    "글로벌 반도체 약세 감지 — 반도체·소재·장비 관련 종목 선정 자제"
                )
                logger.info(f"반도체 약세 경보: {event}")
                break

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

    # 후보 정렬: momentum_score 우선 (OBV·BBWidth·MACD·거래량 종합), 없으면 vol_ratio
    candidates_sorted = sorted(
        candidates,
        key=lambda s: (
            s.momentum_score if s.momentum_score > 0 else (
                (s.is_volume_surge and s.is_price_surge) * 50
                + s.is_breakout * 20
                + s.volume_ratio * 5
                + (s.obv_slope > 0) * 10
            )
        ),
        reverse=True,
    )
    batch = candidates_sorted[:_BATCH_SIZE]

    logger.info(f"Claude Hot List 분석 시작 — 후보 {len(candidates)}개 중 {len(batch)}개 전송")

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,  # Haiku — 비용 최적화 (Gate 5에서 Sonnet 재검증)
            max_tokens=512,   # 10종목 × ~40토큰 + JSON 구조 = 여유 있게 512
            temperature=settings.CLAUDE_TEMPERATURE,
            timeout=30.0,     # 30초 타임아웃 (무한 대기 방지)
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
                        batch, market_score, global_risk_score, _MAX_HOT_LIST,
                        semiconductor_alert=semiconductor_alert,
                        kospi_chg_pct=kospi_chg_pct,
                    ),
                }
            ],
        )
        raw = response.content[0].text.strip()
        raw = _extract_json(raw)
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
        raw_preview = locals().get("raw", "")[:200]
        logger.error(f"Claude 응답 파싱 실패: {e} | raw='{raw_preview}'")
        return _fallback_hot_list(candidates_sorted)
    except Exception as e:
        logger.error(f"Claude API 오류: {type(e).__name__}: {e}")
        from src.utils.notifier import check_claude_error
        check_claude_error(e, "국내 주식 Hot List")
        return _fallback_hot_list(candidates_sorted)


def _extract_json(raw: str) -> str:
    """
    Claude 응답에서 JSON 부분만 추출.
    코드블록(```json ... ```) 또는 순수 JSON 양쪽 모두 처리.
    """
    import re
    # 코드블록 안 JSON 우선 추출
    m = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", raw)
    if m:
        return m.group(1).strip()
    # 중괄호 범위 직접 추출 (첫 { 부터 마지막 } 까지)
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        return raw[start:end + 1]
    return raw


def _fallback_hot_list(candidates: list[StockSnapshot]) -> list[dict]:
    """Claude 실패 시 자동 선정 — 전략 A/B/C 우선순위로 자동 분류."""
    # 전략 A: 갭업 돌파 (OBV 양수 + 갭업)
    gap_up = [
        s for s in candidates
        if s.is_gap_up and s.obv_slope > 0 and s.rsi <= 90
    ]
    # 전략 B: 눌림목 반등 (오늘 하락 + 시가 대비 반등 중 + OBV 양수)
    pullback = [
        s for s in candidates
        if not s.is_gap_up
        and -5.0 <= s.change_pct <= -0.5
        and getattr(s, "intraday_chg_pct", 0.0) >= 0
        and s.obv_slope > 0
        and s.rsi <= 75
    ]
    # 전략 C: 시장 강세 편승 (외인+기관 동시 순매수 + 적정 등락 + RSI 정상)
    mkt_mom = [
        s for s in candidates
        if not s.is_gap_up
        and s.change_pct >= -0.5
        and s.frgn_net_buy > 0 and s.inst_net_buy > 0
        and 45 <= s.rsi <= 70
        and s.above_ma20
    ]
    # 일반 후보 (위 3가지 해당 없음)
    normal = [
        s for s in candidates
        if not s.is_gap_up
        and s.rsi <= 82
        and not (s.obv_slope < 0 and s.rsi > 70)
        and s.is_price_surge
        and s not in pullback and s not in mkt_mom
    ]

    gap_top  = sorted(gap_up,  key=lambda s: s.change_pct,    reverse=True)[:2]
    pb_top   = sorted(pullback, key=lambda s: s.momentum_score, reverse=True)[:1]
    mm_top   = sorted(mkt_mom,  key=lambda s: s.momentum_score, reverse=True)[:1]
    norm_top = sorted(normal,   key=lambda s: s.momentum_score, reverse=True)[:1]

    # 슬롯 채우기: 갭업 우선, 나머지 1자리는 B→C→일반 순
    top = gap_top[:]
    if len(top) < 3:
        for s in (pb_top + mm_top + norm_top):
            if s not in top:
                top.append(s)
            if len(top) >= 3:
                break
    top = top[:3]

    result = []
    for s in top:
        if s.is_gap_up:
            sig = "gap_up_breakout"
            reason = f"갭업{s.change_pct:+.0f}%·OBV{'↑' if s.obv_slope > 0 else '↓'}·거래량{s.volume_ratio:.1f}배"
        elif s in pullback:
            sig = "pullback_rebound"
            intra = getattr(s, "intraday_chg_pct", 0.0)
            reason = f"눌림{s.change_pct:+.0f}%·장중{intra:+.1f}%·OBV↑"
        elif s in mkt_mom:
            sig = "market_momentum"
            reason = f"강세편승·외인{s.frgn_net_buy:+,}·기관{s.inst_net_buy:+,}"
        else:
            sig = "volume_surge"
            reason = f"모멘텀{s.momentum_score:.0f}점·거래량{s.volume_ratio:.1f}배"
        reason += ("·RSI과열_포지션50%" if s.rsi > 72 else "") + " (자동선정)"
        result.append({"ticker": s.ticker, "signal_type": sig, "reason": reason})
    return result
