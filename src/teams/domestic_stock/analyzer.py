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

import hashlib
import json
import time

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
# 호출 중복 제거 캐시
# 5분 주기 반복 스캔에서 후보 종목 집합이 동일하면 Claude 재호출 생략.
# 캐시 TTL = 10분 (최대 2 사이클 재사용) — 가격·거래량 변동 최소 반영.
# ─────────────────────────────────────────────────────────────────────
_cache_key:    str   = ""
_cache_result: list  = []
_cache_ts:     float = 0.0
_CACHE_TTL_SEC = 600   # 10분


def _candidates_fingerprint(candidates: list[StockSnapshot]) -> str:
    """
    상위 10개 후보의 핵심 지표 해시 — 유의미한 변화 시 캐시 무효화.

    포함 필드: ticker / volume_ratio(1dp) / rsi(정수) / exec_strength(10단위) /
               frgn_net_buy(부호) / inst_net_buy(부호)
    미포함(의도적): OBV·BB폭 등 노이즈 많은 파생지표 — 캐시를 너무 민감하게 만들지 않으려고
    """
    key_parts = sorted(
        f"{s.ticker}:{s.volume_ratio:.1f}:{s.rsi:.0f}"
        f":{round(getattr(s, 'exec_strength', 100.0) / 10) * 10}"
        f":{'+' if getattr(s, 'frgn_net_buy', 0) > 0 else ('-' if getattr(s, 'frgn_net_buy', 0) < 0 else '0')}"
        f":{'+' if getattr(s, 'inst_net_buy', 0) > 0 else ('-' if getattr(s, 'inst_net_buy', 0) < 0 else '0')}"
        for s in candidates[:_BATCH_SIZE]
    )
    return hashlib.md5("|".join(key_parts).encode()).hexdigest()

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
- **오늘의 주도섹터 대장주 3종목을 선점**하는 것이 최우선 목표입니다.
- 선점한 종목은 하루 종일 눌릴 때 사고 올라갈 때 파는 반복 매매로 운용됩니다.
- 따라서 모멘텀이 하루 종일 유지될 수 있는 종목을 골라야 합니다.
- 과열·리스크 신호가 있는 종목은 과감히 제외합니다.
- Hot List에 오른 종목은 매매팀이 실제 주문을 실행하므로 정확성이 중요합니다.

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
- 모멘텀점수: 거래량·MACD·BB폭·OBV·거래대금·신고가 종합 점수(0~130). 높을수록 신호 품질 우수
- 거래대금: 당일 누적 거래대금(억원). 후보 종목은 이미 30억 이상 필터됨. 100억↑=충분한 유동성, 30~100억=슬리피지 주의 (진입 가능하나 reason에 "소형주" 명시)
- 120일신고가(🚀): 직전 120일 최고가 돌파 — 위에 저항선 없음, 추가 상승 여력 가장 큰 구간
- 당일범위위치(day_range): (현재가-당일저가)/(당일고가-당일저가). 0=저가권(눌림 반등 유리), 0.9↑=고가권(추격 주의)
- 외인순매수: 당일 외국인 순매수량(주). 양수=외인 매수우위, 음수=외인 매도우위. 절대값이 클수록 신호 강도 높음
- 기관순매수: 당일 기관 순매수량(주). 양수=기관 매수우위. 외인+기관 동시 양수=가장 강한 수급 신호
- 섹터 태그: 🔥=당일 강세 섹터 (KOSPI 초과수익 상위 3개), 🧊=약세 섹터 (하위 3개). 동일 점수이면 🔥 종목 우선 선정
- 조건 태그: 거래량N배 / 가격+N% / BB돌파 (동시 충족할수록 신호 강도 높음)

### 시장 컨텍스트 해석
- 국내 시장점수: -1.0(극약세) ~ +1.0(극강세). 0.0 이상=중립 이상
- 글로벌 리스크: 0(완전 안전) ~ 10(극도 위험). 7↑=보수적 선정 필요
- 현재 시각: 제공된 시각을 참고해 아래 시간대 전략을 적용
  - 09:00~10:30 (오프닝): 갭업·오프닝급락 우선 — 당일 방향성 확정 구간, 신호 신뢰도 최고
  - 10:30~11:30 (전반전): 눌림목·시장강세 편승 주력 — 추세 확립 구간
  - 11:30~13:30 (점심): 수급 활동 저조 — 종목 수 줄이고 신호 강도 요건 높임 (거래량 2배↑ 필요)
  - 13:30~14:00 (후반전): 강한 모멘텀만 — 거래량 3배↑ + RSI 55~70 + OBV 양수 동시 필요
  - 14:00↑ (마감 근접): 신규 선정 최소화 — 갭업 외 모든 전략 위험도 급상승, 가급적 기존 종목 정리 우선

## 매매 전략 선택

**핵심 철학**: 9시 장 시작 → 오늘 주도섹터 파악 → 대장주 3종목 선점 → 하루 종일 반복 매매
갭업 여부보다 "이 종목이 오늘 하루 모멘텀을 유지할 수 있는가"가 판단 기준입니다.
Hot List 안에서 종목마다 다른 전략을 적용할 수 있습니다.

### 1순위: 시장 주도주 선점 (⭐ 태그 종목)
**언제**: 종목 목록에 ⭐주도주 태그가 붙은 종목이 있을 때
- ⭐주도주 = 전체 시장 거래대금 상위 종목. 오늘 시장 자금이 실제로 몰리는 곳
- OBV↑ + RSI 82 이하 + 외인 or 기관 순매수이면 즉시 선정 (기술적 조건 완화 허용)
- 거래대금이 크다는 것 자체가 유동성·관심도·세력 집중 확인 신호
- ⭐주도주이면서 🔥주도섹터까지 겹치면 가장 강한 신호
**signal_type**: gap_up_breakout (갭업 동반) 또는 sector_momentum

### 2순위: 주도섹터 대장주 선점 (🔥 섹터 태그 종목)
**언제**: 시장 컨텍스트에 🔥 주도섹터가 제시된 경우 (⭐주도주 슬롯 여유 있을 때)
- 🔥섹터 종목 중 모멘텀점수 최상위 + OBV↑ + 거래량 2배↑ 조합
- 갭업 없어도 선정 가능, 갭업 +3%↑이 겹치면 신호 강도 더 강함
- RSI 82 이하 + 외인 또는 기관 순매수 동반이면 확정
**signal_type**: gap_up_breakout 또는 sector_momentum

### 2순위: 눌림목 반등매매 (pullback_rebound)
**언제**: 전일 강세 후 오늘 눌려있는 종목 (섹터 무관하게 적용 가능)
아래 중 3개 이상 충족 시 선정:
- 전일 대비 등락 -1% ~ -5% (오늘 소폭~중폭 하락)
- 장중등락(시가대비) 양수 또는 day_range 0.30 이하 저가권에서 반등 중
- OBV기울기 양수 (하락에도 매수세 유입 — 세력 매집)
- MACD 히스토그램 음수이지만 전봉 대비 증가 (수렴 시작)
- 거래량 1.5배↑ + 외인/기관 순매수 동반
**매매 특징**: 트레일링 스탑 적용, 모멘텀 유지 시 수익 계속 끌고감.

### 3순위: 오프닝 급락 반등 (opening_plunge_rebound) — 09:00~10:30 전용
**언제**: 시가 대비 -3% 이상 급락 후 반등 중
- 장중등락(시가대비) -3% 이하 후 day_range 0.20 이상 회복
- OBV기울기 양수 또는 급반등 거래량
- 전일비 등락률 무관 — 시가 급락 후 V자 반등이 핵심
**매매 특징**: 트레일링 스탑 적용, 반등 모멘텀 지속 시 끌고감

### 4순위: 외인+기관 동시 수급 (market_momentum) — 섹터 무관
**언제**: KOSPI 등락과 무관하게 외인+기관이 동시에 사고 있는 종목
- 외인+기관 **동시** 순매수 (핵심 조건)
- 등락 -1%~+6% | RSI 40~72 | 거래량 2배↑ + MA20 위 또는 근접
- 하락장에서 이 패턴이면 오히려 신호 강도 더 높음

### 기타 보조 신호 (슬롯 여유 있을 때만)
- 거래량 3배↑ + 가격 상승 + MACD 양수: volume_surge
- BB상단 돌파 + RSI 60↑ + MA20 위: breakout
- MACD 수렴 중 + RSI 50~65: momentum

### 신고가 돌파 우선순위
- 🚀120일신고가 표시 종목: 위에 저항선 없음 — 동일 점수 종목 중 우선 선정
- 갭업 + 🚀120일신고가 조합: 가장 강한 단타 신호 (세력 돌파 + 저항선 부재)
- 거래대금이 클수록 체결 용이 — 같은 조건이면 거래대금 큰 종목 우선

### 수급 보조 판단 (모든 전략 공통)
- 외인+기관 동시 순매수: 가장 강한 확인 신호 — 최우선 선정
- 외인+기관 동시 순매도: 기술적 신호 무관하게 선정 보류
- 외인 단독 강한 순매도 (거래량 10%↑): 기술적 신호만으로 선정 불가

## RSI 과열 구간별 처리 (차단 대신 포지션 조정)
- RSI 72~82 구간: 선정 가능하나 reason에 "RSI과열_포지션50%" 명시 → 매매팀이 1차 매수 비중 50%로 축소, 손절 1.5%로 타이트하게
- RSI 82↑ (일반 모멘텀): 완전 차단 (극단적 과열 — 폭락 리스크)
- RSI 82~95 (갭업+OBV양수): **선정 가능** — 테마 주도 갭업은 RSI 고점에서도 당일 추가 상승 패턴. reason에 "RSI과열_포지션50%" 명시 필수
- RSI 95↑ (갭업 포함 모든 종목): 완전 차단
- StochRSI 85~87: reason에 "StochRSI과매수" 명시 (선정은 허용, 주의 표시)
- StochRSI 88↑ (갭업 종목 제외): 매매팀 Gate 4.2에서 자동 차단됨 — 선정해도 진입 불가이므로 선정 보류 권고
- OBV기울기 음수 + RSI 70↑: 가격 상승이 매수세 없이 공매도 세력에 의한 쇼트커버 가능성 → 선정 보류 권고

## 거래량 최소 기준 (종목 품질 필터)
- 갭업/눌림목/오프닝급락 아닌 일반 모멘텀 종목: **거래량비 2.0x 미만이면 선정 불가**
  → 수급 없는 일시적 가격 반등은 당일 내 되돌림 위험이 높음 (KEC 패턴)
- 갭업/눌림목/오프닝급락은 거래량비 1.2x 이상이면 허용 (이미 전략 특성상 거래량 기준 완화)

## 제외 기준 (하나라도 해당 시 선정 불가)
- RSI 82↑: 극단적 과열 — 즉각 하락 전환 위험 높음
- 가격 하락 중 + MACD 히스토그램 하락: 명확한 하락 추세
- OBV기울기 음수 + 가격 상승 + RSI 70↑: 수급 없는 가짜 상승
- 거래량 없이 가격만 급등 (거래량비율 < 2.0): 유동성 부족
- 글로벌 리스크 9↑ 시: RSI 60↑ 종목만 선정 (전쟁·금융위기급 보수 모드)
- 외인+기관 동시 순매도: 기술적 신호와 무관하게 선정 보류 (세력 이탈 가능성)
- 외인 단독 강한 순매도 (음수 규모가 거래량의 10%↑): 기술적 신호만으로 선정 불가
- 당일범위위치(day_range) 0.90↑ + 등락 +3%↑: 당일 고가권 추격 매수 — 선정 자제
  (예외: BB돌파 + OBV↑ 동반 시 선정 가능, reason에 "고가권돌파" 명시)

## 시장 상황별 선정 강도 조절
- 시장점수 -0.2↑: 최대 3종목 선정 (소수 집중 — 잘 고른 3종목 반복 매매)
- 시장점수 -0.2↓: 최대 2종목 (약세장 리스크 절감)
- 어느 경우에도 3종목을 초과하지 않는다

## 섹터 수급 활용 원칙
- 🔥강세섹터 + 강한 기술지표 조합: 동일 신뢰도 후보 중 최우선 선정
- 🧊약세섹터: 선정 가능하나 reason에 "약세섹터" 명시. 수급 이탈 섹터에서의 개별 상승은 지속성 약할 수 있음
- 섹터 구분 없이(기타): 기술지표만으로 판단

## 슬롯 개념
매일 3개 슬롯을 운영합니다. 각 슬롯은 역할이 다르며, 하루 종일 그 종목만 집중 매매합니다.

- **leader (주도주)**: 오늘 시장 자금이 가장 많이 몰리는 종목. 장 시작과 동시에 진입.
  **[조건]** 후보 목록에서 거래대금 상위 5위 안에 드는 종목 중 모멘텀점수가 가장 높은 종목.
  거래대금 1위일 필요는 없지만 반드시 상위 5위 안에 들어야 함. 거래대금 500억 미만 완전 배제.
  OBV↑ + RSI 82 이하 + 하루 종일 방향성이 유지될 것으로 판단되는 종목.

- **breakout (신고가 돌파매매)**: 장 시작부터 거래가 폭발하며 최근 고점·신고가를 향해 달리는 종목.
  **[핵심 조건]** 🚀신고가 태그 or at_new_high + 거래량 3배↑. 갭업 +3% 조건 불필요.
  어제 신고가 후 오늘 재돌파 시도 or 장중 신고가 달성 모두 해당.
  조건 불충분하면 null 반환.

- **pullback (장중조정 재진입)**: 오늘 장 시작부터 강하게 슈팅한 종목의 첫 조정 타이밍에 진입.
  **[기존 "전일 대비 -1%~-5%" 조건 무시]** 오늘 장중 급등 후 일시 조정이 핵심.
  오늘 +5%↑ 상승 후 day_range_pos ≤ 0.65로 눌린 종목 — 다음 슈팅 재개 임박 신호 포착.
  MACD buy_pre 전환 + 거래량 감소→재증가 = 진입 시점. 조건 불충분하면 null 반환.

## 응답 규칙
- JSON만 출력합니다. 설명이나 주석을 추가하지 않습니다.
- 슬롯에 적합한 종목이 없으면 해당 슬롯을 null로 반환합니다 (억지로 채우지 않음).
- signal_type은 반드시 다음 중 하나:
  sector_momentum | gap_up_breakout | breakout | pullback_rebound | market_momentum
  | opening_plunge_rebound | volume_surge | momentum
- 선정 이유는 한국어로 20자 이내.
- 동일 종목을 두 슬롯에 배정하지 않습니다.

## 출력 형식
{
  "slots": {
    "leader":   {"ticker": "<6자리>", "signal_type": "<타입>", "reason": "<이유>"},
    "breakout":  {"ticker": "<6자리>", "signal_type": "<타입>", "reason": "<이유>"},
    "pullback":  {"ticker": "<6자리>", "signal_type": "<타입>", "reason": "<이유>"}
  }
}
슬롯에 적합한 종목이 없으면: "leader": null"""


def _build_user_message(
    candidates: list[StockSnapshot],
    market_score: float,
    global_risk_score: int,
    max_hot_list: int,
    semiconductor_alert: str = "",
    kospi_chg_pct: float = 0.0,
    hot_sectors: list[str] | None = None,
    leader_context: str = "",
    leader_tickers: set[str] | None = None,
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
        new_high_str = "🚀120일신고가" if getattr(s, "at_new_high", False) else ""
        trading_val = getattr(s, "trading_value", 0)
        tv_str = f"거래대금{trading_val/1e8:.0f}억" if trading_val > 0 else ""
        try:
            from src.infra.sector_rotation import get_sector
            s_sector = get_sector(s.ticker) or "기타"
        except Exception:
            s_sector = "기타"
        sector_tag = "🔥" if (hot_sectors and s_sector in hot_sectors) else ""
        sector_str = f"섹터[{sector_tag}{s_sector}]"
        leader_tag = " ⭐주도주" if (leader_tickers and s.ticker in leader_tickers) else ""
        lines.append(
            f"- {s.ticker}({s.name}): "
            f"등락{s.change_pct:+.1f}%{' [' + gap_str + ']' if gap_str else ''} | {intraday_str} | RSI {s.rsi:.0f} | "
            f"MACD히스토그램 {s.macd_hist:+.4f} | BB위치 {s.bb_position:.2f} | "
            f"MA20{'위' if s.above_ma20 else '아래'} | {obv_str} | {bbw_str} | {srsi_str} | {mscore_str} | {drp_str}"
            f"{' | ' + tv_str if tv_str else ''}{' | ' + new_high_str if new_high_str else ''}"
            f" | {sector_str}{leader_tag}{supply_str} | {flag_str}"
        )
    stock_block = "\n".join(lines)

    semiconductor_line = f"\n- ⚠️ {semiconductor_alert}" if semiconductor_alert else ""
    # 주도섹터 라인
    if hot_sectors:
        sector_line = f"\n- 🔥 오늘 주도섹터: {', '.join(hot_sectors)} — 이 섹터 대장주를 1순위 선정"
    else:
        sector_line = ""
    # 주도주 라인 (전체 시장 거래대금 기준)
    leader_line = f"\n\n## 오늘의 시장 주도주 (전체 시장 거래대금 순위 기준)\n{leader_context}" if leader_context else ""
    # 전략 선택 힌트
    if kospi_chg_pct >= 0.7:
        strategy_hint = f"KOSPI {kospi_chg_pct:+.1f}% 상승 → 주도섹터 대장주·눌림목 적극 선점"
    elif kospi_chg_pct <= -0.7:
        strategy_hint = f"KOSPI {kospi_chg_pct:+.1f}% 하락 → 오프닝급락반등·외인기관 동시순매수 종목으로 한정, 주도섹터 집중"
    else:
        strategy_hint = f"KOSPI {kospi_chg_pct:+.1f}% 보합 → 주도섹터 대장주 우선, 눌림목·돌파 순으로 선정"
    from datetime import datetime as _dt
    _now_str = _dt.now().strftime("%H:%M")
    return (
        f"## 현재 시장 컨텍스트\n"
        f"- 현재 시각: {_now_str}\n"
        f"- 국내 시장점수: {market_score:+.2f}\n"
        f"- KOSPI 등락: {kospi_chg_pct:+.1f}%\n"
        f"- 글로벌 리스크: {global_risk_score}/10\n"
        f"- 전략 힌트: {strategy_hint}"
        f"{sector_line}"
        f"{semiconductor_line}"
        f"{leader_line}\n\n"
        f"## 후보 종목 ({len(candidates)}개)\n"
        f"(⭐주도주 = 전체 시장 거래대금 상위 종목)\n"
        f"{stock_block}\n\n"
        f"## 슬롯 배정 요청\n"
        f"채워야 할 슬롯: {', '.join(max_hot_list) if isinstance(max_hot_list, list) else 'leader, breakout, pullback'}\n"
        f"⭐주도주 태그 = 전체 시장 거래대금 상위 종목 → leader 슬롯 확정 배정 (거래대금 1위가 조건 충족 시 반드시 leader).\n"
        f"슬롯별 조건에 맞는 종목이 없으면 해당 슬롯은 null로 반환."
    )


_ALL_SLOTS = ["leader", "breakout", "pullback"]


def analyze(
    scan: UniverseScan,
    market_score: float = 0.0,
    global_risk_score: int = 5,
    global_key_events: list[str] | None = None,
    kospi_chg_pct: float = 0.0,
    slots_to_fill: list[str] | None = None,
    exclude_tickers: set[str] | None = None,  # 이미 다른 슬롯에 배정된 종목 제외
) -> dict[str, dict | None]:
    """
    후보 종목을 Claude에 보내 슬롯별 종목 선정.

    Args:
        scan: 유니버스 스캔 결과
        market_score: 국내 시황팀이 산출한 시장 점수 (-1.0~1.0)
        global_risk_score: 글로벌 시황팀이 산출한 리스크 점수 (0~10)
        slots_to_fill: 채워야 할 슬롯 목록 (None=전체 3개)

    Returns:
        {"leader": {...}, "breakout": {...}, "pullback": None, ...}
        슬롯별 {"ticker", "signal_type", "reason"} 또는 None
    """
    global _cache_key, _cache_result, _cache_ts

    target_slots = slots_to_fill if slots_to_fill else _ALL_SLOTS
    _empty_result: dict[str, dict | None] = {s: None for s in target_slots}

    candidates = scan.candidates
    if not candidates:
        logger.info("후보 종목 없음 — 슬롯 비어있음")
        return _empty_result

    # 이미 다른 슬롯에 배정된 종목 제외 (중복 배정 방지)
    if exclude_tickers:
        candidates = [c for c in candidates if c.ticker not in exclude_tickers]
        if not candidates:
            logger.info("exclude 후 후보 없음 — 슬롯 비어있음")
            return _empty_result

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
            logger.info("일봉 MACD 필터 후 후보 없음 — 슬롯 비어있음")
            return _empty_result

    # 후보 정렬: momentum_score + RS + 외인기관 보조
    # momentum_score에 이미 수급이 반영돼 있지만, RS(상대강도)는 별도 보조 키로 추가
    def _sort_key(s: StockSnapshot) -> float:
        base = s.momentum_score if s.momentum_score > 0 else (
            (s.is_volume_surge and s.is_price_surge) * 50
            + s.is_breakout * 20
            + s.volume_ratio * 5
            + (s.obv_slope > 0) * 10
        )
        # RS 보조 (+3 / -3): KOSPI 대비 강세 종목 우선
        rs_bonus = 3.0 if getattr(s, "rs_daily", 0.0) >= 2.0 else (
            -3.0 if getattr(s, "rs_daily", 0.0) <= -2.0 else 0.0
        )
        return base + rs_bonus

    candidates_sorted = sorted(candidates, key=_sort_key, reverse=True)
    batch = candidates_sorted[:_BATCH_SIZE]

    # ── 오늘 주도섹터 조회 ───────────────────────────────────────
    _hot_sectors: list[str] = []
    try:
        from src.infra.sector_rotation import get_hot_sectors
        _hot_sectors = get_hot_sectors(3)
    except Exception:
        pass

    # ── 오늘 시장 주도주 조회 (전체 시장 거래대금 순위) ─────────
    _leader_context: str = ""
    _leader_tickers: set[str] = set()
    try:
        from src.infra.market_leaders import get_leader_context_str, get_leader_tickers
        _leader_context = get_leader_context_str()
        _leader_tickers = get_leader_tickers()
    except Exception:
        pass

    # ── 호출 중복 제거 캐시 체크 (전체 슬롯 요청 시만) ──────────
    fp = _candidates_fingerprint(batch)
    cache_key_full = fp + "|" + ",".join(sorted(target_slots))
    if (cache_key_full == _cache_key
            and (time.time() - _cache_ts) < _CACHE_TTL_SEC
            and _cache_result):
        logger.info(f"슬롯 캐시 히트 — Claude 호출 생략 ({target_slots} 재사용)")
        return {s: _cache_result.get(s) for s in target_slots}

    logger.info(
        f"Claude 슬롯 분석 시작 — 후보 {len(candidates)}개 중 {len(batch)}개 전송 "
        f"(채울 슬롯: {target_slots})"
    )

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=800,
            temperature=settings.CLAUDE_TEMPERATURE,
            timeout=30.0,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {
                    "role": "user",
                    "content": _build_user_message(
                        batch, market_score, global_risk_score, target_slots,
                        semiconductor_alert=semiconductor_alert,
                        kospi_chg_pct=kospi_chg_pct,
                        hot_sectors=_hot_sectors,
                        leader_context=_leader_context,
                        leader_tickers=_leader_tickers,
                    ),
                }
            ],
        )
        raw = response.content[0].text.strip()
        raw = _extract_json(raw)
        parsed = json.loads(raw)
        slots_raw = parsed.get("slots", {})

        usage = response.usage
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
        cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
        if cache_read:
            logger.debug(f"프롬프트 캐시 히트: {cache_read}토큰 절감")
        elif cache_write:
            logger.debug(f"프롬프트 캐시 저장: {cache_write}토큰")

        # 유효성 검증 — ticker 6자리 + 중복 제거
        seen: set[str] = set()
        final: dict[str, dict | None] = {}
        for slot in _ALL_SLOTS:
            if slot not in target_slots:
                continue
            entry = slots_raw.get(slot)
            if not entry or not isinstance(entry, dict):
                final[slot] = None
                continue
            ticker = str(entry.get("ticker", "")).zfill(6)
            if not ticker or ticker == "000000" or ticker in seen:
                final[slot] = None
                continue
            seen.add(ticker)
            final[slot] = {
                "ticker":      ticker,
                "signal_type": str(entry.get("signal_type", "sector_momentum")),
                "reason":      str(entry.get("reason", ""))[:30],
                "slot":        slot,
            }

        # 캐시 갱신 (전체 슬롯 요청 시만)
        if set(target_slots) == set(_ALL_SLOTS):
            _cache_key    = cache_key_full
            _cache_result = final
            _cache_ts     = time.time()

        filled = [s for s, v in final.items() if v]
        logger.info(f"슬롯 확정: {[(s, final[s]['ticker']) for s in filled]}")
        return final

    except json.JSONDecodeError as e:
        raw_preview = locals().get("raw", "")[:200]
        logger.error(f"Claude 응답 파싱 실패: {e} | raw='{raw_preview}'")
        return _fallback_slots(candidates_sorted, target_slots)
    except Exception as e:
        logger.error(f"Claude API 오류: {type(e).__name__}: {e}")
        from src.utils.notifier import check_claude_error
        check_claude_error(e, "국내 주식 슬롯 선정")
        return _fallback_slots(candidates_sorted, target_slots)


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


def _fallback_slots(
    candidates: list[StockSnapshot],
    target_slots: list[str],
) -> dict[str, dict | None]:
    """Claude 실패 시 슬롯별 자동 선정 (leader / breakout / pullback)."""
    from datetime import datetime
    _now_hm = int(datetime.now().strftime("%H%M"))

    # 주도섹터 + 시장 주도주 파악
    try:
        from src.infra.sector_rotation import get_hot_sectors, get_sector
        _fb_hot_sectors = set(get_hot_sectors(3))
    except Exception:
        _fb_hot_sectors = set()
        def get_sector(_t: str) -> str: return ""  # noqa: E306

    try:
        from src.infra.market_leaders import get_leader_tickers as _get_lt
        _fb_leader_tickers = _get_lt()
    except Exception:
        _fb_leader_tickers = set()

    # ── leader 슬롯 후보 ──────────────────────────
    market_leaders = [
        s for s in candidates
        if s.ticker in _fb_leader_tickers and s.obv_slope > 0 and s.rsi <= 82
    ]
    sector_leaders = [
        s for s in candidates
        if _fb_hot_sectors
        and get_sector(s.ticker) in _fb_hot_sectors
        and s.obv_slope > 0 and s.rsi <= 82 and s.volume_ratio >= 2.0
        and s not in market_leaders
    ]

    # ── breakout 슬롯 후보 — 신고가 돌파 + 거래량 3배↑ ──────────
    _ml_set = {s.ticker for s in market_leaders + sector_leaders}
    breakout_cands = [
        s for s in candidates
        if s.at_new_high and s.volume_ratio >= 3.0 and s.obv_slope > 0 and s.rsi <= 90
        and s.ticker not in _ml_set
    ]
    # 신고가 3배 없으면 신고가 + 2배↑로 완화
    if not breakout_cands:
        breakout_cands = [
            s for s in candidates
            if s.at_new_high and s.volume_ratio >= 2.0 and s.obv_slope > 0 and s.rsi <= 90
            and s.ticker not in _ml_set
        ]

    # ── pullback 슬롯 후보 — 오늘 급등 후 장중 눌림 ────────────
    pullback_cands = [
        s for s in candidates
        if s.change_pct >= 5.0 and s.day_range_pos <= 0.65
        and s.obv_slope > 0 and s.rsi <= 82
        and s.ticker not in _ml_set
    ]

    used: set[str] = set()
    result: dict[str, dict | None] = {s: None for s in _ALL_SLOTS}

    def _pick(pool: list[StockSnapshot], slot: str, sig: str, reason_fn) -> None:
        for s in sorted(pool, key=lambda x: x.momentum_score, reverse=True):
            if s.ticker in used:
                continue
            intra = getattr(s, "intraday_chg_pct", 0.0)
            used.add(s.ticker)
            result[slot] = {
                "ticker":      s.ticker,
                "signal_type": sig,
                "reason":      reason_fn(s, intra) + " (자동선정)",
                "slot":        slot,
            }
            return

    if "leader" in target_slots:
        _pick(
            market_leaders,
            "leader",
            "sector_momentum",
            lambda s, _i: f"⭐시장주도주·모멘텀{s.momentum_score:.0f}점·OBV↑",
        )
        if result["leader"] is None:
            _pick(
                sector_leaders,
                "leader",
                "sector_momentum",
                lambda s, _i: f"주도섹터[{get_sector(s.ticker)}]대장주·모멘텀{s.momentum_score:.0f}점·OBV↑",
            )
        # 최종 폴백: 거래대금 상위 + 모멘텀 최고인 후보 (거래 불가 상태 방지)
        if result["leader"] is None:
            _top_tv = [
                s for s in candidates
                if s.obv_slope > 0 and s.rsi <= 85
            ]
            _pick(
                sorted(_top_tv, key=lambda s: (s.trading_value, s.momentum_score), reverse=True),
                "leader",
                "sector_momentum",
                lambda s, _i: f"거래대금대장주·모멘텀{s.momentum_score:.0f}점·OBV↑(폴백)",
            )

    if "breakout" in target_slots:
        _pick(
            breakout_cands,
            "breakout",
            "gap_up_breakout",
            lambda s, _i: f"갭업{s.change_pct:+.0f}%·OBV{'↑' if s.obv_slope > 0 else '↓'}·거래량{s.volume_ratio:.1f}배",
        )

    if "pullback" in target_slots:
        _pick(
            pullback_cands,
            "pullback",
            "pullback_rebound",
            lambda s, i: f"눌림{s.change_pct:+.0f}%·장중{i:+.1f}%·OBV↑",
        )

    filled = [s for s, v in result.items() if v]
    logger.info(f"슬롯 자동선정(fallback): {[(s, result[s]['ticker']) for s in filled]}")
    return result
