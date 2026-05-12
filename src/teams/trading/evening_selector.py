"""⚠️ DEPRECATED (2026-05-12) — 자동 매매 모델에서 사용. 어시스턴트 모델 전환으로 호출되지 않음.
대체: src/teams/research/evening_review.py (15:40 회고).

[원본 docstring]
evening_selector.py — 전일 저녁 내일 매수 후보 5종목 선정 (방향 1 전략)

매일 장 마감 후 16:30에 실행.
오늘 하루 데이터를 바탕으로 Claude가 내일 시초가에 매수할 종목을 1~5순위로 선정.
09:01에 갭 + 호가 체크 후 가장 높은 순위의 합격 종목을 매수한다.

선정 기준 (v2):
  - RSI 45~65 (과열 아님, 상승 추세 중)
  - 거래량 비율 2.0배 이상 (강한 수급 신호)
  - 당일 등락 +0.5%~+6% (급등 제외)
  - 거래대금 100억 이상 (유동성 확보)
  - 당일 고점 위치 60% 이상 (오늘 강세 유지)
  - 체결강도 100 이상 (매수 우위)
  - OBV 상승 (거래량 누적 방향)
  - RS 당일/5일 모두 양수 (시장 대비 강세)
  - 외인 또는 기관 순매수 (수급 확인)
  - ticker_stats 과거 성과 검증 (5회↑ 이력 시 승률 50%↑ + 평균손절 -3% 이내)
  - 강세 섹터 소속 우선
  - 섹터 내 모멘텀 순위 반영
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.infra.kis_gateway import KISGateway, RequestPriority
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

_SYSTEM_PROMPT = """너는 국내 주식 퀀트 트레이더야.
이미 엄격한 정량 필터를 통과한 후보들을 보고 내일 시초가(09:00)에 매수할 종목을 1~5순위로 선정해줘.
1순위가 최선이고 5순위가 차선책이야. 내일 아침에 갭과 호가 상황을 확인 후 통과하는 가장 높은 순위 종목을 산다.

후보들은 이미 다음 기준을 모두 통과한 종목들이야:
- RSI 45~65, 거래량 2배↑, 등락 +0.5~+6%, 거래대금 100억↑
- 오늘 고점 위치 60%↑, 체결강도 100↑, OBV 상승
- RS 당일/5일 양수, 외인 또는 기관 순매수

순위 결정 시 우선순위:
1. 외인+기관 동시 순매수 (OR보다 AND가 훨씬 강한 신호)
2. 섹터 내 모멘텀 1~2위 (섹터에서 가장 강한 종목)
3. 섹터 자체가 KOSPI 대비 강세 (vs코스피 양수)
4. 과거 승률 60%↑ + 평균손익 플러스 (검증된 종목)
5. RS5일 수치 높을수록 (5일간 꾸준히 강세)
6. 체결강도 높을수록 (장 내내 매수 우위)
7. 같은 섹터 중복 피할 것 (분산 효과)
8. 신규 종목(이력 없음)은 다른 조건이 매우 강할 때만 선정

주의사항:
- 당일 급등(+5%↑)은 내일 차익실현 위험 — 같은 점수면 낮은 등락률 우선
- 섹터 내 꼴찌(모멘텀 최하위)는 섹터가 강해도 피할 것
- 후보가 2개 미만이면 기준을 낮추지 말고 있는 만큼만 반환

응답은 반드시 JSON 배열만 (5개 또는 가능한 만큼):
[
  {"rank": 1, "ticker": "종목코드 6자리", "name": "종목명", "reason": "선정 이유 1~2줄"},
  {"rank": 2, "ticker": "...", "name": "...", "reason": "..."},
  ...
]

후보가 아예 없거나 시장이 매우 불안하면: []"""


def run_evening_selection() -> None:
    """장 마감 후 내일 매수 후보 선정. scheduler에서 16:30에 호출."""
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    dow = (date.today() + timedelta(days=1)).weekday()
    if dow >= 5:
        tomorrow = (date.today() + timedelta(days=7 - dow)).isoformat()

    # 이미 선정했으면 스킵
    existing = fetch_one("SELECT ticker FROM tomorrow_pick WHERE pick_date=? AND rank=1", (tomorrow,))
    if existing:
        logger.info(f"[저녁 선점] 이미 선정됨: {existing['ticker']} ({tomorrow})")
        return

    candidates = _build_candidates()
    if not candidates:
        logger.warning("[저녁 선점] 후보 없음 — 선정 건너뜀")
        notify("⚠️ <b>[저녁 선점]</b> 내일 매수 후보 없음 — 오늘 시장 조건 미충족")
        return

    picks = _ask_claude(candidates)

    if not picks:
        logger.info("[저녁 선점] Claude 선정 없음")
        return

    # 기준가 조회
    gw = KISGateway()
    ref_prices = _fetch_ref_prices(gw, [p["ticker"] for p in picks])

    for pick in picks:
        ticker = pick["ticker"]
        rank = pick["rank"]
        ref_price = ref_prices.get(ticker, 0.0)
        execute(
            """
            INSERT OR REPLACE INTO tomorrow_pick (pick_date, rank, ticker, name, reason, ref_price, status)
            VALUES (?, ?, ?, ?, ?, ?, 'pending')
            """,
            (tomorrow, rank, ticker, pick.get("name", ""), pick.get("reason", ""), ref_price),
        )

    # 알림
    top = picks[0]
    ref_str = f"{ref_prices.get(top['ticker'], 0):,.0f}원" if ref_prices.get(top["ticker"]) else "N/A"
    names_str = " / ".join(f"{p['rank']}. {p.get('name','')}" for p in picks)
    msg = (
        f"📌 <b>[내일 선점 완료]</b> {len(picks)}종목 / 후보 {len(candidates)}개 중 선정\n"
        f"📅 매수 예정: {tomorrow} 시초가 (갭+호가 통과 시)\n"
        f"🏆 순위: {names_str}\n"
        f"1순위 기준가: {ref_str}\n"
        f"📝 {top.get('reason','')}"
    )
    logger.info(f"[저녁 선점] {len(picks)}종목 선정 완료 → {tomorrow}")
    notify(msg)


def _build_candidates() -> list[dict]:
    """오늘 hot_list + ticker_stats + sector_strength 기반 엄격 필터 후보 구성."""
    rows = fetch_all(
        """
        SELECT
            h.ticker, h.name, h.rsi, h.volume_ratio, h.price_change_pct,
            h.momentum_score, h.rs_daily, h.rs_5d,
            h.frgn_net_buy, h.inst_net_buy, h.sector, h.signal_type,
            h.trading_value, h.day_range_pos, h.exec_strength,
            h.obv_slope, h.atr_pct,
            t.win_rate, t.avg_pnl_pct, t.total_trades,
            t.avg_win_pct, t.avg_loss_pct,
            ss.vs_kospi AS sector_vs_kospi
        FROM hot_list h
        LEFT JOIN ticker_stats t ON h.ticker = t.ticker
        LEFT JOIN sector_strength ss ON h.sector = ss.sector
        WHERE
            h.rsi BETWEEN 45 AND 65
            AND h.volume_ratio >= 2.0
            AND h.price_change_pct BETWEEN 0.5 AND 6.0
            AND h.trading_value >= 10000000000
            AND h.day_range_pos >= 0.6
            AND h.exec_strength >= 100.0
            AND h.obv_slope > 0
            AND h.rs_daily > 0
            AND h.rs_5d > 0
            AND (h.frgn_net_buy > 0 OR h.inst_net_buy > 0)
        ORDER BY h.momentum_score DESC, h.volume_ratio DESC
        LIMIT 30
        """,
    )

    today_losses = {
        r["ticker"]
        for r in fetch_all(
            "SELECT DISTINCT ticker FROM trades WHERE date=DATE('now','localtime') AND action='stop_loss'",
        )
    }

    candidates = []
    for r in rows:
        if r["ticker"] in today_losses:
            continue

        # ticker_stats 품질 필터: 5회 이상 이력이 있으면 성과 검증
        total_trades = int(r["total_trades"] or 0)
        if total_trades >= 5:
            win_rate = float(r["win_rate"] or 0)
            avg_loss = float(r["avg_loss_pct"] or 0)
            if win_rate < 0.5 or avg_loss < -3.0:
                continue  # 검증됐는데 성과 나쁜 종목 제외

        d = dict(r)
        d["both_buy"] = (int(r["frgn_net_buy"] or 0) > 0) and (int(r["inst_net_buy"] or 0) > 0)
        d["total_trades"] = total_trades
        candidates.append(d)

    # 섹터 내 모멘텀 순위 계산
    sector_groups: dict[str, list] = {}
    for c in candidates:
        sec = c.get("sector") or "기타"
        sector_groups.setdefault(sec, []).append(c)
    for group in sector_groups.values():
        group.sort(key=lambda x: float(x.get("momentum_score") or 0), reverse=True)
        for rank, item in enumerate(group, 1):
            item["sector_rank"] = rank
            item["sector_size"] = len(group)

    logger.info(
        f"[저녁 선점] 최종 후보 {len(candidates)}종목 "
        f"(섹터 {len(sector_groups)}개)"
    )
    return candidates


def _ask_claude(candidates: list[dict]) -> list[dict]:
    """Claude에게 5순위 후보 목록 요청. 반환: [{rank, ticker, name, reason}, ...]"""
    market = fetch_one(
        "SELECT market_score, direction FROM market_condition ORDER BY collected_at DESC LIMIT 1"
    )
    market_summary = ""
    if market:
        market_summary = (
            f"오늘 시장: 점수={market['market_score']}, 방향={market['direction']}"
        )

    prompt = f"{market_summary}\n\n후보 종목 목록 ({len(candidates)}개):\n"
    for i, c in enumerate(candidates, 1):
        total = c.get("total_trades") or 0
        if total >= 5:
            wr = float(c.get("win_rate") or 0)
            ap = float(c.get("avg_pnl_pct") or 0)
            stats_str = f"과거={wr*100:.0f}%승/{total}회/평균{ap:+.1f}%"
        else:
            stats_str = "과거=신규"

        if c.get("both_buy"):
            supply_str = "외인+기관동시"
        elif int(c.get("frgn_net_buy") or 0) > 0:
            supply_str = "외인만"
        else:
            supply_str = "기관만"

        sec_rank = c.get("sector_rank", "-")
        sec_size = c.get("sector_size", "-")
        sec_vs = float(c.get("sector_vs_kospi") or 0)
        sec_str = f"{c.get('sector','?')}(섹터내{sec_rank}/{sec_size}위 vs코스피{sec_vs:+.1f}%)"

        prompt += (
            f"{i}. {c['name']}({c['ticker']}) "
            f"RSI={float(c['rsi']):.0f} "
            f"등락={float(c['price_change_pct']):+.1f}% "
            f"거래량={float(c['volume_ratio']):.1f}배 "
            f"체결강도={float(c.get('exec_strength') or 0):.0f} "
            f"고점위치={float(c.get('day_range_pos') or 0):.0%} "
            f"모멘텀={float(c.get('momentum_score') or 0):.0f} "
            f"RS당일={float(c.get('rs_daily') or 0):+.1f}% "
            f"RS5일={float(c.get('rs_5d') or 0):+.1f}% "
            f"수급={supply_str} "
            f"섹터={sec_str} "
            f"{stats_str}\n"
        )

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_FAST,
            max_tokens=800,
            temperature=0,
            timeout=30.0,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()
        result = json.loads(raw)
        if not isinstance(result, list):
            return []
        valid = []
        for item in result:
            if item.get("ticker") and item.get("rank"):
                valid.append({
                    "rank": int(item["rank"]),
                    "ticker": str(item["ticker"]),
                    "name": str(item.get("name", "")),
                    "reason": str(item.get("reason", "")),
                })
        valid.sort(key=lambda x: x["rank"])
        return valid[:5]
    except Exception as e:
        logger.error(f"[저녁 선점] Claude 호출 실패: {e}")
        return []


def _fetch_ref_prices(gw: KISGateway, tickers: list[str]) -> dict[str, float]:
    """종목별 현재가 조회 (기준가 저장용)."""
    prices: dict[str, float] = {}
    for ticker in tickers:
        try:
            resp = gw.request(
                method="GET",
                path="/uapi/domestic-stock/v1/quotations/inquire-price",
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                tr_id="FHKST01010100",
                priority=RequestPriority.LOW,
            )
            price = float(resp.get("output", {}).get("stck_prpr", 0) or 0)
            if price > 0:
                prices[ticker] = price
        except Exception as e:
            logger.debug(f"[저녁 선점] {ticker} 기준가 조회 실패: {e}")
    return prices
