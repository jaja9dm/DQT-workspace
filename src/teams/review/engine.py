"""
engine.py — 일일 매매 복기 팀

역할:
  매일 장 마감 후 16:30에 자동 실행.
  오늘 발생한 모든 매매를 분석하고 무엇이 잘 됐는지, 무엇을 고쳐야 하는지
  Claude가 판단해 trade_review 테이블에 저장하고 Telegram으로 리포트한다.

시황 맥락 기능:
  - 당일 KOSPI/KOSDAQ 등락, 외인/기관 매매, 글로벌 리스크를 복기에 포함
  - market_regime 태그 (강세_외인주도 등) 저장 — 유사 시황에서 과거 대응 참조 가능
  - "이런 시장 상황에서 이런 전략이 효과적이었다"는 패턴 누적

출력:
  - trade_review 테이블에 당일 복기 레코드 저장 (market_context JSON 포함)
  - Telegram 복기 요약 발송
  - 연구소가 내일 전략 파라미터 조정에 참고

자기 개선 흐름:
  오늘 복기 → improvements JSON → 내일 _ask_claude / opening_gate 프롬프트 참고
  → 장기적으로 전략 파라미터 자동 튜닝의 기초 데이터로 활용
"""

from __future__ import annotations

import json
from datetime import date, datetime

import anthropic

from src.config.settings import settings
from src.infra.database import execute, fetch_all, fetch_one
from src.utils.logger import get_logger
from src.utils.notifier import notify

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)


# ──────────────────────────────────────────────
# 메인 진입점
# ──────────────────────────────────────────────

def run_daily_review() -> dict | None:
    """
    오늘의 매매를 복기하고 trade_review에 저장.

    Returns:
        저장된 리뷰 딕셔너리 또는 None (매매 없는 날)
    """
    today = str(date.today())
    logger.info(f"=== 일일 복기 시작: {today} ===")

    # 이미 오늘 복기가 있으면 스킵 (재실행 방지)
    existing = fetch_one("SELECT id FROM trade_review WHERE review_date = ?", (today,))
    if existing:
        logger.info("오늘 복기 이미 완료됨 — 스킵")
        return None

    trades = _load_todays_trades(today)
    if not trades:
        logger.info("오늘 매매 내역 없음 — 복기 스킵")
        notify("📋 <b>[일일 복기]</b> 오늘 매매 없음")
        return None

    # 기초 통계
    stats = _calc_stats(trades)

    # 포지션 스냅샷 (오늘 보유 종목의 가격 흐름 맥락)
    snapshots = _load_snapshots_context(today, [t["ticker"] for t in trades])

    # 당일 시황 컨텍스트 (KOSPI/KOSDAQ/외인/글로벌 리스크)
    market_ctx = _load_market_context(today)

    # 유사 시황 패턴 과거 복기 (데이터 쌓이면 참조용)
    similar_days = _load_similar_market_days(market_ctx, days_back=60)

    # Claude 분석
    review = _ask_claude_review(today, trades, stats, snapshots, market_ctx, similar_days)

    # DB 저장
    _save_review(today, stats, review, market_ctx)

    # Telegram 발송
    _notify_review(today, stats, review, market_ctx)

    logger.info(f"일일 복기 완료 — 매매 {stats['total']}건, 수익 {stats['win']}건, 손실 {stats['loss']}건")
    return review


# ──────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────

def _load_todays_trades(today: str) -> list[dict]:
    """오늘 체결된 매매 내역 전체 조회 (매수가 연결 포함)."""
    rows = fetch_all(
        """
        SELECT ticker, name, action, order_type, exec_price,
               quantity, pnl, pnl_pct, signal_source, strategy_id,
               created_at, filled_at
        FROM trades
        WHERE date = ?
          AND status IN ('filled', 'pending')
        ORDER BY created_at ASC
        """,
        (today,),
    )
    return [dict(r) for r in rows] if rows else []


def _load_market_context(today: str) -> dict:
    """
    당일 시황 데이터 수집.
    market_condition (국내) + global_condition (글로벌) 최신 레코드 활용.
    Returns:
        {
            "kospi_chg": float,          # 당일 KOSPI 등락률
            "kosdaq_chg": float,         # 당일 KOSDAQ 등락률
            "foreign_dir": str,          # "매수" | "매도" | "중립"
            "institutional_dir": str,    # "매수" | "매도" | "중립"
            "market_score": float,       # -1.0 ~ 1.0
            "market_direction": str,     # bullish|neutral|bearish
            "global_risk": int,          # 0~10
            "korea_outlook": str,        # positive|neutral|negative
            "leading_force": str,        # foreign|institutional|individual|mixed
            "summary": str,              # 국내 시황 한줄 요약
        }
    """
    ctx = {
        "kospi_chg": 0.0, "kosdaq_chg": 0.0,
        "foreign_dir": "중립", "institutional_dir": "중립",
        "market_score": 0.0, "market_direction": "neutral",
        "global_risk": 5, "korea_outlook": "neutral",
        "leading_force": "mixed", "summary": "",
    }
    try:
        mc = fetch_one(
            "SELECT * FROM market_condition WHERE date(created_at) = ? ORDER BY created_at DESC LIMIT 1",
            (today,),
        )
        if mc:
            mc = dict(mc)
            ctx["market_score"]      = mc.get("market_score", 0.0) or 0.0
            ctx["market_direction"]  = mc.get("market_direction", "neutral") or "neutral"
            fnet = mc.get("foreign_net_buy_bn") or 0
            inet = mc.get("institutional_net_buy_bn") or 0
            ctx["foreign_dir"]       = "매수" if fnet > 100 else "매도" if fnet < -100 else "중립"
            ctx["institutional_dir"] = "매수" if inet > 50 else "매도" if inet < -50 else "중립"
            summary_json = mc.get("summary") or "{}"
            try:
                s = json.loads(summary_json)
                ctx["kospi_chg"]    = s.get("kospi", 0.0) or 0.0
                ctx["kosdaq_chg"]   = s.get("kosdaq", 0.0) or 0.0
                ctx["leading_force"] = s.get("leading_force", "mixed") or "mixed"
                ctx["summary"]      = s.get("analysis", "") or ""
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"국내 시황 조회 실패: {e}")

    try:
        gc = fetch_one(
            "SELECT * FROM global_condition WHERE date(created_at) = ? ORDER BY created_at DESC LIMIT 1",
            (today,),
        )
        if gc:
            gc = dict(gc)
            ctx["global_risk"]   = int(gc.get("global_risk_score") or 5)
            ctx["korea_outlook"] = gc.get("korea_market_outlook", "neutral") or "neutral"
    except Exception as e:
        logger.debug(f"글로벌 시황 조회 실패: {e}")

    return ctx


def _load_similar_market_days(market_ctx: dict, days_back: int = 60) -> list[dict]:
    """
    오늘과 유사한 시황 패턴의 과거 복기 조회.
    market_direction + foreign_dir 기준으로 유사 날을 찾아 참조용으로 반환.
    Returns:
        최대 3개의 과거 복기 요약 리스트
    """
    direction  = market_ctx.get("market_direction", "neutral")
    foreign    = market_ctx.get("foreign_dir", "중립")

    try:
        rows = fetch_all(
            """
            SELECT review_date, total_pnl, win_trades, loss_trades,
                   summary, market_context
            FROM trade_review
            WHERE market_context IS NOT NULL
              AND created_at >= datetime('now', ? || ' days')
            ORDER BY created_at DESC
            LIMIT 10
            """,
            (f"-{days_back}",),
        )
        if not rows:
            return []

        similar = []
        for r in rows:
            r = dict(r)
            try:
                mc = json.loads(r.get("market_context") or "{}")
                # 시황 방향 + 외인 방향이 같은 날만 유사로 간주
                if mc.get("market_direction") == direction and mc.get("foreign_dir") == foreign:
                    similar.append({
                        "date": r["review_date"],
                        "total_pnl": r.get("total_pnl") or 0,
                        "win": r.get("win_trades") or 0,
                        "loss": r.get("loss_trades") or 0,
                        "summary": r.get("summary") or "",
                        "global_risk": mc.get("global_risk", 5),
                    })
                    if len(similar) >= 3:
                        break
            except Exception:
                continue
        return similar
    except Exception as e:
        logger.debug(f"유사 시황 조회 실패: {e}")
        return []


def _load_snapshots_context(today: str, tickers: list[str]) -> dict[str, list[dict]]:
    """
    오늘 포지션 스냅샷 — 각 종목의 가격 흐름 (최대 10개 포인트).
    복기 맥락 제공용.
    """
    ctx: dict[str, list[dict]] = {}
    for ticker in set(tickers):
        rows = fetch_all(
            """
            SELECT pnl_pct, current_price, snapshot_at
            FROM position_snapshot
            WHERE ticker = ?
              AND date(snapshot_at) = ?
            ORDER BY snapshot_at ASC
            LIMIT 10
            """,
            (ticker, today),
        )
        if rows:
            ctx[ticker] = [dict(r) for r in rows]
    return ctx


# ──────────────────────────────────────────────
# 통계 계산
# ──────────────────────────────────────────────

def _calc_stats(trades: list[dict]) -> dict:
    """매매 기초 통계 + 종목별 매수/매도 페어링."""
    sell_trades = [t for t in trades if t["action"] in ("sell", "stop_loss", "take_profit", "time_cut", "time_cut")]
    buy_trades  = [t for t in trades if t["action"] == "buy"]

    win_trades  = [t for t in sell_trades if (t.get("pnl_pct") or 0) > 0]
    loss_trades = [t for t in sell_trades if (t.get("pnl_pct") or 0) <= 0]
    total_pnl   = sum((t.get("pnl") or 0) for t in sell_trades)

    best  = max(sell_trades, key=lambda t: t.get("pnl_pct") or 0) if sell_trades else None
    worst = min(sell_trades, key=lambda t: t.get("pnl_pct") or 0) if sell_trades else None

    # 종목별 페어링 (매수가 → 매도가 연결)
    ticker_summary: dict[str, dict] = {}
    for t in buy_trades:
        tk = t["ticker"]
        if tk not in ticker_summary:
            ticker_summary[tk] = {
                "ticker": tk,
                "name": t.get("name") or tk,
                "buy_price": t.get("exec_price") or 0,
                "buy_qty": t.get("quantity") or 0,
                "sell_price": None,
                "sell_qty": 0,
                "pnl_pct": None,
                "pnl_amt": 0.0,
                "action": "보유중",
                "status": "holding",
            }
        else:
            # 추가 매수: 평균단가 갱신
            prev = ticker_summary[tk]
            total_qty = prev["buy_qty"] + (t.get("quantity") or 0)
            if total_qty > 0:
                prev["buy_price"] = (
                    prev["buy_price"] * prev["buy_qty"]
                    + (t.get("exec_price") or 0) * (t.get("quantity") or 0)
                ) / total_qty
            prev["buy_qty"] = total_qty

    for t in sell_trades:
        tk = t["ticker"]
        if tk in ticker_summary:
            ticker_summary[tk]["sell_price"] = t.get("exec_price") or 0
            ticker_summary[tk]["sell_qty"] = t.get("quantity") or 0
            ticker_summary[tk]["pnl_pct"] = t.get("pnl_pct") or 0
            ticker_summary[tk]["pnl_amt"] = t.get("pnl") or 0.0
            ticker_summary[tk]["action"] = t["action"]
            ticker_summary[tk]["status"] = "closed"
        else:
            # 매수 없이 매도만 있는 경우 (오버나잇 포지션 청산 등)
            ticker_summary[tk] = {
                "ticker": tk,
                "name": t.get("name") or tk,
                "buy_price": None,
                "buy_qty": 0,
                "sell_price": t.get("exec_price") or 0,
                "sell_qty": t.get("quantity") or 0,
                "pnl_pct": t.get("pnl_pct") or 0,
                "pnl_amt": t.get("pnl") or 0.0,
                "action": t["action"],
                "status": "closed",
            }

    return {
        "total":          len(trades),
        "buys":           len(buy_trades),
        "sells":          len(sell_trades),
        "win":            len(win_trades),
        "loss":           len(loss_trades),
        "total_pnl":      total_pnl,
        "win_rate":       len(win_trades) / len(sell_trades) if sell_trades else 0.0,
        "best":           best,
        "worst":          worst,
        "ticker_summary": ticker_summary,
    }


# ──────────────────────────────────────────────
# Claude 분석
# ──────────────────────────────────────────────

def _ask_claude_review(
    today: str,
    trades: list[dict],
    stats: dict,
    snapshots: dict[str, list[dict]],
    market_ctx: dict,
    similar_days: list[dict],
) -> dict:
    """
    Claude에게 오늘 매매 복기 및 개선점 분석 요청.

    Returns:
        {
          "pattern_hits":  [...],   # 잘 작동한 패턴
          "pattern_fails": [...],   # 실패한 패턴
          "improvements":  [...],   # 내일 당장 적용 가능한 개선점
          "market_regime": "...",   # 오늘 시장 성격 태그 (예: 강세_외인주도)
          "strategy_fit":  "...",   # 이 시장 상황에 맞는 전략 평가
          "summary":       "..."    # 한국어 총평
        }
    """
    # 매매 내역 텍스트화
    trade_lines = []
    for t in trades:
        pnl_str = f"손익 {t['pnl_pct']:+.2f}%" if t.get("pnl_pct") is not None else "손익 미확정"
        trade_lines.append(
            f"  - [{t['action']}] {t.get('name', t['ticker'])}({t['ticker']}) "
            f"{t['quantity']}주 @ {t.get('exec_price', 0):,.0f}원 | {pnl_str} | "
            f"사유: {t.get('strategy_id', '') or ''}"
        )

    # 가격 흐름 맥락
    snap_lines = []
    for ticker, snaps in snapshots.items():
        if snaps:
            first_pnl = snaps[0].get("pnl_pct", 0)
            last_pnl  = snaps[-1].get("pnl_pct", 0)
            snap_lines.append(f"  - {ticker}: 장중 손익 {first_pnl:+.1f}% → {last_pnl:+.1f}%")

    # 종목별 페어링 요약
    ticker_summary = stats.get("ticker_summary", {})
    pair_lines = []
    for tk, ts in ticker_summary.items():
        buy_str  = f"매수 {ts['buy_price']:,.0f}원×{ts['buy_qty']}주"  if ts.get("buy_price") else "매수미상"
        sell_str = f"매도 {ts['sell_price']:,.0f}원×{ts['sell_qty']}주" if ts.get("sell_price") else "미청산(보유중)"
        pnl_str  = f"손익 {ts['pnl_pct']:+.2f}%" if ts.get("pnl_pct") is not None else ""
        pair_lines.append(f"  - {ts['name']}({tk}): {buy_str} → {sell_str} {pnl_str}")

    # 유사 시황 과거 복기 요약
    similar_lines = []
    for s in similar_days:
        pnl_sign = "+" if s["total_pnl"] >= 0 else ""
        similar_lines.append(
            f"  - {s['date']}: 승률 {s['win']}/{s['win']+s['loss']}건 | "
            f"손익 {pnl_sign}{s['total_pnl']:,.0f}원 | {s['summary'][:40]}"
        )

    similar_section = ""
    if similar_lines:
        similar_section = f"""
## 유사 시황 과거 성과 (참고용 — {market_ctx['market_direction']} 장, 외인 {market_ctx['foreign_dir']})
{chr(10).join(similar_lines)}
→ 위 과거 데이터 기반으로 오늘 전략의 적합성을 평가하세요."""

    prompt = f"""당신은 국내 주식 단타 퀀트 트레이딩 시스템의 성과 분석 AI입니다.
오늘({today}) 매매 전체를 분석하고, 시장 상황과 연결하여 무엇이 잘 됐는지·무엇을 고쳐야 하는지 판단하세요.
시스템은 당일 매수·매도 단타 전략(스캘핑 포함)입니다. 오버나잇은 예외적입니다.

## 오늘 시장 상황
- KOSPI: {market_ctx['kospi_chg']:+.2f}% | KOSDAQ: {market_ctx['kosdaq_chg']:+.2f}%
- 시장 방향: {market_ctx['market_direction']} (점수 {market_ctx['market_score']:+.2f})
- 외인: {market_ctx['foreign_dir']} | 기관: {market_ctx['institutional_dir']} | 주도세력: {market_ctx['leading_force']}
- 글로벌 리스크: {market_ctx['global_risk']}/10 | 한국 전망: {market_ctx['korea_outlook']}
- 시황 요약: {market_ctx['summary'] or '데이터 없음'}{similar_section}

## 오늘 매매 요약
- 총 거래: {stats['total']}건 (매수 {stats['buys']}, 매도/익절/손절 {stats['sells']})
- 승률: {stats['win_rate']*100:.0f}% (수익 {stats['win']}건 / 손실 {stats['loss']}건)
- 당일 실현 손익: {stats['total_pnl']:+,.0f}원

## 종목별 매수→매도 상세
{chr(10).join(pair_lines) if pair_lines else "  (없음)"}

## 장중 가격 흐름 (최고/최저 손익)
{chr(10).join(snap_lines) if snap_lines else "  (없음)"}

## 전체 거래 내역
{chr(10).join(trade_lines) if trade_lines else "  (없음)"}

## 시스템 전략 참고
- 매수 조건: Hot List + MACD 강세 + 거래량급증, 분할 매수 (60/25/15%)
- 손절: 트레일링 스톱(-3%), MACD 역행 시 조기 손절
- 스캘핑: MACD 피크 + 수익권 → 부분 익절 후 재진입
- 장마감: 14:50 수익 청산, 15:20 전량 강제 청산

## 분석 요청 (반드시 구체적으로, 종목명 포함)
1. **pattern_hits**: 오늘 잘 작동한 진입/청산 패턴 (종목명·수익률 포함, 최대 4개)
2. **pattern_fails**: 손실이 났거나 아쉬웠던 부분 (종목명·이유 포함, 최대 4개)
3. **improvements**: 내일 당장 바꿀 수 있는 개선점 (파라미터 수치 제안 포함, 최대 4개)
4. **market_regime**: 오늘 시장 성격을 태그로 요약 (예: "강세_외인주도", "혼조_개인장", "약세_매도압력" 등 20자 이내)
5. **strategy_fit**: 오늘 시장 상황에 현재 전략이 얼마나 잘 맞았는지 평가 (30자 이내)
6. **summary**: 오늘 총평 (시황 + 승률 + 실현손익 + 특이사항 포함, 2~3문장)

JSON만 응답:
{{
  "pattern_hits":  ["...", ...],
  "pattern_fails": ["...", ...],
  "improvements":  ["...", ...],
  "market_regime": "...",
  "strategy_fit":  "...",
  "summary":       "..."
}}"""

    try:
        response = _client.messages.create(
            model=settings.CLAUDE_MODEL_MAIN,
            max_tokens=1024,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as e:
        logger.error(f"Claude 복기 분석 실패: {e}")
        # Claude 실패 시 — 데이터 기반 자동 생성 (빈 결과 대신)
        return _fallback_review(stats)


def _fallback_review(stats: dict) -> dict:
    """Claude API 실패 시 거래 데이터로 자동 생성하는 복기."""
    hits, fails, improvements = [], [], []
    ticker_summary = stats.get("ticker_summary", {})

    win_rate = stats.get("win_rate", 0)
    total_pnl = stats.get("total_pnl", 0)

    # 잘 된 것: 수익 난 종목
    for tk, ts in ticker_summary.items():
        pnl = ts.get("pnl_pct") or 0
        if pnl > 0:
            hits.append(
                f"{ts['name']}({tk}) +{pnl:.2f}% 익절 성공"
                + (f" (매수 {ts['buy_price']:,.0f}→매도 {ts['sell_price']:,.0f}원)" if ts.get("sell_price") else "")
            )

    # 아쉬운 것: 손실 난 종목
    for tk, ts in ticker_summary.items():
        pnl = ts.get("pnl_pct") or 0
        if pnl < 0:
            fails.append(
                f"{ts['name']}({tk}) {pnl:.2f}% 손절"
                + (f" ({ts.get('action','?')} 사유)" if ts.get("action") else "")
            )

    # 개선 포인트: 승률·손익 기반 자동 생성
    if win_rate < 0.4 and stats.get("sells", 0) >= 3:
        improvements.append(f"승률 {win_rate*100:.0f}% — 진입 기준 강화 필요 (Hot List 신호 복합 조건 추가 검토)")
    if total_pnl < 0:
        improvements.append("당일 실현 손익 마이너스 — 손절선 타이트하게 조정 (trailing_initial_stop 축소 검토)")
    if stats.get("buys", 0) > stats.get("sells", 0) + 2:
        improvements.append("미청산 포지션 다수 — 장마감 청산 로직 확인 필요")
    if not improvements:
        improvements.append("Claude API 오류로 자동 분석 — 로그에서 상세 확인 필요")

    pnl_sign = "+" if total_pnl >= 0 else ""
    summary = (
        f"총 {stats['total']}건 매매 | 승률 {win_rate*100:.0f}% | "
        f"실현손익 {pnl_sign}{total_pnl:,.0f}원. "
        f"(Claude API 미응답 — 데이터 기반 자동 생성)"
    )

    return {
        "pattern_hits":  hits[:4],
        "pattern_fails": fails[:4],
        "improvements":  improvements[:4],
        "market_regime": "",
        "strategy_fit":  "",
        "summary":       summary,
    }


# ──────────────────────────────────────────────
# DB 저장
# ──────────────────────────────────────────────

def _save_review(today: str, stats: dict, review: dict, market_ctx: dict) -> None:
    best_t  = stats.get("best")
    worst_t = stats.get("worst")

    best_json  = json.dumps(
        {"ticker": best_t["ticker"],  "pnl_pct": best_t.get("pnl_pct"),  "reason": best_t.get("strategy_id")},
        ensure_ascii=False,
    ) if best_t else None
    worst_json = json.dumps(
        {"ticker": worst_t["ticker"], "pnl_pct": worst_t.get("pnl_pct"), "reason": worst_t.get("strategy_id")},
        ensure_ascii=False,
    ) if worst_t else None

    market_context_json = json.dumps(
        {
            **market_ctx,
            "market_regime": review.get("market_regime", ""),
            "strategy_fit":  review.get("strategy_fit", ""),
        },
        ensure_ascii=False,
    )

    execute(
        """
        INSERT OR REPLACE INTO trade_review
            (review_date, total_trades, win_trades, loss_trades, total_pnl,
             best_trade, worst_trade, pattern_hits, pattern_fails, improvements,
             summary, market_context)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            today,
            stats["total"],
            stats["win"],
            stats["loss"],
            stats["total_pnl"],
            best_json,
            worst_json,
            json.dumps(review.get("pattern_hits",  []), ensure_ascii=False),
            json.dumps(review.get("pattern_fails", []), ensure_ascii=False),
            json.dumps(review.get("improvements",  []), ensure_ascii=False),
            review.get("summary", ""),
            market_context_json,
        ),
    )
    logger.info(f"trade_review 저장 완료: {today}")


# ──────────────────────────────────────────────
# Telegram 알림
# ──────────────────────────────────────────────

def _notify_review(today: str, stats: dict, review: dict, market_ctx: dict) -> None:
    win_rate_str = f"{stats['win_rate']*100:.0f}%"
    total_pnl    = stats.get("total_pnl") or 0
    pnl_str      = f"{total_pnl:+,.0f}원"
    pnl_emoji    = "🟢" if total_pnl >= 0 else "🔴"

    lines = [
        f"📊 <b>[일일 복기] {today}</b>",
        f"매매 {stats['total']}건 | 승률 {win_rate_str} | {pnl_emoji} {pnl_str}",
        "",
    ]

    # 시장 상황 요약
    regime = review.get("market_regime", "")
    strategy_fit = review.get("strategy_fit", "")
    market_line = (
        f"📈 KOSPI {market_ctx.get('kospi_chg', 0):+.2f}% | KOSDAQ {market_ctx.get('kosdaq_chg', 0):+.2f}%"
        f" | 외인 {market_ctx.get('foreign_dir', '-')} | 글로벌리스크 {market_ctx.get('global_risk', '-')}/10"
    )
    lines.append(market_line)
    if regime:
        lines.append(f"🏷 시장성격: <b>{regime}</b>{f' | {strategy_fit}' if strategy_fit else ''}")
    lines.append("")

    # 종목별 상세 (매수가 → 매도가, 손익, 미청산 여부)
    ticker_summary = stats.get("ticker_summary", {})
    if ticker_summary:
        lines.append("📋 <b>종목별 성과</b>")
        for ts in sorted(ticker_summary.values(), key=lambda x: x.get("pnl_pct") or 0, reverse=True):
            pnl = ts.get("pnl_pct")
            name_str = ts.get("name") or ts["ticker"]
            if ts["status"] == "closed" and pnl is not None:
                p_emoji = "▲" if pnl >= 0 else "▼"
                buy_str  = f"{ts['buy_price']:,.0f}" if ts.get("buy_price") else "?"
                sell_str = f"{ts['sell_price']:,.0f}" if ts.get("sell_price") else "?"
                lines.append(
                    f"  {p_emoji} {name_str}({ts['ticker']}): "
                    f"{buy_str}→{sell_str}원 | <b>{pnl:+.2f}%</b>"
                )
            else:
                # 미청산 포지션
                lines.append(f"  ⏳ {name_str}({ts['ticker']}): 보유중 (미청산)")
        lines.append("")

    # Claude 분석
    if review.get("pattern_hits"):
        lines.append("✅ <b>잘 된 것</b>")
        for h in review["pattern_hits"][:3]:
            lines.append(f"  • {h}")
        lines.append("")

    if review.get("pattern_fails"):
        lines.append("⚠️ <b>아쉬운 것</b>")
        for f_ in review["pattern_fails"][:3]:
            lines.append(f"  • {f_}")
        lines.append("")

    if review.get("improvements"):
        lines.append("🔧 <b>내일 개선 포인트</b>")
        for imp in review["improvements"][:3]:
            lines.append(f"  • {imp}")
        lines.append("")

    if review.get("summary"):
        lines.append(f"💬 {review['summary']}")

    notify("\n".join(lines))
