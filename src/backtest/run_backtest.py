"""
run_backtest.py — 백테스트 실행 스크립트

사용법:
    python -m src.backtest.run_backtest
    python -m src.backtest.run_backtest --start 2023-01-01 --end 2023-12-31
    python -m src.backtest.run_backtest --start 2024-01-01 --end 2024-12-31 --capital 10000000
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.backtest.engine import Backtester


def main() -> None:
    parser = argparse.ArgumentParser(description="DQT 전략 백테스트")
    parser.add_argument("--start",   default="2024-01-01", help="시작일 YYYY-MM-DD")
    parser.add_argument("--end",     default="2024-12-31", help="종료일 YYYY-MM-DD")
    parser.add_argument("--capital", default=1_000_000, type=float, help="초기 자본 (원)")
    parser.add_argument("--stop",    default=1.75, type=float, help="손절선 (%)")
    parser.add_argument("--target",  default=5.5,  type=float, help="목표 수익률 (%)")
    parser.add_argument("--year",     default=None, help="단축: 연도만 입력 (예: 2024)")
    parser.add_argument("--no-gates", action="store_true", help="시황 Gate 비활성화 (순수 기술지표만)")
    args = parser.parse_args()

    if args.year:
        args.start = f"{args.year}-01-01"
        args.end   = f"{args.year}-12-31"

    print(f"\nDQT 백테스트 시작")
    print(f"  기간: {args.start} ~ {args.end}")
    print(f"  자본: {args.capital:,.0f}원")
    print(f"  손절: {args.stop}% / 목표: {args.target}%\n")

    use_gates = not args.no_gates
    print(f"  시황 Gate: {'ON (Gate 1~3.5)' if use_gates else 'OFF (기술지표만)'}\n")
    bt = Backtester(
        start=args.start,
        end=args.end,
        capital=args.capital,
        stop_pct=args.stop / 100,
        target_pct=args.target / 100,
        use_market_gates=use_gates,
        verbose=True,
    )
    result = bt.run()
    result.print_summary()

    # 월별 수익률 출력
    if result.equity_curve and len(result.equity_curve) > 20:
        print("월별 수익 분포 (거래 기준):")
        from collections import defaultdict
        monthly: dict[str, list[float]] = defaultdict(list)
        for trade in result.trades:
            key = trade.entry_date.strftime("%Y-%m")
            monthly[key].append(trade.pnl_pct)
        for month in sorted(monthly.keys()):
            pnls = monthly[month]
            avg  = sum(pnls) / len(pnls)
            total = sum(pnls)
            bar  = "█" * max(0, int(total)) + ("▒" * max(0, int(-total)) if total < 0 else "")
            print(f"  {month}: {total:+6.2f}% ({len(pnls)}건, 평균 {avg:+.2f}%) {bar}")


if __name__ == "__main__":
    main()
