# DQT-workspace 구현 진행 현황

> 이 파일은 Claude와의 작업 연속성을 위한 진행 추적 문서입니다.
> 새 대화를 시작하면 Claude가 이 파일을 먼저 읽고 작업을 이어갑니다.

---

## 구현 완료

### 1단계 — 기반 인프라 (커밋 `df4a7af`)
- `db/schema.sql` — 9개 테이블 + 인덱스 (WAL 모드)
  - universe, sentiment_cache, hot_list, market_condition, global_condition
  - risk_status, active_strategies, trades, position_snapshot
- `src/infra/database.py` — SQLite 연결 컨텍스트 매니저 (fetch_one, fetch_all, execute)
- `src/infra/kis_gateway.py` — KIS API 싱글턴 게이트웨이
  - 토큰 자동 갱신 (만료 30분 전), 우선순위 큐, Rate Limit, 3회 재시도
  - paper/live 모드 전환 (`KIS_MODE` 환경변수)
- `src/config/settings.py` — 환경 변수 기반 전역 설정
- `src/utils/logger.py` — 콘솔 + 파일 로거 (`logs/dqt.log`)
- `src/teams/*/` — 8개 팀 패키지 폴더 + `__init__.py`
- `main.py` — 시스템 진입점 뼈대
- `requirements.txt`, `.env.example`

### 2단계 — 글로벌 시황팀 (커밋 `50d5c09`)
- `src/teams/global_market/collector.py`
  - yfinance: 미국 3대 지수·VIX·WTI·금·환율·미국 10년물·기술주
  - FRED API: 향후 2일 이내 경제지표 발표 일정
- `src/teams/global_market/analyzer.py`
  - Claude `claude-sonnet-4-6` (temperature=0) 리스크 분석
  - 글로벌 리스크 점수 (0~10), 한국 시장 전망, 주요 리스크 요인
  - Claude 실패 시 VIX 기반 폴백 자동 적용
- `src/teams/global_market/engine.py`
  - 1시간 주기 루프, 즉시 트리거 (VIX≥25, 지수±2%, FX±1%)
  - `global_condition` 테이블 저장, 경보 로깅

### 3단계 — 종목 유니버스 (커밋 `ca4a9c4`)
- `src/infra/universe.py` — 공통 인프라 0-3
  - KOSPI 200 + KOSDAQ 150 + 거래량 Top 100 = ~450종목
  - FinanceDataReader로 장 전 1회 전체 재구성
  - KIND RSS 공시 감시 (2분 주기) → 공시 종목 즉시 편입
  - 싱글턴, `is_in_universe()`, `add_disclosure_ticker()` 제공

### 4단계 — 감성 분석 캐시 (커밋 `24cfeee`)
- `src/infra/sentiment_cache.py`
  - URL SHA-256 해시 중복 제거
  - Claude `claude-haiku-4-5` 1회 분석 → DB 저장 → 전 팀 공유
  - 24시간 만료, `get_by_ticker()`, `avg_score_by_ticker()`, `purge_expired()` 제공

### 5단계 — 국내 시황팀 (커밋 다음)
- `src/infra/universe.py` 버그 수정
  - `fdr.StockListing('KOSPI200')` → `fdr.StockListing('KOSPI')` Marcap 상위 200
  - KIND RSS 404 → KIND 공시 페이지 HTML 파싱 (브라우저 헤더)
- `src/teams/domestic_market/collector.py` ✅
  - KIS API: KOSPI/KOSDAQ 지수 현재가·등락률 (`_KIS_INDEX_PATH`)
  - KIS API: 투자자별 매매동향 외국인·기관·개인 (`_KIS_INVESTOR_PATH`)
  - KIS 실패 시 FinanceDataReader(KS11/KQ11) 폴백
  - FinanceDataReader: 60일 OHLCV → 5/20/60일 이동평균·추세
  - 네이버금융: 국내 증시 뉴스 최대 10건 (regex 파싱, euc-kr)
- `src/teams/domestic_market/analyzer.py` ✅
  - Claude `claude-sonnet-4-6` (temperature=0) 시황 분석
  - 시장 점수(-1~1), 방향(bullish/neutral/bearish), 주도 주체, 요약
  - Claude 실패 시 지수 등락률 기반 폴백
- `src/teams/domestic_market/engine.py` ✅
  - 30분 주기 루프, 즉시 트리거 (KOSPI±1.5%, 외국인±2000억)
  - `market_condition` 테이블 저장
  - 수집 뉴스 → SentimentCache 비동기 제출

### 6단계 — 국내 주식팀 (커밋 다음)
- `src/teams/domestic_stock/collector.py` ✅
  - KIS API: 유니버스 ~450종목 실시간 현재가·등락률·거래량
  - KIS 실패 시 FDR 폴백 없음 (현재가는 실시간만 의미있음)
  - FinanceDataReader: 120일 OHLCV → RSI(14), MACD(12/26/9), 볼린저밴드(20/2), MA5/20/60
  - pandas-ta 우선, 미설치 시 수동 계산 폴백
  - 신호 플래그: is_volume_surge(3배↑), is_price_surge(3%↑), is_breakout(BB상단돌파)
- `src/teams/domestic_stock/analyzer.py` ✅
  - 후보 종목 최대 20개 배치 → Claude sonnet-4-6 Hot List 판단
  - 과열 종목(RSI>70 + BB>0.9) 자동 제외
  - Claude 실패 시 복합신호 종목 자동 선정
- `src/teams/domestic_stock/engine.py` ✅
  - 5분 주기, 즉시 트리거(거래량5배↑, 가격5%↑)
  - market_condition·global_condition DB 참조
  - `hot_list` 테이블 저장, `get_latest_hot_list()` 공개 API

---

## 구현 예정 (순서대로)

### 7단계 — 위기 관리팀 (커밋 다음)
- `src/teams/risk/engine.py` ✅
  - DB에서 global_condition, market_condition, hot_list 읽어 리스크 점수(0~100) 산출
    - 글로벌 리스크 0~40pt + 국내시황 0~20pt + VIX 0~15pt + 포트폴리오 0~15pt + 과열 0~10pt
  - KIS API: 보유 잔고·평가손익 조회 (모의 VTTC8434R / 실거래 TTTC8434R)
  - 리스크 레벨 1~5 결정 → `risk_status` 저장
  - 긴급 강제 상향: 글로벌≥8 또는 KOSPI-2% → Level 4, 포트폴리오-5% → Level 5
  - 15분 주기
  - `get_current_risk()`, `get_stop_loss_pct()` 공개 API (매매·포지션 감시팀 사용)

### 8단계 — 포지션 감시 서브엔진 (커밋 다음)
- `src/teams/position_monitor/engine.py` ✅
  - KIS API: 보유 잔고·현재가 90초 주기 조회 (POSITION_MONITOR 최우선 큐)
  - 손절: 리스크 레벨 연동 (L1~3=-5%, L2=-3%, L4~5=-1%)
  - 분할 익절: +5% → 1/3 매도(1차), +10% → 1/3 추가 매도(2차)
  - 타임컷: 5 영업일 초과 전량 청산
  - Level 5 긴급 전량 청산
  - `position_snapshot` 저장, `trades` 이력 기록
  - `_calc_held_days()`: trades 최초 매수일 기준 영업일 계산
  - `_count_partial_sells()`: 오늘 익절 횟수 추적 (2차 중복 방지)

### 9단계 — 매매팀 (커밋 다음)
- `src/teams/trading/engine.py` ✅
  - 게이트 구조 (순서대로, 하나라도 실패 시 전체 차단):
    - Gate 1: 리스크 레벨 ≥ 4 → 신규 진입 금지
    - Gate 2: 글로벌 outlook == 'negative' → 진입 보류
    - Gate 3: 국내 market_score < -0.3 → 진입 보류
    - Gate 4: Hot List (최근 10분) 비어있으면 대기
    - Gate 5: Claude sonnet-4-6 최종 매수 판단 (종목별)
  - 분할 매수: 1차 40% 즉시 → 2·3차(35%/25%) 5분 후 -1% 추가 하락 시 진입
  - KIS 예수금 × position_limit_pct × max_single_trade_pct로 투자 한도 계산
  - 당일 중복 매수 방지 (today_tickers 세트)
  - `trades` 테이블 저장, 종목별 감성 점수 참조

### 10단계 — 리포트팀 (커밋 다음)
- `src/utils/notifier.py` ✅ (12단계 선행 구현)
  - 텔레그램 Bot API 발송 모듈 (카카오톡/Slack 대체)
  - `notify()`, `notify_trade()`, `notify_risk()`, `notify_daily_report()`, `notify_error()`
  - HTML 포맷, 재시도 1회, 동시 발송 직렬화
- `src/teams/report/engine.py` ✅
  - 장 마감 후 배치 전용 엔진 (스케줄러 호출)
  - trades + position_snapshot + risk_status DB 집계
  - 당일 손익%·거래건수·승률·손익비·종목별 성과·Hot List 적중률
  - 텔레그램 일일 리포트 발송
  - `ReportEngine().run()` 단일 진입점

### 11단계 — 연구소 (커밋 다음)
- `src/teams/research/engine.py` ✅
  - Claude opus-4-6 전략별 성과 분석 (30일 trades 기반 승률·손익비)
  - `active_strategies` 파라미터 자동 조정 (keep|adjust|deprecate)
  - 기본 전략 3종 자동 초기화: 거래량급등모멘텀·BB돌파·MACD모멘텀
  - 6개월 FDR 백테스트 (deep=True, 주 1회)
  - `ResearchEngine().run(deep=False|True)` 단일 진입점

### 12단계 — 알림 유틸리티
- `src/utils/notifier.py` ✅ 10단계에서 선행 구현 완료 (텔레그램)

### 13단계 — 스케줄러 (커밋 다음)
- `src/scheduler/scheduler.py` ✅
  - APScheduler(BackgroundScheduler) 기반
  - 장 전(08:50): 유니버스 재구성
  - 장 시작(09:00): 전체 실시간 엔진 기동
  - 장 마감(15:35): 실시간 엔진 역순 정지
  - 배치(15:40~): 리포트팀 → 연구소 일일 → 연구소 심층(일요일)
  - `trigger_now(job_id)` 수동 실행 지원
- `main.py` 스케줄러 중심으로 리팩토링
  - `python main.py --now`: 즉시 엔진 기동 (개발·테스트용)

### 14단계 — 트레일링 스톱 + 사다리 매수 (2026-04-12)
- `db/schema.sql` ✅ — `trailing_stop` 테이블 추가
  - ticker, entry_price, trailing_floor, highest_price, ladder_bought
- `src/config/settings.py` ✅ — 트레일링 스톱 파라미터 5종 추가
  - TRAILING_INITIAL_STOP_PCT(10%), TRAILING_TRIGGER_PCT(10%), TRAILING_FLOOR_PCT(5%)
  - LADDER_TRIGGER_PCT(20%), LADDER_QTY_RATIO(1.0)
- `src/teams/trading/engine.py` ✅ — 매수 시 `_init_trailing_stop()` 자동 호출
- `src/teams/position_monitor/engine.py` ✅ — 트레일링 스톱 로직 전면 교체
  - 90초 주기로 손절선 업데이트 (단방향 상승)
  - 수익 +10% 이상 시 손절선 = max(현재, 현재가×0.95)
  - 현재가 ≤ 손절선 → 전량 매도 + 텔레그램 알림
  - 하락 -20% 시 사다리 매수 (보유량 ×1배 추가 매수)
  - 기존 고정 손절은 트레일링 미등록 포지션에만 적용
- `src/utils/notifier.py` ✅ — 트레일링 스톱 발동 즉시 알림 (inline)
- `.env.example` ✅ — 트레일링 스톱 파라미터 주석 추가
- `docs/planning/concept.md` ✅ — v0.2.3 포지션 감시 섹션 업데이트

### 15단계 — MACD Pre-Cross 전략 + 장중 다회 매매 (2026-04-12)
- **전략 변경 핵심**
  - 일봉 MACD 필터: MACD 비강세 종목 Hot List 원천 제외
  - 분봉 Pre-Cross 진입: 완전 크로스 전 히스토그램 수렴 시 선제 진입
  - 손절 유동화: `TRAILING_INITIAL_STOP_PCT` 기본값 10% → 5% (`.env`에서 자유 조정)
  - MACD 조기손절: 진입 후 분봉 MACD 역행 시 손절선 무관 즉시 청산
  - 장중 재매수: MACD 조기손절 후 신호 복귀 시 동일 종목 재진입 허용
- `src/utils/macd.py` ✅ (신규) — MACD 계산 + Pre-Cross 감지 유틸
  - `calc_macd()`, `get_signal()`, `is_daily_macd_bullish()`, `aggregate_candles()`
- `db/schema.sql` ✅ — `intraday_macd_signal` 테이블 추가
- `src/infra/kis_gateway.py` ✅ — `get_minute_candles()` 추가 (KIS 분봉 API)
- `src/teams/domestic_stock/collector.py` ✅
  - `StockSnapshot.daily_macd_ok`, `macd_hist_prev` 필드 추가
  - `_calc_macd_manual()` → 직전 히스토그램 함께 반환
- `src/teams/domestic_stock/analyzer.py` ✅ — 일봉 MACD 필터 하드게이트 추가
- `src/teams/intraday_macd/engine.py` ✅ (신규 팀) — 장중 MACD 모니터링
  - 3분 주기, Hot List + 보유 포지션 대상
  - 1분봉 → 3분봉·5분봉 집계 → Pre-Cross 감지 → DB 기록
  - `get_latest_macd_signal(ticker)` — position_monitor·trading 팀 공용
- `src/teams/position_monitor/engine.py` ✅ — MACD 조기손절 로직 추가 (최우선 체크)
- `src/teams/trading/engine.py` ✅ — 재매수 로직 추가
  - `_macd_reentry_ok` set으로 재진입 허용 종목 관리
  - buy_pre 신호 복귀 + 포지션 없음 확인 후 재매수
- `src/config/settings.py` ✅ — MACD 파라미터 4종 추가
  - MACD_DAILY_FILTER, MACD_HIST_CONV_BARS, MACD_EARLY_EXIT_ENABLED, MACD_EARLY_EXIT_MIN_LOSS_PCT
- `src/scheduler/scheduler.py` ✅ — IntradayMACDEngine 등록
- `.env.example` ✅ — MACD 파라미터 주석 추가

### 16단계 — 오프닝 게이트 + 9:10 재점검 + 15:10 오버나잇 판단 (2026-04-12)
- **오프닝 게이트 (Gate 0)**
  - 장 시작 첫 사이클: Claude가 시황 평가 → "진짜 좋으면" 즉시 매수, 아니면 9:10 대기
  - 판단 기준: 리스크 레벨 ≤ 2, 글로벌 리스크 ≤ 3, 국내 시황 ≥ +0.3 (모두 충족 시)
  - 결과 텔레그램 알림 (즉시 매수 or 관망)
- **9:10 재점검** (스케줄러 잡 추가)
  - 오프닝 게이트 해제 (무조건 매수 재개)
  - 국내 주식팀 즉시 재스캔 (Hot List 갱신)
  - 매매팀 즉시 1회 실행
- **15:10 오버나잇 판단** (스케줄러 잡 추가)
  - `src/teams/trading/overnight.py` (신규): 보유 포지션별 Claude 판단
  - 판단 기준: 분봉 MACD 신호, 현재 손익, 보유 일수, 글로벌 리스크
  - 청산 결정 시 즉시 시장가 매도 실행
  - 결과 텔레그램 요약 발송 (유지/청산 종목 목록)
- `src/teams/trading/engine.py` ✅ — Gate 0 오프닝 게이트 로직 추가
- `src/teams/trading/overnight.py` ✅ (신규) — 오버나잇 판단 모듈
- `src/scheduler/scheduler.py` ✅ — 9:10, 15:10 잡 등록 + 콜백 구현

---

## 다음 할일 — 통합 테스트

### Phase 1: 환경 설정 및 첫 실행
1. `.env` 파일 생성 (`.env.example` 참고)
   - `KIS_MODE=paper` (모의투자)
   - `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_ACCOUNT_NO` 입력
   - `ANTHROPIC_API_KEY` 입력
   - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` 입력 (텔레그램 봇 생성 필요)
2. 패키지 설치: `pip install -r requirements.txt`
3. 즉시 실행 테스트: `python main.py --now`

### Phase 2: 팀별 단위 테스트 (실행 확인)
- [ ] DB 초기화 확인 (`db/dqt.db` 생성, 9개 테이블)
- [ ] KIS 게이트웨이 토큰 발급 확인 (로그 `KIS 게이트웨이 준비 완료`)
- [ ] 유니버스 재구성 확인 (로그 `유니버스 확정: N종목`)
- [ ] 글로벌 시황팀 1회 실행 확인 (`global_condition` 테이블 row 삽입)
- [ ] 국내 시황팀 1회 실행 확인 (`market_condition` 테이블 row 삽입)
- [ ] 국내 주식팀 스캔 확인 (후보 종목 로그)
- [ ] 위기 관리팀 리스크 레벨 산출 확인 (`risk_status` 테이블)
- [ ] 텔레그램 알림 수신 확인 (시스템 시작 메시지)

### Phase 3: 매매 흐름 검증
- [ ] Hot List 생성 확인 (`hot_list` 테이블)
- [ ] 매매팀 게이트 로그 확인 (Gate 1~5 통과 여부)
- [ ] 모의투자 매수 주문 체결 확인 (KIS 모의 계좌)
- [ ] 포지션 감시 손절·익절 동작 확인
- [ ] 장 마감 후 리포트 텔레그램 수신 확인

### Phase 4: 안정화 (최소 1주일 모의 운용)
- [ ] 오류 로그 (`logs/dqt.log`) 모니터링
- [ ] 이상 동작 버그 수정
- [ ] 실전 전환 여부 결정 (`KIS_MODE=live`)

---

## 주요 설계 원칙 (변경 금지)

| 항목 | 내용 |
|------|------|
| KIS API 접근 | 반드시 `KISGateway` 경유. 팀에서 직접 호출 금지 |
| 뉴스 감성 분석 | `sentiment_cache`에서 읽기. 팀에서 Claude 직접 호출 금지 |
| 팀 간 통신 | 공유 DB (SQLite) 비동기. 직접 함수 호출 금지 |
| Claude temperature | 거래 판단 전부 `0.0` |
| 모델 할당 | haiku=감성캐시, sonnet=매매·위기·시황, opus=연구소 |
| 스캔 대상 | 유니버스 ~450종목만. 전 종목 무차별 스캔 금지 |

---

## 파일 구조 (현재)

```
DQT-workspace/
├── main.py                          ← 시스템 진입점
├── requirements.txt
├── .env.example
├── db/
│   └── schema.sql                   ← 9개 테이블
├── src/
│   ├── config/settings.py           ← 환경 변수 설정
│   ├── infra/
│   │   ├── database.py              ← SQLite 연결
│   │   ├── kis_gateway.py           ← KIS API 게이트웨이 ✅
│   │   └── universe.py              ← 종목 유니버스 ✅
│   ├── utils/logger.py
│   └── teams/
│       ├── global_market/           ✅ 완료
│       │   ├── collector.py
│       │   ├── analyzer.py
│       │   └── engine.py
│       ├── domestic_market/         ✅ 완료
│       │   ├── collector.py
│       │   ├── analyzer.py
│       │   └── engine.py
│       ├── domestic_stock/          ✅ 완료
│       │   ├── collector.py
│       │   ├── analyzer.py
│       │   └── engine.py
│       ├── risk/                    ✅ 완료
│       │   └── engine.py
│       ├── position_monitor/        ✅ 완료
│       │   └── engine.py
│       ├── trading/                 ✅ 완료
│       │   └── engine.py
│       ├── intraday_macd/           ✅ 완료 (15단계)
│       │   └── engine.py
│       ├── report/                  ✅ 완료
│       │   └── engine.py
│       └── research/                ✅ 완료
│           └── engine.py
│   └── utils/
│       ├── logger.py
│       ├── notifier.py
│       └── macd.py                  ✅ 완료 (15단계)
└── docs/
    └── planning/
        ├── concept.md               ← 상세 설계 문서 (v0.2.1)
        └── concept.html
```
