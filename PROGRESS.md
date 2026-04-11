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

---

## 구현 예정 (순서대로)

### 4단계 — 감성 분석 캐시
- `src/infra/sentiment_cache.py`
  - URL SHA-256 해시 중복 제거
  - Claude `claude-haiku-4-5` 1회 분석 → DB 저장 → 전 팀 공유
  - 24시간 만료 (`sentiment_cache` 테이블)

### 5단계 — 국내 시황팀
- `src/teams/domestic_market/collector.py`
  - KIS API: KOSPI/KOSDAQ 지수, 외국인·기관 수급, 투자자별 매매동향
  - FinanceDataReader: 과거 지수 데이터 (이동평균·추세)
- `src/teams/domestic_market/analyzer.py` — Claude 시황 분석
- `src/teams/domestic_market/engine.py` — 30분 주기 + 즉시 트리거

### 6단계 — 국내 주식팀
- `src/teams/domestic_stock/collector.py`
  - KIS API: 실시간 현재가·거래량·호가 (유니버스 ~450종목 대상)
  - FinanceDataReader: 60일 OHLCV (RSI, MACD, 볼린저밴드)
  - pandas-ta: 기술적 지표 계산
- `src/teams/domestic_stock/analyzer.py` — Claude Hot List 판단
- `src/teams/domestic_stock/engine.py` — 5분 주기 + 급등 즉시 트리거

### 7단계 — 위기 관리팀
- `src/teams/risk/engine.py`
  - DB에서 global_condition, market_condition, hot_list 읽기
  - KIS API: 현재 포트폴리오 손익 조회
  - 리스크 점수 산출 → 레벨 1~5 결정 → `risk_status` 저장
  - 15분 주기 + 이벤트 즉시 트리거

### 8단계 — 포지션 감시 서브엔진
- `src/teams/position_monitor/engine.py`
  - KIS API: 보유 잔고·현재가 1~2분 주기 조회
  - 손절 (기본 -5%, 레벨2 -3%, 레벨4 -1%), 분할 익절 (+5% 1/3, +10% 1/3)
  - 타임컷 (5 영업일 초과), 레벨5 전량 청산
  - `position_snapshot` 저장, 알림 발송

### 9단계 — 매매팀
- `src/teams/trading/engine.py`
  - 게이트 구조: 리스크 레벨 → 시황 → 글로벌 → Hot List → 진입 판단
  - Claude `claude-sonnet-4-6` 최종 매수·매도 결정
  - 분할 매수 3회 (40% / 35% / 25%), KIS API 주문 실행
  - `trades` 테이블 저장

### 10단계 — 리포트팀
- `src/teams/report/engine.py`
  - 장 마감 후 배치: trades + position_snapshot 기반 일일 성과 집계
  - 팀별 기여도, 전략별 승률·손익비
  - 카카오톡·Slack 알림 발송

### 11단계 — 연구소
- `src/teams/research/engine.py`
  - 장 마감 후 배치: 전략별 성과 분석, 임계값 조정 판단
  - FinanceDataReader 백테스트 (최소 6개월)
  - `active_strategies` 테이블 업데이트

### 12단계 — 알림 유틸리티
- `src/utils/notifier.py` — 카카오톡 / Slack 공통 발송 모듈

### 13단계 — 스케줄러
- `src/scheduler/` — 각 팀 엔진 기동 타이밍 통합 관리 (APScheduler 등)

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
│       ├── domestic_market/         ⏳ 5단계
│       ├── domestic_stock/          ⏳ 6단계
│       ├── risk/                    ⏳ 7단계
│       ├── position_monitor/        ⏳ 8단계
│       ├── trading/                 ⏳ 9단계
│       ├── report/                  ⏳ 10단계
│       └── research/                ⏳ 11단계
└── docs/
    └── planning/
        ├── concept.md               ← 상세 설계 문서 (v0.2.1)
        └── concept.html
```
