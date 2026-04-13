# Changelog

모든 변경 이력을 날짜와 버전 기준으로 기록합니다.

---

## [v1.0.3] - 2026-04-13

### Added (WebSocket 실시간 손절)
- `src/infra/kis_websocket.py` (신규) — KIS H0STCNT0 실시간 체결가 구독
  - 싱글턴, 자동 재연결, PINGPONG 처리
  - `subscribe()` / `unsubscribe()` / `mark_selling()` / `clear_selling()`
- `requirements.txt` — `websocket-client>=1.6` 추가

### Changed
- `src/teams/position_monitor/engine.py`
  - WebSocket 실시간 구독으로 폴링 갭(90초) 없이 즉시 손절 대응
  - `_on_ws_price_tick()` — tick마다 트레일링 스톱 비교, 돌파 즉시 시장가 매도
  - `_sync_ws_subscriptions()` — 포지션 변화에 따라 구독 자동 동기화
  - 3단계 안전망: WS 즉시 반응 → 거래소 지정가 주문 → 90초 폴링

---

## [v1.0.2] - 2026-04-13

### Added (거래소 사전 손절 주문 안전망)
- `src/infra/stop_order_manager.py` (신규)
  - KIS 지정가 매도 주문 제출·취소·갱신 단일 모듈
  - `place_stop_order()` / `cancel_stop_order()` / `update_stop_order()`
  - 취소 API: `/uapi/domestic-stock/v1/trading/order-rvsecncl`
- `db/schema.sql` — `stop_orders` 테이블 추가

### Changed
- `src/teams/trading/engine.py`
  - 매수 1차 체결 직후 `place_stop_order()` 호출 → 초기 손절가 즉시 거래소 제출
- `src/teams/position_monitor/engine.py`
  - `_place_sell()` 진입 즉시 `cancel_stop_order()` 호출 (이중 매도 방지)
  - `_update_trailing_floor()` — `quantity` 파라미터 추가, 손절선 상향 시 `update_stop_order()` 자동 호출

---

## [v1.0.1] - 2026-04-13

### Changed (API 비용 최적화)
- `src/teams/domestic_stock/engine.py`
  - `_is_trading_blocked()` 추가 — 매매팀 Gate 1~3 차단 조건을 Hot List 분석 전에 사전 체크
  - 리스크 레벨 ≥ 4, 글로벌 시황 negative, 국내 시황 점수 < -0.3 중 하나라도 해당하면
    Claude `analyze()` 호출 없이 즉시 반환 (Sonnet API 비용 절감)
  - 모듈 상수 `_GATE_RISK_LEVEL_MAX`, `_GATE_MARKET_SCORE_MIN` 추가 (매매팀과 동일 임계값)

---

## [v1.0.0] - 2026-04-13

### 첫 실전 모의투자 가동

- 시스템 첫 거래일(2026-04-13 월요일) 정상 가동 확인
  - 스케줄러 기동 → "🚀 DQT 시스템 시작" 텔레그램 수신 확인
  - 유니버스 450종목 스캔 → 후보 68종목 → Hot List 8종목 확정 (10:57 KST)
  - Hot List 텔레그램 자동 발송 확인
- `simulate_intraday.py` 추가
  - 오늘 hot list 기반 장중 시뮬레이션
  - 실제 OHLCV 사용 + Brownian Bridge 가격 경로 생성
  - 78개 5분봉 압축 재생 (봉당 1.5초 = 약 2분)
  - 손절·1차·2차 익절·트레일링 스톱 이벤트 자동 발동
  - 각 이벤트·30분 현황·최종 결과 텔레그램 발송
- `README.md` 최초 작성 — 시스템 개요·전략·스케줄·API 파라미터 전체 정리

### 오늘 Hot List (2026-04-13)
- 티엠씨(217590): +30.0% 모멘텀 / 조일알미늄(018470): +22.2% 거래량급등
- 실리콘투(257720): +9.3% / KT&G(033780): BB돌파 / 삼성전기(009150): MACD 최대
- 퍼스텍(010820): MACD강세 / 동방(004140): BB돌파 / 코람코더원리츠(417310): BB돌파

---

## [v0.2.5] - 2026-04-12

### Added (시뮬레이션 v2)
- `simulate_friday.py` 업그레이드
  - 일봉 MACD 필터 적용 (비강세 종목 원천 제외)
  - 오프닝 게이트 판단 추가 (Claude: 즉시 매수 vs 9:10 대기)
  - 글로벌 시황(yfinance) + 국내 시황(FDR) 연동
  - 결과 텔레그램 자동 발송

---

## [v0.2.4] - 2026-04-12

### Added (네트워크 복원력)
- `src/utils/retry.py` — 지수 백오프 재시도 유틸 (`retry_call`)
- `src/teams/domestic_stock/collector.py`
  - FDR 조회 최대 3회 재시도 적용
  - 체크포인트 기반 중단 재개 (5분 단위 cycle_id)
  - 완료 종목 건너뛰기 → 재시작 시 이어받기
  - 오래된 체크포인트 자동 정리
- `db/schema.sql` — `fetch_checkpoint` 테이블 추가

---

## [v0.2.1] - 2026-04-11

### Added
- `공통 인프라 0-1. KIS 게이트웨이` — 싱글턴 토큰 관리, Rate Limit 큐, 우선순위 라우팅 (포지션 감시 > 매매팀 > 수집팀)
- `공통 인프라 0-2. 감성 분석 캐시` — URL SHA-256 해시 중복 제거, claude-haiku-4-5, sentiment_cache 테이블
- `공통 인프라 0-3. 종목 유니버스 관리` — KOSPI 200 + KOSDAQ 150 + 거래량 Top 100 ≈ 450종목 스캔 대상 확정
- `팀 4-1. 포지션 감시 서브엔진` — 1~2분 주기, 리스크 레벨 연동 손절, 분할 익절, 5일 타임컷
- `Section 8-1` DB 스키마 — `sentiment_cache`, `universe`, `position_snapshot` 테이블 추가
- `Section 8-2` 스케줄 표 — 4개 신규 컴포넌트 실행 주기 명세 추가
- `docs/architecture/daily_quant_trading_architecture.svg` — 10개 엔진 반영 아키텍처 다이어그램 전면 개정

---

## [v0.1.1] - 2026-04-11

### Added
- `docs/architecture/daily_quant_trading_architecture.svg` — 시스템 아키텍처 시각화 다이어그램
- `concept.md` 섹션 8 추가: 레이어별 상세 설계 정책
  - Layer 1: 데이터 수집 정책 (수집 주기, 중복 제거, 기술적 지표 명세, 이상치 기준)
  - Layer 2: Claude API 정책 (호출 설정, 감성 분석 스키마, 매매 판단 스키마, 리스크 체크 스키마)
  - Layer 3: 전략 판단 정책 (매수 AND 조건 5가지, 매도 OR 조건 5가지, 수량 결정 공식)
  - Layer 4: 주문 실행 정책 (주문 방식, KIS API 인증, Circuit Breaker 조건)
  - Layer 5: 피드백 정책 (DB 스키마, 일일 리포트 항목, 알림 정책)
  - Layer 6: 스케줄러 정책 (시간대별 실행 스케줄, 휴장일 처리)
  - Layer 7: 에러 핸들링 정책 (에러 유형별 처리 방식)

---

## [v0.1.0] - 2026-04-11

### Added
- 프로젝트 초기 구조 생성 (docs, src, tests 폴더)
- `.gitignore` 추가
- `docs/planning/concept.md` 최초 작성
  - 시스템 아키텍처 정의
  - Claude API 활용 방식 3가지 정의
  - 초기 전략 로직 3가지 정의
  - 기술 스택 확정
  - 리스크 정책 초안
  - 개발 로드맵 Phase 0~7 정의
- `docs/planning/CHANGELOG.md` 추가

---

> 버전 규칙
> - `0.x.x` — 기획 및 설계 단계
> - `1.x.x` — 개발 및 테스트 단계
> - `2.x.x` — 실전 운용 단계
