# Changelog

모든 변경 이력을 날짜와 버전 기준으로 기록합니다.

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
