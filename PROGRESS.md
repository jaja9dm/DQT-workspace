# DQT-workspace 구현 진행 현황

> 이 파일은 Claude와의 작업 연속성을 위한 진행 추적 문서입니다.
> 새 대화를 시작하면 Claude가 이 파일을 먼저 읽고 작업을 이어갑니다.

---

## 최종 업데이트: 2026-04-29 (집 노트북 환경 세팅 + 실전 피드백 반영)

**상태: 완전체 구현 완료 — 실거래 피드백 기반 로직 개선 중**

GitHub: https://github.com/jaja9dm/DQT-workspace (최신 커밋 반영)

---

## 구현 완료 — 전체 팀

### 기반 인프라
- `db/schema.sql` — 9개 테이블 + 인덱스 (WAL 모드)
- `src/infra/database.py` — SQLite 연결 컨텍스트 매니저, 마이그레이션 자동 적용
- `src/infra/kis_gateway.py` — KIS API 싱글턴, 토큰 자동 갱신, 우선순위 큐, Rate Limit, paper/live 전환
- `src/config/settings.py` — 환경 변수 기반 전역 설정
- `src/utils/logger.py` — 콘솔 + 파일 로거
- `src/infra/universe.py` — KOSPI200 + KOSDAQ150 + 거래량Top100 = ~450종목
- `src/infra/sentiment_cache.py` — 뉴스 감성 분석 캐시 (claude-haiku-4-5)

### 글로벌 시황팀 (`src/teams/global_market/`)
- yfinance: 미국 3대 지수·VIX·WTI·금·환율·미국 10년물·기술주
- Claude sonnet 리스크 분석, VIX 기반 폴백
- 1시간 주기, 즉시 트리거 (VIX≥25, 지수±2%, FX±1%)

### 국내 시황팀 (`src/teams/domestic_market/`)
- KOSPI/KOSDAQ 시황 수집 + Claude 분석
- market_condition 테이블 저장

### 국내 주식팀 (`src/teams/domestic_stock/`) — 핵심 팀
- **모멘텀 점수** 130점 기준 정규화 (거래량비·거래대금·OBV·BB·StochRSI·ATR·RS·수급)
- **Hot List** 3~5종목 (시황 점수 기반 동적 조정)
- **3슬롯 시스템** — LEADER(모멘텀), BREAKOUT(갭업돌파), PULLBACK(눌림목)
- **슬롯 건강 평가** — 10분 주기, 건강점수 < 40 → 교체 요청
- **전략 A/B/C/D** — 갭업돌파·눌림반등·강세편승·오프닝급락반등
- **시간대별 적응형 매매** — 오전 공격적(9:30~11:30) / 점심·마감 보수적
- **자기학습** — ticker_stats (종목별 승률·평균손익·보유시간·최빈시간대)
- **Kelly Criterion 포지션 사이징** — 표본 5회 이상 시 자동 적용
- **ATR 기반 동적 손절** — ATR×1.2/1.5 트레일 파라미터
- **섹터 로테이션** — 강세섹터 진입보너스, 약세섹터 패널티
- **상대강도(RS)** — 1일/5일 RS 진입점수 반영
- **수급** — 외인·기관 순매수 진입점수 (+8/+4/-5pt)
- **관심종목 급등 감시** — 45초 폴링, 체결강도≥145 → 즉시 재스캔
- **Claude 배치 판단** — fingerprint 캐시(10분 TTL), 후보 요약 제공

### 장중 MACD팀 (`src/teams/intraday_macd/`)
- 3분봉+5분봉 듀얼 MACD Pre-Cross 감지
- 진입 중인 종목 MACD 확인 후 매수 허용
- signal_strength 정량화

### 위기 관리팀 (`src/teams/risk/`)
- 계좌 리스크 레벨 산출 (risk_status 테이블)

### 포지션 감시팀 (`src/teams/position_monitor/`)
- 트레일링 스톱 (trigger_pct / floor_pct 동적 파라미터)
- 12:00 이후 60일선 이탈 시 청산
- 연속 손절 감지 → 전략 파라미터 튜닝

### 매매팀 (`src/teams/trading/`)
- **Gate 1~5** — 시황·리스크·포지션·진입점수·진입품질
- **Gate 4.2** — RSI·StochRSI·BB·OBV 복합 조건
- **Gate 4.5** — VWAP·체결강도·호가불균형 진입품질
- **오버나잇 제거** — overnight.py 비활성화 (슬롯 시스템으로 대체)
- **전일 손절 종목 1일 쿨다운**
- **60일선 이탈 자동 청산**

### 복기 리포트팀 (`src/teams/review/`)
- 장 마감 후 일일 복기 리포트
- 잔고·PF·보유시간·연속손절·드로우다운·전환율·놓친기회·내일전략
- 텔레그램 전송

### 연구소 (`src/teams/research/`)
- 백테스트 기능

---

## 데이터베이스 테이블 (총 15개)

| 테이블 | 용도 |
|--------|------|
| `universe` | 스캔 대상 종목 |
| `hot_list` | 당일 관심종목 (모멘텀 점수 포함) |
| `slot_assignments` | 3슬롯 현황 (LEADER/BREAKOUT/PULLBACK) |
| `market_condition` | 국내 시황 |
| `global_condition` | 글로벌 시황 |
| `risk_status` | 계좌 리스크 레벨 |
| `trades` | 매수/매도 이력 |
| `position_snapshot` | 실시간 포지션 |
| `trailing_stop` | 트레일링 스톱 설정 |
| `intraday_macd_signal` | MACD 신호 |
| `intraday_candles` | 장중 캔들 |
| `sector_strength` | 섹터별 강도 |
| `trade_context` | 진입 컨텍스트 (자기학습) |
| `trade_review` | 일일 복기 |
| `ticker_stats` | 종목별 누적 통계 |
| `strategy_params` | 자동 튜닝 파라미터 |

---

## 전략 파라미터 (`strategy_params`)

| 파라미터 | 기본값 | 설명 |
|---------|-------|------|
| initial_stop_pct | 2.0 | 초기 손절선 (%) |
| hot_list_min_vol_ratio | 2.0 | Hot List 최소 거래량비 |
| hot_list_max_rsi | 82.0 | RSI 극과열 차단 상한 |
| max_positions | 3.0 | 최대 동시 보유 종목 수 |
| gate_entry_score_min | 50.0 | Gate 4.2 최소 진입 점수 |
| gate_market_down_pct | -1.5 | 하락장 판단 기준 (%) |
| sector_hot_bonus | 5.0 | 강세 섹터 보너스 |
| sector_cold_penalty | 3.0 | 약세 섹터 패널티 |
| review_win_rate_target | 55.0 | 목표 승률 (%) |

---

## 운용 현황

- **모드**: `KIS_MODE=paper` (모의투자)
- **실전 전환 방법**: `.env`에서 `KIS_MODE=live` + 실계좌 앱키로 변경
- **API 오류 이력**: KIS 토큰 만료 시 403 → 자동 재발급 로직 구현됨
- **websocket-client**: `pip install websocket-client` 필요 (체결강도 실시간)

## 2026-04-28 버그 수정 이력

- **장중 재시작 시 유니버스 자동 재구성** — 08:50 스케줄 놓쳐도 즉시 재구성 (스캔 정상화)
- **섹터 매핑 컬럼 오류** — FDR `Dept` 컬럼 인식 추가 → 2880종목 로드 정상
- **텔레그램 알림 타임아웃** — notifier.py urllib → requests.Session 교체
- **trailing_stop INSERT 컬럼 오류** — `current_floor` → `trailing_floor` 수정
- **trailing_stop 컬럼 4개 누락** — `scale_in_count`, `dip_buy_count`, `scalp_exit_price`, `scalp_exit_qty` DB/스키마/마이그레이션 동기화 (불타기·물타기 횟수 제한 정상화)
- **장 마감 후 스캐너 계속 실행** — `collect()`에 stop_event 전달, 정지 신호 수신 시 즉시 중단
- **WebSocket 장 마감 후 무한 재연결** — `position_monitor.stop()`에서 `ws.stop()` 호출 추가
- **WebSocket 싱글톤 재기동 불가** — `stop()` 시 `_instance` 초기화, 다음 날 새 인스턴스 생성
- **위기 관리팀 즉시 트리거 미연결** — 글로벌·국내 경보 발생 시 `trigger_emergency()` 호출 연결
- **trades INSERT 에러 로깅** — silent pass → logger.error로 변경 (중복 매수 방지 진단 가능)
- **KIS 500 오류 메시지** — "장외시간 가능성" 오해 표현 제거

## 2026-04-29 작업 이력

- **집 노트북 환경 세팅 완료** — venv 생성, requirements.txt 설치, crontab @reboot 등록
- **run.sh 경로 하드코딩 제거** — 스크립트 위치 기준 자동 감지 (어느 머신에서도 동작)
- **breakout 신호 RSI<55 hard_fail 추가** — BB 상단 돌파인데 모멘텀 없는 가짜 신호 차단 (포스코스틸리온 유형)
- **volume_price_surge RSI 예외 95까지 완화** — 섹터 테마 급등 시 OBV 양수 조건 유지하며 진입 기회 확보 (이수화학 유형)
- **복기 Claude API timeout 60→90s + haiku 폴백** — 3회차 실패 시 haiku 모델로 재시도

## 알려진 이슈

- KIS 모의 계좌 403 오류: OAuth 토큰 만료 시 발생, 재시작으로 해결
- hot_list에 volume_ratio < 1인 종목 저장됨 — 필터는 매매팀에서 적용 (정상 동작)

---

## 파일 구조 (현재)

```
DQT-workspace/
├── main.py
├── requirements.txt
├── .env.example
├── db/
│   ├── schema.sql
│   └── dqt.db
├── logs/
│   └── dqt.log
├── src/
│   ├── config/settings.py
│   ├── infra/
│   │   ├── database.py
│   │   ├── kis_gateway.py
│   │   ├── universe.py
│   │   └── sentiment_cache.py
│   ├── scheduler/scheduler.py
│   └── teams/
│       ├── global_market/      ✅
│       ├── domestic_market/    ✅
│       ├── domestic_stock/     ✅ (핵심)
│       ├── intraday_macd/      ✅
│       ├── risk/               ✅
│       ├── position_monitor/   ✅
│       ├── trading/            ✅
│       ├── review/             ✅
│       └── research/           ✅
├── docs/
│   └── planning/
│       ├── concept.md          (v2.0 슬롯 시스템)
│       └── concept.html
└── scripts/
```

---

## 다음 할일

없음 — 전체 구현 완료. 실전 전환 시 `.env` 수정 후 `python main.py` 실행.
