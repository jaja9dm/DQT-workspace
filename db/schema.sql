-- DQT-workspace 공유 DB 스키마
-- concept.md Section 8-1 기준
-- 모든 팀이 이 DB를 통해 비동기 통신

PRAGMA journal_mode=WAL;   -- 동시 읽기·쓰기 허용
PRAGMA foreign_keys=ON;

-- ────────────────────────────────────────
-- 공통 인프라 0-3: 종목 유니버스
-- 매일 장 전 재생성 / 당일 스캔 대상 확정
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS universe (
    ticker      TEXT NOT NULL,
    name        TEXT,
    market      TEXT NOT NULL,  -- KOSPI | KOSDAQ
    reason      TEXT NOT NULL,  -- kospi200 | kosdaq150 | volume_top | disclosure
    active_date DATE NOT NULL,
    added_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, active_date)
);

-- ────────────────────────────────────────
-- 공통 인프라 0-2: 감성 분석 캐시
-- URL SHA-256 해시 중복 제거, 24h 만료
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sentiment_cache (
    url_hash    TEXT PRIMARY KEY,
    url         TEXT,
    ticker      TEXT,
    category    TEXT NOT NULL,  -- stock | market | global
    score       REAL NOT NULL,  -- -1.0 ~ 1.0
    direction   TEXT NOT NULL,  -- bullish | bearish | neutral
    confidence  REAL NOT NULL,  -- 0.0 ~ 1.0
    key_factors TEXT,           -- JSON array
    analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at  DATETIME NOT NULL
);

-- ────────────────────────────────────────
-- 국내 주식팀 출력: 종목 Hot List
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hot_list (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker           TEXT NOT NULL,
    name             TEXT,
    signal_type      TEXT NOT NULL,  -- volume_surge | sector_momentum | breakout | ...
    volume_ratio     REAL,           -- 현재 거래량 / 평균 거래량
    price_change_pct REAL,           -- 당일 등락률 (%)
    rsi              REAL,
    sector           TEXT,
    reason           TEXT,           -- Claude 판단 근거 요약
    momentum_score   REAL DEFAULT 0.0,  -- 종합 모멘텀 점수 (0~100)
    obv_slope        REAL DEFAULT 0.0,  -- OBV 5봉 기울기
    day_range_pos    REAL DEFAULT 0.5,  -- 당일 가격 범위 내 위치 (0=저가권, 1=고가권)
    stoch_rsi        REAL DEFAULT 50.0, -- Stochastic RSI
    bb_width_ratio   REAL DEFAULT 1.0,  -- 볼린저밴드 폭 비율
    trading_value    INTEGER DEFAULT 0, -- 당일 누적 거래대금 (원)
    exec_strength    REAL DEFAULT 100.0, -- 체결강도 (100=균형, 130↑=강한매수세, 80↓=매도우위)
    rs_daily         REAL DEFAULT 0.0,  -- 당일 KOSPI 대비 초과수익률 (%)
    rs_5d            REAL DEFAULT 0.0,  -- 5일 KOSPI 대비 누적 초과수익률 (%)
    frgn_net_buy     INTEGER DEFAULT 0, -- 외국인 순매수량 (주, 양수=매수우위)
    inst_net_buy     INTEGER DEFAULT 0, -- 기관 순매수량 (주, 양수=매수우위)
    atr_pct          REAL DEFAULT 0.0,  -- ATR 14봉 / 현재가 × 100 (%) — 손절가 산출 기준
    slot             TEXT DEFAULT NULL, -- 'leader' | 'breakout' | 'pullback'
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 슬롯 배정: 당일 3개 슬롯 상태 관리
-- leader(주도주) / breakout(돌파) / pullback(눌림목)
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS slot_assignments (
    slot         TEXT PRIMARY KEY,  -- 'leader' | 'breakout' | 'pullback'
    ticker       TEXT,              -- 배정된 종목코드 (NULL=비어있음)
    name         TEXT,
    signal_type  TEXT,
    reason       TEXT,
    trade_date   DATE NOT NULL,     -- 당일만 유효 (날짜 바뀌면 초기화)
    status       TEXT DEFAULT 'empty',  -- 'active' | 'empty'
    assigned_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 섹터 로테이션: 업종별 강도 (당일 스캔 기반)
-- inject_scan_results() 호출 시 갱신
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sector_strength (
    sector      TEXT PRIMARY KEY,
    avg_ret_1d  REAL NOT NULL,   -- 섹터 평균 당일 등락률 (%)
    vs_kospi    REAL NOT NULL,   -- KOSPI 대비 초과수익률 (%)
    stock_count INTEGER NOT NULL, -- 집계 종목 수
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 국내 시황팀 출력: 국내 시장 상태
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_condition (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    market_score             REAL NOT NULL,    -- -1.0(약세) ~ 1.0(강세)
    market_direction         TEXT NOT NULL,    -- bullish | bearish | neutral
    foreign_net_buy_bn       REAL,             -- 외국인 순매수 (억원)
    institutional_net_buy_bn REAL,             -- 기관 순매수 (억원)
    advancing_stocks         INTEGER,
    declining_stocks         INTEGER,
    summary                  TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 글로벌 시황팀 출력: 글로벌 시장 상태
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS global_condition (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    global_risk_score    INTEGER NOT NULL,  -- 0(안전) ~ 10(위험)
    vix                  REAL,
    sp500_change         REAL,              -- S&P 500 등락률 (%)
    nasdaq_change        REAL,
    usd_krw              REAL,
    wti_oil              REAL,
    us_10y_yield         REAL,
    korea_market_outlook TEXT NOT NULL,     -- positive | neutral | negative
    key_events           TEXT,              -- JSON array (주요 이벤트)
    created_at           DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 위기 관리팀 출력: 리스크 레벨
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS risk_status (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    risk_level           INTEGER NOT NULL CHECK(risk_level BETWEEN 1 AND 5),
    risk_score           INTEGER NOT NULL,
    position_limit_pct   INTEGER NOT NULL,  -- 0 | 40 | 70 | 100
    max_single_trade_pct REAL NOT NULL,     -- 1회 주문 최대 비중 (%)
    stop_loss_tighten    INTEGER NOT NULL DEFAULT 0,  -- 0 | 1 (BOOLEAN)
    active_alerts        TEXT,              -- JSON array
    recommended_action   TEXT,
    created_at           DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 연구소 출력: 활성 전략 목록
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS active_strategies (
    strategy_id   TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    conditions    TEXT NOT NULL,  -- JSON (매수 조건 정의)
    win_rate      REAL,           -- 0.0 ~ 1.0
    profit_factor REAL,           -- 손익비
    parameters    TEXT,           -- JSON (임계값 파라미터)
    status        TEXT NOT NULL DEFAULT 'active',  -- active | testing | deprecated
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 매매팀 + 포지션 감시 출력: 거래 이력
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trades (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          DATE NOT NULL,
    ticker        TEXT NOT NULL,
    name          TEXT,
    action        TEXT NOT NULL,    -- buy | sell | stop_loss | take_profit | time_cut
    order_type    TEXT NOT NULL DEFAULT 'limit',  -- limit | market
    order_price   REAL,
    exec_price    REAL,
    quantity      INTEGER NOT NULL,
    tranche       INTEGER,          -- 분할 회차 (1 | 2 | 3)
    status        TEXT NOT NULL DEFAULT 'pending',  -- pending | filled | cancelled | failed
    pnl           REAL,             -- 손익 (원)
    pnl_pct       REAL,             -- 손익률 (%)
    signal_source TEXT,             -- hot_list | position_monitor | manual
    strategy_id   TEXT,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    filled_at     DATETIME
);

-- ────────────────────────────────────────
-- 포지션 감시 출력: 보유 포지션 스냅샷
-- 1~2분 주기로 갱신
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS position_snapshot (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker        TEXT NOT NULL,
    name          TEXT,
    quantity      INTEGER NOT NULL,
    avg_price     REAL NOT NULL,
    current_price REAL NOT NULL,
    pnl_pct       REAL NOT NULL,   -- 평균 단가 대비 손익률 (%)
    held_days     INTEGER NOT NULL DEFAULT 0,
    tranche1_qty  INTEGER DEFAULT 0,
    tranche2_qty  INTEGER DEFAULT 0,
    tranche3_qty  INTEGER DEFAULT 0,
    partial_sold  INTEGER DEFAULT 0,  -- 익절로 이미 부분 매도한 수량
    snapshot_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 포지션 감시: 트레일링 스톱 상태
-- 매수 시 생성, 포지션 청산 시 삭제
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trailing_stop (
    ticker           TEXT PRIMARY KEY,
    entry_price      REAL NOT NULL,        -- 최초 매수 평균 단가
    trailing_floor   REAL NOT NULL,        -- 현재 손절선 (단방향 상승만 허용)
    highest_price    REAL NOT NULL,        -- 진입 후 최고가
    ladder_bought    INTEGER NOT NULL DEFAULT 0,   -- 사다리 매수 실행 횟수 (큰 하락 시 평단 낮추기)
    scale_in_count   INTEGER NOT NULL DEFAULT 0,   -- 피라미딩 실행 횟수 (상승 시 비중 추가)
    dip_buy_count    INTEGER NOT NULL DEFAULT 0,   -- 스마트 물타기 횟수 (일시 눌림 + 조건 충족 시)
    scalp_exit_price REAL DEFAULT NULL,            -- 부분 익절(스캘핑) 실행 가격 (재진입 기준선)
    scalp_exit_qty   INTEGER DEFAULT 0,            -- 부분 익절 시 매도 수량 (재진입 목표 수량)
    trigger_pct      REAL NOT NULL DEFAULT 3.0,    -- 트레일링 시작 수익률 (%) — 종목·시황별 동적 설정
    floor_pct        REAL NOT NULL DEFAULT 2.5,    -- 트레일링 간격 (%) — 종목 변동성별 동적 설정
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 장중 분봉 캔들 (ATR·거래량 압력 계산용)
-- IntradayMACDEngine이 분봉 수집 시 저장 (종목별 최근 30봉 유지)
-- PositionMonitor가 ATR·거래량 압력 산출에 활용 (추가 API 호출 없음)
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS intraday_candles (
    ticker     TEXT NOT NULL,
    bar_time   TEXT NOT NULL,   -- HHmmss (1분봉 시각)
    open       REAL NOT NULL,
    high       REAL NOT NULL,
    low        REAL NOT NULL,
    close      REAL NOT NULL,
    volume     INTEGER NOT NULL,
    saved_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, bar_time)
);

CREATE INDEX IF NOT EXISTS idx_intraday_candles ON intraday_candles(ticker, bar_time DESC);

-- ────────────────────────────────────────
-- hot_list_max_rsi: 82↑ 완전차단 / hot_list_rsi_hot_limit: 72~82 포지션50% / hot_list_min_obv_slope: OBV 역행 차단

-- 장중 MACD 신호: 분봉 MACD Pre-Cross 감지 결과
-- IntradayMACDEngine이 3분 주기로 기록
-- TradingEngine·PositionMonitor가 참조
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS intraday_macd_signal (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker     TEXT NOT NULL,
    signal     TEXT NOT NULL,   -- buy_pre | sell_pre | hold
    hist_3m    REAL,            -- 3분봉 마지막 히스토그램
    hist_5m    REAL,            -- 5분봉 마지막 히스토그램
    macd_3m    REAL,
    signal_3m  REAL,
    macd_5m    REAL,
    signal_5m  REAL,
    sig_3m          TEXT NOT NULL DEFAULT 'hold',  -- 3분봉 개별 신호 (buy_pre|sell_pre|hold)
    sig_5m          TEXT NOT NULL DEFAULT 'hold',  -- 5분봉 개별 신호
    signal_strength REAL DEFAULT 0.0,              -- 신호 강도 (0~100)
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 데이터 수집 체크포인트
-- 450종목 스캔 중단 시 재시작 후 이어서 진행
-- cycle_id: 5분 단위 타임스탬프 (YYYYMMDDHHMM)
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fetch_checkpoint (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id   TEXT NOT NULL,           -- 사이클 ID (YYYYMMDDHHMM 5분 단위)
    scan_type  TEXT NOT NULL,           -- domestic_stock | universe | global_market
    item_key   TEXT NOT NULL,           -- 종목코드 또는 심볼
    status     TEXT NOT NULL DEFAULT 'done',  -- done | error
    error_msg  TEXT,
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(cycle_id, scan_type, item_key)
);

-- ────────────────────────────────────────
-- 거래소 사전 손절 주문 (KIS 지정가 매도)
-- 매수 직후 제출 → 트레일링 스톱 상향 시 취소 후 재제출
-- 시스템 다운 시에도 거래소 서버에서 자동 체결되는 안전망
--
-- 주의: 지정가 주문이므로 stop_price 이상에서만 체결됨
--       갭 하락 시에는 체결 안 될 수 있음 (폴링 시스템이 백업 역할)
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stop_orders (
    ticker      TEXT PRIMARY KEY,
    order_no    TEXT NOT NULL,             -- KIS ODNO (주문번호)
    krx_orgno   TEXT NOT NULL DEFAULT '',  -- KRX_FWDG_ORD_ORGNO (취소 시 필요)
    stop_price  REAL NOT NULL,             -- 지정가 손절 가격
    quantity    INTEGER NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 자동 파라미터 튜닝: 조정 가능한 전략 수치
-- 장 마감 후 복기 엔진이 자동 갱신 (안전 범위 내에서만)
-- 엔진들이 하드코딩 상수 대신 이 값을 우선 참조
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS strategy_params (
    param_name   TEXT PRIMARY KEY,
    current_val  REAL NOT NULL,
    default_val  REAL NOT NULL,
    min_val      REAL NOT NULL,    -- 안전 하한 (이 아래로 내리지 않음)
    max_val      REAL NOT NULL,    -- 안전 상한 (이 위로 올리지 않음)
    description  TEXT,
    tuned_by     TEXT DEFAULT 'default',  -- default | auto | manual
    updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 일일 복기: 매매 피드백 저장
-- 매 영업일 장 마감 후 자동 생성
-- TradingEngine·ResearchEngine이 참조해 전략 개선에 활용
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_review (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    review_date   DATE NOT NULL UNIQUE,
    total_trades  INTEGER NOT NULL DEFAULT 0,
    win_trades    INTEGER NOT NULL DEFAULT 0,
    loss_trades   INTEGER NOT NULL DEFAULT 0,
    total_pnl     REAL,              -- 당일 실현 손익 합계 (원)
    best_trade    TEXT,              -- JSON {ticker, pnl_pct, reason}
    worst_trade   TEXT,              -- JSON {ticker, pnl_pct, reason}
    pattern_hits  TEXT,              -- JSON array — 잘 작동한 패턴
    pattern_fails TEXT,              -- JSON array — 실패한 패턴
    improvements  TEXT,              -- JSON array — Claude 권고 개선사항
    summary       TEXT,              -- Claude 자연어 총평
    market_context TEXT,             -- JSON {regime, kospi_chg, kosdaq_chg, foreign_dir, global_risk, strategy_fit}
    signal_analytics TEXT,           -- JSON 신호 차원별 승률·손익 (자기학습 피드백 루프)
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 매수 진입 컨텍스트 (자기학습 피드백 루프)
-- 매수 체결 시 신호 메타데이터 저장 → 복기에서 신호별 승률 계산
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_context (
    trade_id       INTEGER PRIMARY KEY,   -- trades.id 참조
    ticker         TEXT NOT NULL,
    trade_date     DATE NOT NULL,
    signal_type    TEXT,                  -- gap_up_breakout | pullback_rebound | ...
    rsi            REAL,
    entry_score    REAL,                  -- Gate 4.2 신뢰도 점수 (0~100)
    momentum_score REAL,
    rs_daily       REAL,                  -- 당일 KOSPI 대비 초과수익률
    rs_5d          REAL,                  -- 5일 KOSPI 대비 누적
    sector         TEXT,
    exec_strength  REAL,
    ob_imbalance   REAL,
    entry_hhmm     TEXT,                  -- 진입 시각 (HHMM 문자열)
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 종목별 누적 패턴 통계 (자기학습 피드백)
-- 매도 체결 시 자동 갱신 → 다음 스캔 때 Claude 프롬프트에 반영
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ticker_stats (
    ticker            TEXT PRIMARY KEY,
    name              TEXT,
    total_trades      INTEGER NOT NULL DEFAULT 0,
    win_count         INTEGER NOT NULL DEFAULT 0,
    loss_count        INTEGER NOT NULL DEFAULT 0,
    win_rate          REAL DEFAULT 0.0,        -- win_count / total_trades
    avg_pnl_pct       REAL DEFAULT 0.0,        -- 평균 손익률 (%)
    avg_win_pct       REAL DEFAULT 0.0,        -- 평균 이익 거래 수익률 (%) — Kelly 분자
    avg_loss_pct      REAL DEFAULT 0.0,        -- 평균 손실 거래 손실률 (양수, %) — Kelly 분모
    avg_hold_minutes  REAL DEFAULT 0.0,        -- 평균 보유 시간 (분)
    best_entry_hour   INTEGER DEFAULT NULL,    -- 최고 성과 진입 시각 (시 단위, 0~15)
    frgn_buy_win_rate REAL DEFAULT NULL,       -- 외국인 순매수 시 승률
    inst_buy_win_rate REAL DEFAULT NULL,       -- 기관 순매수 시 승률
    best_signal_type  TEXT DEFAULT NULL,       -- 가장 성과 좋은 신호 유형
    notes             TEXT DEFAULT NULL,       -- Claude 패턴 메모 (복기 엔진 갱신)
    last_updated      DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
-- 인덱스
-- ────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_checkpoint_cycle   ON fetch_checkpoint(cycle_id, scan_type);
CREATE INDEX IF NOT EXISTS idx_hot_list_created   ON hot_list(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_date        ON trades(date DESC);
CREATE INDEX IF NOT EXISTS idx_trades_ticker      ON trades(ticker);
CREATE INDEX IF NOT EXISTS idx_trades_status      ON trades(status);
CREATE INDEX IF NOT EXISTS idx_position_snapshot  ON position_snapshot(snapshot_at DESC, ticker);
CREATE INDEX IF NOT EXISTS idx_risk_status_latest ON risk_status(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_expires  ON sentiment_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_universe_date      ON universe(active_date, ticker);
CREATE INDEX IF NOT EXISTS idx_trade_context_date ON trade_context(trade_date, ticker);
CREATE INDEX IF NOT EXISTS idx_slot_trade_date    ON slot_assignments(trade_date);
CREATE INDEX IF NOT EXISTS idx_hot_list_ticker    ON hot_list(ticker);
CREATE INDEX IF NOT EXISTS idx_macd_signal_ticker ON intraday_macd_signal(ticker, created_at DESC);

-- ────────────────────────────────────────
-- 전일 저녁 선점 종목 (방향 1 전략)
-- 매일 16:40 Claude가 내일 시초가 매수할 종목 1개를 선정해 저장
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tomorrow_pick (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pick_date   DATE NOT NULL,          -- 매수 실행 날짜 (내일)
    rank        INTEGER NOT NULL DEFAULT 1,  -- 우선순위 1~5
    ticker      TEXT NOT NULL,
    name        TEXT,
    reason      TEXT,                   -- Claude 선정 이유
    ref_price   REAL DEFAULT NULL,      -- 선정 시점 기준가 (갭 체크용)
    entry_price REAL DEFAULT NULL,      -- 실제 진입가 (체결 후 기록)
    status      TEXT DEFAULT 'pending', -- pending | executed | skipped
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(pick_date, rank)
);

-- ────────────────────────────────────────
-- 일일 시장 저널: 장 마감 직후(15:35) 누적 저장
-- 다음날 08:50 morning_picker가 최근 7일 시계열로 활용
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_market_journal (
    date              DATE PRIMARY KEY,                  -- 거래일
    kospi_close       REAL,
    kospi_chg_pct     REAL,
    kosdaq_close      REAL,
    kosdaq_chg_pct    REAL,
    foreign_net_buy   REAL,                              -- 외국인 순매수 (억원)
    inst_net_buy      REAL,                              -- 기관 순매수 (억원)
    top30_by_value    TEXT,                              -- JSON: [{ticker,name,value_krw,chg_pct,sector},...]
    sector_scores     TEXT,                              -- JSON: {sector_name: score,...}
    notable_themes    TEXT,                              -- JSON: ["AI반도체","조선","바이오"] (Claude 요약)
    summary           TEXT,                              -- Claude 시장 요약 (문장형)
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_daily_market_journal_date ON daily_market_journal(date DESC);

-- ════════════════════════════════════════════════════════════════════
-- 어시스턴트 모델 전환 (2026-05-12) — Phase 2 신규 테이블
-- ════════════════════════════════════════════════════════════════════

-- ① 일일 거래대금 TOP N 스냅샷 (매일 15:35 적재)
CREATE TABLE IF NOT EXISTS daily_top_value (
    date              DATE NOT NULL,
    rank              INTEGER NOT NULL,
    ticker            TEXT NOT NULL,
    name              TEXT,
    sector            TEXT,
    -- 시세
    open_price        REAL,
    high_price        REAL,
    low_price         REAL,
    close_price       REAL,
    prev_close        REAL,
    chg_pct           REAL,
    volume            INTEGER,
    trading_value     REAL,
    market_cap        REAL,
    listed_shares     INTEGER,
    high_52w          REAL,
    low_52w           REAL,
    per               REAL,
    pbr               REAL,
    eps               REAL,
    bps               REAL,
    -- 수급
    indiv_net_buy     REAL,
    foreign_net_buy   REAL,
    foreign_hold_pct  REAL,
    inst_net_buy      REAL,
    program_net_buy   REAL,
    margin_balance    REAL,
    -- 공매도
    short_volume      INTEGER,
    short_value       REAL,
    short_ratio       REAL,
    -- 기술지표 (분봉/일봉 기반, 시스템 자체 계산)
    rsi_14            REAL,
    macd_signal       TEXT,  -- 'bull' | 'bear' | 'neutral'
    atr_pct           REAL,
    bb_width_ratio    REAL,
    -- 메타
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_daily_top_value_date   ON daily_top_value(date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_top_value_ticker ON daily_top_value(ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_top_value_sector ON daily_top_value(sector, date DESC);

-- ② 종목 테마 태깅
CREATE TABLE IF NOT EXISTS ticker_themes (
    ticker      TEXT NOT NULL,
    theme       TEXT NOT NULL,
    weight      REAL DEFAULT 1.0,
    source      TEXT NOT NULL,  -- 'krx' | 'news' | 'manual' | 'naver'
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, theme, source)
);
CREATE INDEX IF NOT EXISTS idx_ticker_themes_theme ON ticker_themes(theme);

-- ③ 미국 시장 일일 스냅샷
CREATE TABLE IF NOT EXISTS us_market_daily (
    date              DATE PRIMARY KEY,           -- 한국시간 기준 (전일 미국장 마감)
    sp500_close       REAL,
    sp500_chg_pct     REAL,
    nasdaq_close      REAL,
    nasdaq_chg_pct    REAL,
    dow_close         REAL,
    dow_chg_pct       REAL,
    vix               REAL,
    vix_chg           REAL,
    us10y_yield       REAL,
    -- 주요 ETF
    soxx              REAL,  -- 반도체
    soxx_chg_pct      REAL,
    lit               REAL,  -- 리튬/2차전지
    lit_chg_pct       REAL,
    -- 거래량 상위 (JSON: [{ticker, name_kr, volume, chg_pct},...])
    top_volume_tickers TEXT,
    -- 주요 종목 (NVDA, TSM, AAPL, MSFT, GOOGL, TSLA, AMD, META)
    key_stocks         TEXT, -- JSON
    created_at         DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ④ 아침 브리핑 (08:45 발송)
CREATE TABLE IF NOT EXISTS morning_briefing (
    date            DATE PRIMARY KEY,
    overnight_us    TEXT,  -- JSON: us_market_daily 요약
    macro           TEXT,  -- JSON: 환율/금리/유가
    kr_context      TEXT,  -- JSON: 어제 KOSPI/KOSDAQ + 외인/기관 수급
    market_regime   TEXT,  -- 'strong'|'sideways'|'weak'|'reversal'|'volatile'
    sectors_hot     TEXT,  -- JSON: [{sector, score, reason}]
    sectors_watch   TEXT,  -- JSON
    sectors_cold    TEXT,  -- JSON
    sectors_avoid   TEXT,  -- JSON
    picks           TEXT,  -- JSON: [{rank, ticker, name, reason, confidence, entry, stop_loss, take_profit, themes, risk}]
    avoids          TEXT,  -- JSON: [{ticker, name, reason}]
    lessons_applied TEXT,  -- JSON: 적용된 learnings ID 리스트
    strategy_tone   TEXT,  -- 오늘 전략 톤 텍스트
    headline        TEXT,  -- 한 줄 요약
    full_message    TEXT,  -- 텔레그램으로 보낸 전체 메시지 원문
    sent_at         DATETIME
);

-- ⑤ 저녁 회고 (15:40 발송)
CREATE TABLE IF NOT EXISTS evening_review (
    date              DATE PRIMARY KEY,
    market_summary    TEXT,  -- JSON: KOSPI/KOSDAQ 종가/거래대금/수급
    sectors_strong    TEXT,  -- JSON: 오늘 강세 섹터 TOP 5
    sectors_weak      TEXT,  -- JSON: 오늘 약세 섹터 TOP 5
    top10_volume      TEXT,  -- JSON: 거래대금 TOP 10 + 어제 대비 순위 변화
    picks_result      TEXT,  -- JSON: 아침 추천 결과 [{ticker, predicted, actual, hit/miss}]
    avoids_result     TEXT,  -- JSON: 회피 종목 결과
    accuracy_pct      REAL,  -- 적중률
    accuracy_avoid_pct REAL, -- 회피 적중률
    new_lessons       TEXT,  -- JSON: 오늘 도출 새 교훈
    lessons_validated TEXT,  -- JSON: 검증된 기존 교훈 ID
    lessons_failed    TEXT,  -- JSON: 실패한 기존 교훈 ID
    tomorrow_outlook  TEXT,  -- 내일 전망
    headline          TEXT,  -- 한 줄 요약
    full_message      TEXT,  -- 텔레그램 메시지 원문
    sent_at           DATETIME,
    FOREIGN KEY (date) REFERENCES morning_briefing(date)
);

-- ⑥ 누적 학습 (자기 개선 핵심)
CREATE TABLE IF NOT EXISTS learnings (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    discovered_at     DATE NOT NULL,
    category          TEXT NOT NULL,  -- 'pattern' | 'sector' | 'macro' | 'avoid' | 'entry_timing' | 'risk'
    content           TEXT NOT NULL,
    evidence          TEXT,  -- JSON: [{date, observation, outcome}]
    confidence        REAL DEFAULT 0.5,  -- 0.0 ~ 1.0
    times_validated   INTEGER DEFAULT 0,
    times_failed      INTEGER DEFAULT 0,
    last_used         DATE,
    last_validated    DATE,
    status            TEXT DEFAULT 'active',  -- 'active' | 'deprecated' | 'experimental'
    -- 시장 국면별 적용 (옵션 Q Phase 2-D): NULL=전체 / JSON ["strong","weak","sideways","reversal","volatile"]
    applicable_regime TEXT,
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_learnings_category ON learnings(category, status);
CREATE INDEX IF NOT EXISTS idx_learnings_confidence ON learnings(confidence DESC, status);

-- ⑦ KOSDAQ 시황 보강 (market_condition은 KOSPI 중심) — 별도 테이블로 분리
CREATE TABLE IF NOT EXISTS kosdaq_condition (
    date              DATE PRIMARY KEY,
    close             REAL,
    chg_pct           REAL,
    volume            REAL,        -- 거래량 (만주)
    trading_value     REAL,        -- 거래대금 (억원)
    foreign_net_buy   REAL,
    inst_net_buy      REAL,
    indiv_net_buy     REAL,
    program_net_buy   REAL,
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    source            TEXT         -- 'naver' | 'pykrx' | 'kis' | NULL (수급 데이터 출처)
);

-- ════════════════════════════════════════════════════════════════════
-- 일일 주요 뉴스 (2026-05-13) — morning_brief / evening_review 통합
-- 태그 기반 영구 보관. 4분류 (macro/sector/company/risk)
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS daily_news (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            DATE NOT NULL,
    market          TEXT NOT NULL,           -- 'kr' | 'us' | 'global'
    source          TEXT,                    -- '네이버' / 'Yahoo' / 'Reuters' / ...
    headline        TEXT NOT NULL,           -- 한글 (해외도 번역)
    headline_orig   TEXT,                    -- 원문 (해외인 경우)
    summary         TEXT,                    -- Claude 1~2줄 요약
    category        TEXT NOT NULL,           -- 'macro' | 'sector' | 'company' | 'risk'
    tags            TEXT,                    -- JSON: ["반도체", "FOMC", "엔비디아"]
    related_tickers TEXT,                    -- JSON: ["005380","000660"]
    importance      INTEGER DEFAULT 3,       -- 1~5 (Claude 평가)
    url             TEXT,
    published_at    DATETIME,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_daily_news_date     ON daily_news(date DESC, importance DESC);
CREATE INDEX IF NOT EXISTS idx_daily_news_category ON daily_news(category, date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_news_market   ON daily_news(market, date DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_news_url_uniq ON daily_news(url) WHERE url IS NOT NULL;
