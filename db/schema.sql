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
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP
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
    ticker         TEXT PRIMARY KEY,
    entry_price    REAL NOT NULL,        -- 최초 매수 평균 단가
    trailing_floor REAL NOT NULL,        -- 현재 손절선 (단방향 상승만 허용)
    highest_price  REAL NOT NULL,        -- 진입 후 최고가
    ladder_bought  INTEGER NOT NULL DEFAULT 0,   -- 사다리 매수 실행 횟수 (하락 시 추가)
    scale_in_count INTEGER NOT NULL DEFAULT 0,   -- 피라미딩 실행 횟수 (상승 시 추가)
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────
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
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
