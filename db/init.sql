-- init.sql (final recommended)
-- Ensure Timescale extension present
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Use UTC timezone for consistency
SET timezone = 'UTC';

-- K-line table: use composite primary key that includes the timestamp (required for hypertable)
CREATE TABLE IF NOT EXISTS kline_data (
  exchange TEXT NOT NULL,                  -- 数据来源的交易所（binance、okx等）
  symbol TEXT NOT NULL,                    -- 交易对（BTC-USDT）
  timeframe TEXT NOT NULL,                 -- 时间粒度（1m、5m、1h）
  timestamp TIMESTAMPTZ NOT NULL,          -- 这根K线的起始时间
  open NUMERIC(30,8) NOT NULL,             -- 开盘价
  high NUMERIC(30,8) NOT NULL,             -- 最高价
  low NUMERIC(30,8) NOT NULL,              -- 最低价
  close NUMERIC(30,8) NOT NULL,            -- 收盘价
  volume NUMERIC(40,8) NOT NULL,           -- 成交量（币的数量，比如BTC的数量）
  quote_volume NUMERIC(40,8),              -- 成交额（计价货币的数量，比如USDT）
  trade_count INTEGER,                     -- 交易笔数（部分交易所提供）
  taker_buy_volume NUMERIC(40,8),          -- 主动买单成交量
  taker_buy_quote_volume NUMERIC(40,8),    -- 主动买单成交额
  created_at TIMESTAMPTZ DEFAULT NOW(),    -- 插入到数据库的时间（不是K线时间）
  PRIMARY KEY (exchange, symbol, timeframe, timestamp)
);


CREATE INDEX IF NOT EXISTS idx_kline_symbol_timeframe ON kline_data (symbol, timeframe);
CREATE INDEX IF NOT EXISTS idx_kline_timestamp ON kline_data (timestamp);

-- Create hypertable with a reasonable chunk interval (adjust for your load: e.g. '7 days' or '1 day')
SELECT create_hypertable('kline_data', 'timestamp', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);

-- Enable compression (optional) and add compression policy (compress data older than 30 days)
ALTER TABLE kline_data SET (timescaledb.compress = true, timescaledb.compress_segmentby = 'symbol, timeframe');
SELECT add_compression_policy('kline_data', INTERVAL '30 days');

-- Strategy signals (separate table; normal table with BIGSERIAL PK)
CREATE TABLE IF NOT EXISTS strategy_signals (
  id BIGSERIAL PRIMARY KEY,                -- 自增ID，唯一标识（日志型表常用）
  strategy_name TEXT NOT NULL,             -- 策略名称（例如 "mean_reversion"）
  symbol TEXT NOT NULL,                    -- 对应交易对
  action TEXT NOT NULL,                    -- 动作（BUY/SELL/HOLD）
  strength NUMERIC(8,4),                   -- 信号强度（可选：0.0-1.0）
  timestamp TIMESTAMPTZ NOT NULL,          -- 信号产生的时间
  price NUMERIC(30,8),                     -- 当时的价格
  meta JSONB,                              -- 存放额外信息（指标值、因子组合、debug信息）
  created_at TIMESTAMPTZ DEFAULT NOW()     -- 插入时间
);


CREATE INDEX IF NOT EXISTS idx_signals_strategy ON strategy_signals (strategy_name, timestamp);
