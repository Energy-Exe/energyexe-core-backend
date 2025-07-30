-- This SQL file should be run manually after installing TimescaleDB extension
-- It creates the necessary tables and hypertables for storing time-series generation data

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create hypertable for generation data
CREATE TABLE IF NOT EXISTS power_generation_data (
    time TIMESTAMP NOT NULL,
    area_code VARCHAR(10) NOT NULL,
    generation_unit_code VARCHAR(50),
    generation_unit_source VARCHAR(20) DEFAULT 'ENTSOE',
    production_type VARCHAR(20) NOT NULL,
    value_mw FLOAT NOT NULL,
    data_quality_score FLOAT DEFAULT 1.0,
    fetch_history_id INTEGER REFERENCES entsoe_fetch_history(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_power_generation_data PRIMARY KEY (time, area_code, production_type)
);

-- Convert to hypertable
SELECT create_hypertable('power_generation_data', 'time', 
  chunk_time_interval => INTERVAL '1 week',
  if_not_exists => TRUE);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_generation_area_type_time 
  ON power_generation_data (area_code, production_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_generation_unit_time 
  ON power_generation_data (generation_unit_code, time DESC);
CREATE INDEX IF NOT EXISTS idx_generation_fetch_history 
  ON power_generation_data (fetch_history_id);

-- Create continuous aggregate for hourly data
CREATE MATERIALIZED VIEW IF NOT EXISTS generation_hourly_summary
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', time) AS hour,
    area_code,
    production_type,
    AVG(value_mw) as avg_mw,
    MIN(value_mw) as min_mw,
    MAX(value_mw) as max_mw,
    COUNT(*) as data_points
FROM power_generation_data
GROUP BY hour, area_code, production_type
WITH NO DATA;

-- Refresh policy for continuous aggregate
SELECT add_continuous_aggregate_policy('generation_hourly_summary',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- Create continuous aggregate for daily data
CREATE MATERIALIZED VIEW IF NOT EXISTS generation_daily_summary
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', time) AS day,
    area_code,
    production_type,
    AVG(value_mw) as avg_mw,
    MIN(value_mw) as min_mw,
    MAX(value_mw) as max_mw,
    SUM(value_mw) as total_mw,
    COUNT(*) as data_points
FROM power_generation_data
GROUP BY day, area_code, production_type
WITH NO DATA;

-- Refresh policy for daily aggregate
SELECT add_continuous_aggregate_policy('generation_daily_summary',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

-- Data retention policy (keep raw data for 1 year)
SELECT add_retention_policy('power_generation_data', 
    INTERVAL '1 year',
    if_not_exists => TRUE);

-- Compression policy (compress data older than 1 month)
SELECT add_compression_policy('power_generation_data', 
    INTERVAL '1 month',
    if_not_exists => TRUE);