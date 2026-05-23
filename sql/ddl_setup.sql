%sql
-- =============================================================
-- IndianRailways BigData Pipeline — DDL Setup
-- Updated with SCD Type 1 & Type 2 tables
-- Run on: Azure Databricks with Unity Catalog
-- FIX: Converted to Managed Tables (Removed legacy /mnt/ locations)
-- =============================================================

-- ─────────────────────────────────────────────────────────────
-- DATABASES / SCHEMAS
-- ─────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Raw ingestion layer — no transforms';
CREATE DATABASE IF NOT EXISTS silver COMMENT 'Cleaned & enriched — 25.8M records';
CREATE DATABASE IF NOT EXISTS gold   COMMENT 'Business metrics, SCD dimensions';

-- =============================================================
-- BRONZE LAYER
-- =============================================================

-- Raw delay log records (76.8M rows in production)
CREATE TABLE IF NOT EXISTS bronze.raw_delay_logs (
    train_no        STRING,
    station_code    STRING,
    station_name    STRING,
    sched_arr       STRING,
    actual_arr      STRING,
    sched_dep       STRING,
    actual_dep      STRING,
    delay_arr_mins  INT,
    delay_dep_mins  INT,
    event_date      STRING,
    source_file     STRING
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Bronze — raw CSV delay records, 76.8M rows'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- Raw train schedule JSON (exploded)
CREATE TABLE IF NOT EXISTS bronze.raw_train_schedules (
    train_no        STRING,
    train_name      STRING,
    zone            STRING,
    origin          STRING,
    destination     STRING,
    total_stops     INT,
    schedule_json   STRING,  -- raw nested JSON blob
    ingested_at     TIMESTAMP
)
USING DELTA
COMMENT 'Bronze — nested JSON schedules before Regex extraction';

-- Live train status snapshots (new — feeds SCD pipeline)
CREATE TABLE IF NOT EXISTS bronze.live_train_status (
    train_no        STRING  NOT NULL,
    train_name      STRING,
    zone            STRING,
    current_station STRING,
    station_code    STRING,
    scheduled_dep   STRING,
    actual_dep      STRING,
    delay_mins      INT,
    status          STRING,   -- On Time | Running | Late
    run_date        STRING,
    ingested_at     TIMESTAMP,
    data_source     STRING    -- RailRadar_API | NTES_Snapshot
)
USING DELTA
PARTITIONED BY (run_date, zone)
COMMENT 'Bronze — live NTES/RailRadar snapshots; appended each run';

-- =============================================================
-- SILVER LAYER
-- =============================================================

-- Enriched delay facts: Regex-parsed station codes + join-recovered
-- Result of the 99% join-loss recovery (25.8M rows)
CREATE TABLE IF NOT EXISTS silver.fact_train_delays (
    train_no        STRING  NOT NULL,
    train_name      STRING,
    station_code    STRING  NOT NULL,  -- extracted via Regex from JSON metadata
    station_name    STRING,
    zone            STRING,
    event_date      DATE    NOT NULL,
    sched_arr       STRING,
    actual_arr      STRING,
    delay_arr_mins  INT,
    sched_dep       STRING,
    actual_dep      STRING,
    delay_dep_mins  INT,
    distance_km     INT,
    processed_at    TIMESTAMP
)
USING DELTA
PARTITIONED BY (zone, event_date)
COMMENT 'Silver — 25.8M enriched delay records post join-recovery'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.dataSkippingNumIndexedCols' = '4'
);

-- =============================================================
-- GOLD LAYER — AGGREGATED METRICS (existing)
-- =============================================================

-- Top delayed trains (fact aggregate)
CREATE TABLE IF NOT EXISTS gold.agg_train_delay_rankings (
    train_no            STRING,
    train_name          STRING,
    zone                STRING,
    avg_delay_mins      DOUBLE,
    max_delay_mins      INT,
    total_delay_events  BIGINT,
    p95_delay_mins      DOUBLE,
    computed_date       DATE
)
USING DELTA
COMMENT 'Gold — top delayed trains; recomputed daily';

-- Station congestion metrics
CREATE TABLE IF NOT EXISTS gold.agg_station_congestion (
    station_code        STRING,
    station_name        STRING,
    zone                STRING,
    avg_arr_delay_mins  DOUBLE,
    avg_dep_delay_mins  DOUBLE,
    trains_per_day      INT,
    congestion_score    DOUBLE,
    computed_date       DATE
)
USING DELTA
COMMENT 'Gold — station-level bottleneck analysis';

-- =============================================================
-- GOLD LAYER — SCD TYPE 1 DIMENSION (NEW)
-- =============================================================

CREATE TABLE IF NOT EXISTS gold.dim_train_master_scd1 (
    train_no        STRING  NOT NULL COMMENT 'Natural business key (IRCTC train number)',
    train_name      STRING,
    zone            STRING  COMMENT 'Current IR zone: WR|ER|NR|SR|SCR|SWR|NCR|ECR|WCR|...',
    rake_type       STRING  COMMENT 'ICF | LHB | VB T18 | VB T20',
    classes         STRING  COMMENT 'Comma-separated: 1A,2A,3A,SL,CC,EC,GS',
    max_speed_kmh   INT     COMMENT 'Authorised max speed in km/h',
    pantry          STRING  COMMENT 'Yes | No | Cafe',
    origin_station  STRING,
    dest_station    STRING,
    distance_km     INT,
    frequency       STRING  COMMENT 'Daily | Weekly | BiWeekly | TriWeekly',
    source_system   STRING,
    updated_at      TIMESTAMP COMMENT 'Last SCD1 MERGE timestamp — no prior values stored'
)
USING DELTA
COMMENT 'SCD Type 1 — current train master; old values are silently overwritten'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- =============================================================
-- GOLD LAYER — SCD TYPE 2 DIMENSION (NEW)
-- =============================================================

CREATE TABLE IF NOT EXISTS gold.dim_train_schedule_scd2 (
    surrogate_key       STRING  NOT NULL COMMENT 'UUID — unique per train version',
    train_no            STRING  NOT NULL COMMENT 'Natural business key',
    train_name          STRING,
    zone                STRING,
    origin_station      STRING,
    dest_station        STRING,
    scheduled_dep       STRING  COMMENT 'HH:MM origin departure',
    scheduled_arr       STRING  COMMENT 'HH:MM destination arrival',
    journey_duration_h  DOUBLE,
    route_stops         INT     COMMENT 'Number of intermediate halts',
    distance_km         INT,
    frequency           STRING,
    eff_from_date       DATE    NOT NULL COMMENT 'Version valid from (inclusive)',
    eff_to_date         DATE    NOT NULL COMMENT '9999-12-31 for current row',
    is_current          STRING  NOT NULL COMMENT 'Y = active | N = expired',
    change_reason       STRING  COMMENT 'Human-readable reason for version creation'
)
USING DELTA
PARTITIONED BY (zone)
COMMENT 'SCD Type 2 — full schedule history; enables point-in-time delay attribution'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'  -- stream SCD changes downstream
);