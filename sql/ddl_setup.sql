-- =================================================================---
-- PROJECT: RailFlow - 76M+ Record Analytics Pipeline
-- DESCRIPTION: Database and Table Definitions (Medallion Architecture)
-- =================================================================---

-- 0. Database Environment Setup
CREATE DATABASE IF NOT EXISTS railway_analytics;
USE railway_analytics;

-- 1. BRONZE LAYER: Raw Data Ingestion
-- Raw landing for 76.8M CSV delay records
CREATE TABLE IF NOT EXISTS bronze.raw_train_delays (
    train_no INT,
    station_name STRING, -- Current source stores Station Codes here
    delay INT,
    arrival_time STRING,
    departure_time STRING,
    _ingested_at TIMESTAMP
) USING DELTA;

-- Raw landing for train schedules (JSON source)
CREATE TABLE IF NOT EXISTS bronze.raw_train_schedules (
    train_no INT,
    schedule_json STRING,
    _ingested_at TIMESTAMP
) USING DELTA;

-- 2. SILVER LAYER: Enriched & Cleaned Tables
-- The "Source of Truth" after solving the 99% Join Loss anomaly
CREATE TABLE IF NOT EXISTS silver.fact_train_delays_enriched (
    train_no INT,
    train_name STRING,
    station_code STRING,      -- Normalized MJ, FLD, etc.
    station_name_full STRING, -- Normalized Marwar Jn, Phulad, etc.
    delay_minutes INT,
    arrival_status STRING,    -- Derived from delay logic
    _processed_at TIMESTAMP
) USING DELTA;

-- 3. GOLD LAYER: Business-Ready Aggregations
-- Aggregated performance metrics for BI Dashboards
CREATE TABLE IF NOT EXISTS gold.top_delayed_trains (
    train_no INT,
    train_name STRING,
    total_stops_recorded LONG,
    average_delay_minutes DOUBLE
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.station_bottlenecks (
    station_code STRING,
    station_name_full STRING,
    total_traffic LONG,
    avg_station_delay DOUBLE
) USING DELTA;