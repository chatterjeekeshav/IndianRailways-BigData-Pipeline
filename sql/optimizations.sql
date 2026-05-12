-- =================================================================---
-- PROJECT: RailFlow - 76M+ Record Analytics Pipeline
-- DESCRIPTION: Performance Tuning & Storage Maintenance
-- =================================================================---

-- 1. Physical Data Layout Optimization
-- This significantly speeds up joins and filters for specific trains or regions.
OPTIMIZE railway_analytics.silver.fact_train_delays_enriched
ZORDER BY (train_no, station_code);

-- 2. Statistics Collection
-- Ensures the Spark Catalyst Optimizer has the most accurate metadata 
-- for query planning across the 25.8M row Silver table.
ANALYZE TABLE railway_analytics.silver.fact_train_delays_enriched 
COMPUTE STATISTICS FOR ALL COLUMNS;

-- 3. Storage Maintenance
-- Removes old versions of data files no longer needed by Delta Time Travel.
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM railway_analytics.silver.fact_train_delays_enriched RETAIN 168 HOURS;

-- 4. Gold Layer Performance
OPTIMIZE railway_analytics.gold.top_delayed_trains;
OPTIMIZE railway_analytics.gold.station_bottlenecks;