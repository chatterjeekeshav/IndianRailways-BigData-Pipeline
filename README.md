# RailFlow: Indian Railways Medallion Lakehouse 

## Summary
RailFlow is a production-grade data lakehouse built on **Azure Databricks** and **PySpark**. It processes 76.8 million historical Indian Railways records while orchestrating a daily pipeline to track live schedule changes. Utilizing a Medallion Architecture and Slowly Changing Dimensions (SCD), it transforms raw, messy logs into high-value business metrics like train delay rankings and station bottleneck analysis.

## Architecture (Unity Catalog Enabled)
* **Bronze:** Raw ingestion of 76.8M historical CSVs and daily API JSON snapshots.
* **Silver:** Cleaned, schema-enforced, and enriched via Regex parsing and multi-key joins.
* **Gold (Insights & Dimensions):** * **SCD Type 1 (`dim_train_master_scd1`):** Overwrites current train attributes (zone, classes).
  * **SCD Type 2 (`dim_train_schedule_scd2`):** Tracks historical schedule changes using Delta Lake's Change Data Feed (CDF).
  * **Aggregations:** Actionable metrics for train performance and station congestion.

## Key Engineering Challenges Solved
1. **The "99% Join Loss" Recovery:** Used dynamic Regex to extract abbreviated station codes from nested JSON, recovering 25.8M records that initially failed inner joins.
2. **Performance at Scale:** Processed 76M+ rows efficiently by implementing Broadcast Hash Joins and Z-Ordering (`train_no`, `station_code`).
3. **Unity Catalog Migration:** Transitioned from legacy DBFS mounts to Managed Tables, securing the pipeline and resolving permission exceptions.

## Tech Stack
**Azure Databricks** | **PySpark** | **Delta Lake (ACID, CDF)** | **Unity Catalog**

## Repository Structure

RailFlow Analytics/
├── notebooks/
│   ├── 00_bootstrap_scd_data.ipynb          # Historical Delta table seeder
│   ├── 01_ingest_bronze.ipynb               # Raw data landing 
│   ├── 03_process_silver.ipynb              # Regex & Enrichment logic
│   ├── 04_scd_type1_train_master.ipynb      # Daily Incremental: SCD1 Merge
│   ├── 05_scd_type2_schedule_history.ipynb  # Daily Incremental: SCD2 Merge
│   └── 06_live_data_ingestion.ipynb         # API/Offline snapshot ingestion
├── sql/
│   └── ddl_setup.sql                        # Unity Catalog Managed Tables
└── README.md