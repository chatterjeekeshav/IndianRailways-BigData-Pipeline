RailFlow is a production-grade data lakehouse built on Azure Databricks and PySpark. It processes a massive dataset of 76.8 million Indian Railways records, transforming raw, messy logs into high-value business metrics. This project implements a full Medallion Architecture, addressing complex challenges in data reconciliation, schema evolution, and big data optimization.

->Architecture
    The pipeline follows the Medallion Architecture to ensure data quality and reliability:

    Bronze (Raw): Direct ingestion of 76.8M CSV delay records and nested JSON train schedules.

    Silver (Enriched): Data cleaning, feature extraction via Python Regex, and complex joins resulting in a 25.8M row.

    Gold (Insights): Aggregated metrics for train performance rankings and station-level bottleneck analysis.

->Key Engineering Challenges & Solutions
    1. The "99% Join Loss" Recovery
    During initial processing, an inner join between the delay logs and schedules resulted in a 99% data loss.

    Problem: Discovered via diagnostic Left-Anti Joins that the 76M dataset stored abbreviated Station Codes (e.g., "NDLS") in a column labeled for names, while the JSON schedule used full names.

    Solution: Implemented dynamic Regex Parsing to extract codes from the JSON metadata and performed a multi-key join, recovering 25.8 million records for analysis.

    2. Performance at Scale (76.8M Records)
    Processing tens of millions of rows requires efficient resource management:

    Broadcast Hash Joins: Optimized the join between the massive fact table and smaller dimension tables to minimize network shuffle.

    Z-Ordering: Physically co-located data on disk by train_no and station_code in the Silver layer, reducing query latency for downstream BI tools.

->Repository Structure
    Indian Railways Performance Analytics Pipeline
    ├── notebooks/
    │   ├── 01_ingest_bronze.ipynb          # Raw data landing & schema setup
    │   ├── 2_Resource_Conversion_JSON_Notebook.ipynb   # Converting Json data 
    │   ├── 03_process_silver.ipynb         # Regex parsing & Enrichment logic
    │   └── 03_create_gold_metrics.ipynb    # Business aggregations
    ├── sql/
    │   ├── ddl_setup.sql            # Delta Table definitions
    │   └── optimizations.sql        # Z-Ordering & maintenance scripts
    ├── data/
    │   └── sample_delays.csv        # 1k row sample for testing
    └── README.md


->Business Insights (Gold Layer)
    The pipeline generates actionable metrics saved in managed Delta tables:

    Train Delay Rankings: Identifies the top 10 most chronically delayed trains across India.

    Station Congestion: Pinpoints junctions with the highest average arrival delays to identify infrastructure bottlenecks.

->Tech Stack
    Engine: PySpark (Spark 3.x)

    Platform: Azure Databricks

    Storage: Delta Lake (ACID compliant)

    Orchestration: Unity Catalog for data governance


NOTE: This repository uses a 500-row sample for lightweight cloning, while the underlying logic is fully optimized for the 76.8M record production dataset. In accordance with Big Data best practices, the ~1GB raw source is excluded from version control to favor a decoupled architecture designed for cloud storage like Azure Data Lake Storage (ADLS).