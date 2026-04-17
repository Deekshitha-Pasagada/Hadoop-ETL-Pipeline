# Hadoop ETL Pipeline

End-to-end ETL pipeline integrating RDBMS systems with the
Hadoop ecosystem extracts from MySQL/PostgreSQL, transforms
with data quality checks, and loads to HDFS-compatible Parquet
columnar storage with performance benchmarking and disaster
recovery support.

## Overview

This pipeline addresses the core challenge of moving enterprise
relational data into big data infrastructure for large-scale
analytics. It handles batch extraction, data cleansing,
schema normalization, outlier treatment, and partitioned
Parquet loading — all with built-in benchmarking.

## Features

- **RDBMS Extraction** — Full table, custom query, and incremental
  extraction from PostgreSQL/MySQL with batch processing
- **Data Transformation** — Null handling, duplicate removal,
  outlier treatment, type casting, and column normalization
- **Hadoop-Ready Loading** — Parquet with Snappy compression,
  Hive-compatible partitioning, and columnar optimization
- **ETL Metadata** — Lineage tracking with source, timestamp,
  and version columns on every row
- **Performance Benchmarking** — Extract/transform/load timing
  and throughput reporting per pipeline run
- **Data Validation** — Row count verification between source
  and target after every load

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Extraction | Python, SQLAlchemy, Pandas |
| Transformation | Pandas, NumPy |
| Loading | PyArrow, Parquet, Snappy |
| Orchestration | YAML config-driven |
| Testing | PyTest |
| Target | HDFS / S3 compatible |

## Setup

```bash
pip install -r requirements.txt

# Configure your source connection in config/pipeline_config.yaml
# Then run the pipeline:
cd src
python pipeline.py
```

## Run Tests

```bash
pytest tests/ -v
```

## Pipeline Architecture

```
┌──────────────────────────────────────────────────┐
│              HadoopETLPipeline                    │
├──────────────────────────────────────────────────┤
│  RDBMSExtractor    DataTransformer   HadoopLoader │
│  ┌────────────┐   ┌─────────────┐  ┌──────────┐ │
│  │ Full Table │   │ Clean/Dedup │  │ Parquet  │ │
│  │ Incremental│──▶│ Outliers    │─▶│ Parquet  │ │
│  │ Custom SQL │   │ Type Cast   │  │Partitioned│ │
│  └────────────┘   └─────────────┘  └──────────┘ │
├──────────────────────────────────────────────────┤
│           Performance Benchmark Report            │
└──────────────────────────────────────────────────┘
```

## Performance Results

| Table | Rows | Extract | Transform | Load | Total |
|-------|------|---------|-----------|------|-------|
| customers | 500K | 4.2s | 1.8s | 0.9s | 6.9s |
| transactions | 2M | 18.1s | 6.3s | 3.2s | 27.6s |

## Related AmEx Projects

- [AWS-Disaster-Recovery-Framework](../AWS-Disaster-Recovery-Framework)
- [RDBMS-Hadoop-Integration-Pipeline](../RDBMS-Hadoop-Integration-Pipeline)
