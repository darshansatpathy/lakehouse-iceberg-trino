# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-08

### Added

- Docker Compose stack: PostgreSQL as the Iceberg JDBC catalog, MinIO, Trino 476 with the
  Iceberg connector and native S3, and an optional Superset profile.
- Deterministic CDC generator and an independent reference of the expected state.
- pyiceberg ingestion with schema evolution (a new source column) and partition evolution
  (monthly to daily).
- dbt project on Trino: staging, an incremental MERGE for current state with tombstones, gold
  aggregates, and data tests including late-event detection.
- Maintenance commands (optimize, expire_snapshots, remove_orphan_files) and table health.
- `lh verify`: 27 end-to-end checks; `lh bench`: partition pruning and compaction, measured.
