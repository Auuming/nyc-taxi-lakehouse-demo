# nyc-taxi-lakehouse-demo

## Phase
- Phase 1: Build a minimal lakehouse using the NYC Yellow Taxi dataset.
- Phase 2: Optimize transformations, docker with MinIO, add additional analytics, and improve query performance.
- Phase 3: Support Green Taxi, FHV, and HVFHV datasets with a unified schema and cross-service analytics.

## Checkbox
- Download Dataset
- ~~Bronze Pipeline~~
- ~~Silver Pipeline~~
- Gold Pipeline
- Iceberg Catalog
- SQL Analytics
- visualization

## Future Improvement
- Phase 2
  - add recover and checkpoint in pipeline for distributed transaction between log and table
  - review fare validation rules and improve Silver-layer data cleaning logic.
  - Dashboard
  - MinIO
  - Docker Compose
- Addition
  - Documentation