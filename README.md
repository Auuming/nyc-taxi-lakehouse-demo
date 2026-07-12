# NYC Taxi Lakehouse Demo

A minimal local data lakehouse built with NYC Yellow Taxi trip data,
Apache Iceberg, PyArrow, PyIceberg, and SQL analytics.

## Project Phases

### Phase 1 — Minimal Lakehouse

Build a complete end-to-end lakehouse using three months of
NYC Yellow Taxi data.

- [x] Download Yellow Taxi datasets
- [x] Build Bronze ingestion pipeline
- [x] Build Silver cleaning and quarantine pipeline
- [ ] Build Gold aggregation pipeline
- [x] Configure local Iceberg catalog
- [ ] Add SQL analytics queries
- [ ] Add a simple visualization
- [ ] Complete README and architecture documentation

### Phase 2 — Reliability and Infrastructure

Improve pipeline reliability, deployment, and query performance.

- [ ] Add recovery and checkpoint handling for partial pipeline writes
- [ ] Improve idempotency between Iceberg table writes and pipeline logs
- [ ] Review fare validation rules
- [ ] Improve Silver data-quality rules
- [ ] Add query-performance experiments
- [ ] Add a dashboard
- [ ] Add MinIO object storage
- [ ] Add Docker Compose

### Phase 3 — Multi-Service Taxi Lakehouse

Expand the lakehouse to support additional NYC TLC datasets.

- [ ] Add Green Taxi data
- [ ] Add FHV data
- [ ] Add HVFHV data
- [ ] Design a unified trip schema
- [ ] Add cross-service analytics