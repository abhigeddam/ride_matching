# Original User Request

## Initial Request — 2026-09-15T15:50:07Z

Build a fully runnable local real-time ride-hailing driver-location ingestion and proximity dispatch matching system based on /Users/abhiramtarungeddam/ride-hailing-system/plan.md.

Working directory: /Users/abhiramtarungeddam/ride-hailing-system
Integrity mode: development

Reference: plan.md in the working directory defines the core architectural components, schemas (DriverLocationPing, RideRequest, RideMatch), and Redis spatial key layout.

## Requirements

### R1. Local Infrastructure Orchestration
Provide a Docker Compose environment running Apache Kafka (KRaft mode) and Redis. The setup must ensure topics (driver-locations, ride-requests, and ride-matches) are automatically initialized upon startup.

### R2. Real-Time Spatial Ingestion Pipeline
Implement an Apache Flink streaming pipeline in Java that consumes DriverLocationPing records from Kafka, computes Uber H3 spatial indices (resolution 8, ~460m edge length), and maintains active driver sets (cell:<h3_cell>:drivers) and driver metadata hashes (driver:<driver_id>) in Redis with automatic eviction via TTL.

### R3. Dispatch & Proximity Matcher
Implement a dispatch service that consumes RideRequest records from Kafka, queries Redis across the pickup H3 cell and its 6 immediate neighbors (k-ring=1), selects the closest available driver based on Haversine distance, and publishes RideMatch records to Kafka.

### R4. Automated Verification & Simulation Harness
Provide an automated test and simulation script that runs non-interactively to generate simulated driver movements and ride requests, programmatically validating end-to-end match generation, latency, and stale driver eviction.

## Acceptance Criteria

### Service Health & Build
- [ ] `docker compose up -d` starts Kafka and Redis, with all required topics (driver-locations, ride-requests, ride-matches) created and verified.
- [ ] The Java Flink streaming job and dispatch service build cleanly using standard build tools (Maven or Gradle) with zero compilation errors.

### Streaming & Spatial Indexing
- [ ] Emitting DriverLocationPing events updates Redis: drivers are added to cell:<h3_cell>:drivers sets and driver:<driver_id> hashes.
- [ ] When a driver moves to a new H3 cell, the driver is removed from the previous cell's set and added to the new one.
- [ ] Ceasing pings for a driver causes the driver to expire and be evicted from active availability via Redis TTL.

### Dispatch Matching
- [ ] When a RideRequest is submitted, candidate drivers are queried across the pickup H3 cell and all 6 neighbor cells (7 cells total).
- [ ] The system accurately calculates Haversine distance and outputs a RideMatch event to Kafka for the closest available driver.
- [ ] Offline or expired drivers are not matched.

### End-to-End Verification
- [ ] An automated verification script executes from start to finish, outputting structured pass/fail results for ingestion, Redis state updates, proximity matching, and stale driver expiration.
