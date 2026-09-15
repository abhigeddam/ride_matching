# Project: Real-Time Ride-Hailing Driver-Location Ingestion & Proximity Dispatch Matching System

## Architecture
The system consists of five decoupled layers cooperating via event-driven messaging and in-memory spatial indexes:

```
[Simulated Fleet / Clients]
        │
        ├── Location Pings (every 3s) ──> Kafka Topic: driver-locations
        └── Ride Requests (pickup GPS) ─> Kafka Topic: ride-requests
                                                  │
                                                  ▼
[1. Apache Flink Streaming Pipeline]      [3. Dispatch & Proximity Matcher]
 - Consumes: driver-locations              - Consumes: ride-requests
 - Filters & validates GPS                 - Computes pickup H3 Res 8 cell
 - Maps to Uber H3 Res 8 Hexagon           - Expands to 7 cells (gridDisk k=1)
 - Redis Spatial Sink:                     - Queries active drivers in Redis
   * SADD/SREM cell:<h3_cell>:drivers      - Calculates Haversine distance
   * HSET driver:<id> with 15s TTL         - Selects closest available driver
        │                                  - Atomically reserves driver (Lua)
        ▼                                  - Publishes match record
[2. Redis In-Memory Spatial State]                │
 - cell:<h3_cell>:drivers (Set)                   ▼
 - driver:<driver_id> (Hash + 15s TTL)    [4. Output Layer]
                                           - Kafka Topic: ride-matches
                                                  │
                                                  ▼
                                          [5. Automated Verification Harness]
                                           - Non-interactive test runner
                                           - Simulates 15 drivers & requests
                                           - Verifies latency (<10ms),
                                             proximity matching & 15s TTL eviction
```

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Docker Compose Environment | Multi-container setup for Kafka KRaft and Redis | M1 | Survey R1 |
| 2 | Kafka KRaft Broker | Single-node ZooKeeper-less Kafka broker on port 9092 | M1 | Survey R1 |
| 3 | Redis Spatial Store | Redis 7 Alpine container on port 6379 | M1 | Survey R1 |
| 4 | Automated Topic Provisioning | Init mechanism creating `driver-locations`, `ride-requests`, `ride-matches` | M1 | Survey R1 |
| 5 | DriverLocationPing Model & SerDe | POJO and JSON serialization schema for driver GPS telemetry | M2 | Survey R2 |
| 6 | RideRequest Model & SerDe | POJO and JSON serialization schema for user ride requests | M2 | Survey R3 |
| 7 | RideMatch Model & SerDe | POJO and JSON serialization schema for ride matching events | M2 | Survey R3 |
| 8 | Uber H3 Resolution 8 Spatial Index | Uber H3 wrapper mapping (lat, lon) to Res 8 hex index (~461m edge) | M2 | Survey R2/R3 |
| 9 | 7-Cell Neighborhood Expansion | Expands origin cell to 7 cells (center + 6 neighbors) via `gridDisk(cell, 1)` | M2 | Survey R3 |
| 10 | Exact Haversine Distance Calculator | Spherical trigonometry calculating meter-accurate distance | M2 | Survey R3 |
| 11 | Maven Build Configuration | POM configuring Java 17 release, Flink 1.19, H3 4.4.0, Jedis 5.2.0 | M2 | Survey Explorer |
| 12 | Flink Kafka Consumer Source | Stream consumer reading `driver-locations` | M3 | Survey R2 |
| 13 | GPS Coordinate Validation Filter | Filter discarding invalid latitudes/longitudes or empty driver IDs | M3 | Survey R2 |
| 14 | Flink H3 Resolution 8 MapFunction | Transforms validated pings into `(DriverLocationPing, h3Cell)` | M3 | Survey R2 |
| 15 | Redis Spatial Cell Set Management | Manages `cell:<h3_cell>:drivers` Sets in Redis | M3 | Survey R2 |
| 16 | Redis Driver Metadata Hash Management | Manages `driver:<driver_id>` Hash `{lat, lon, status, h3_cell, last_ping}` | M3 | Survey R2 |
| 17 | Automatic 15s TTL Eviction on Driver Hash | Attaches 15s TTL to driver hash on every update | M3 | Survey R2 |
| 18 | Dynamic Cell Migration Handling | Detects cell change; executes atomic `SREM old_cell` and `SADD new_cell` | M3 | Survey R2 |
| 19 | Driver Status Transition Handling | Reflects `AVAILABLE` vs `BUSY`/`OFFLINE` status in cell sets | M3 | Survey R2 |
| 20 | Kafka RideRequest Consumer Loop | Continuous consumer polling `ride-requests` topic | M4 | Survey R3 |
| 21 | Multi-Cell Candidate Driver Fetch | Queries Redis across 7 H3 cells (`cell:<cell>:drivers`) and driver hashes | M4 | Survey R3 |
| 22 | Driver Availability & Freshness Filter | Excludes non-AVAILABLE, expired, or stale (>15s) drivers | M4 | Survey R3 |
| 23 | Stale Driver Lazy Eviction | Purges expired driver IDs from cell sets during candidate evaluation | M4 | Survey R3 |
| 24 | Closest Driver Selection & Tie-Breaking | Evaluates Haversine distance ascending; tie-breaks on last_ping then id | M4 | Survey R3 |
| 25 | Atomic Driver Reservation (Lua) | Atomically marks driver `OFFERED` in Redis, preventing race conditions | M4 | Survey R3 |
| 26 | Kafka RideMatch Producer | Publishes finalized match records to `ride-matches` topic | M4 | Survey R3 |
| 27 | Out-of-Range Graceful Handling | Gracefully logs and skips unfulfilled requests without crashing | M4 | Survey R3 |
| 28 | Non-Interactive Verification Runner | Automated test runner exiting with 0 on pass, 1 on fail | E2E Track / M5 | Survey R4 |
| 29 | Fleet Driver Movement Simulation | Simulates 10-20 drivers emitting pings every 3s in localized area | E2E Track / M5 | Survey R4 |
| 30 | Cell Boundary Crossing Verification | Asserts set membership migration when driver crosses H3 cell boundary | E2E Track / M5 | Survey R4 |
| 31 | Ingestion Latency Measurement | Asserts Flink-to-Redis ingestion latency satisfies <10ms SLA | E2E Track / M5 | Survey R4 |
| 32 | Proximity Match Correctness Assertion | Asserts mathematically closest driver chosen and distance is accurate | E2E Track / M5 | Survey R4 |
| 33 | Stale Driver TTL Eviction Assertion | Asserts 15s TTL expires and stale driver is excluded from matching | E2E Track / M5 | Survey R4 |
| 34 | Structured Pass/Fail Output | Generates console summary and machine-parseable JSON test results | E2E Track / M5 | Survey R4 |
| 35 | Adversarial Coverage Hardening | Tier 5 white-box stress testing and boundary validation | M5 Phase 2 | Project Pattern |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Local Infrastructure Orchestration | Docker Compose with Kafka KRaft, Redis 7, and automated topic provisioning | None | PLANNED |
| M2 | Core Domain Models, Spatial Index & Build | Maven POM, Java POJOs, H3 v4.4.0 wrapper, Haversine calculator, unit tests | None | PLANNED |
| M3 | Real-Time Spatial Ingestion Pipeline | Apache Flink streaming job, GPS filter, H3 mapping, atomic Redis spatial sink | M1, M2 | PLANNED |
| M4 | Dispatch & Proximity Matcher Service | Kafka ride-requests consumer, 7-cell lookup, Haversine ranker, Lua reservation, ride-matches producer | M1, M2, M3 | PLANNED |
| M5 | Final Milestone: E2E Verification & Hardening | Phase 1: 100% pass of E2E test suite (Tiers 1-4); Phase 2: Adversarial hardening (Tier 5) | M1, M2, M3, M4, E2E Track | PLANNED |
| E2E | E2E Testing Track Orchestration | Requirement-driven opaque-box test suite (Tiers 1-4), simulation runner, `TEST_READY.md` | M1, M2 | PLANNED |

## Interface Contracts

### 1. Kafka Topic Contracts
- `driver-locations`: Key: String (driverId) or null; Value: JSON formatted `DriverLocationPing`.
- `ride-requests`: Key: String (requestId) or null; Value: JSON formatted `RideRequest`.
- `ride-matches`: Key: String (requestId); Value: JSON formatted `RideMatch`.

### 2. Redis Key Layout Contracts
- `cell:<h3_res8_hex>:drivers`: Redis Set containing active `driverId` strings.
- `driver:<driverId>`: Redis Hash containing fields:
  - `lat`: String representing double latitude (e.g. "37.7749")
  - `lon`: String representing double longitude (e.g. "-122.4194")
  - `status`: String ("AVAILABLE", "OFFERED", "BUSY", "OFFLINE")
  - `h3_cell`: String (15-char H3 Res 8 hex address)
  - `last_ping`: String representing millisecond epoch timestamp
  - TTL: 15 seconds set via `EXPIRE driver:<driverId> 15`

### 3. Spatial Calculation Contracts
- Uber H3 Resolution: 8. Edge length ~461m.
- Pickup neighborhood: `h3.gridDisk(pickupCell, 1)` yielding exactly 7 cells (center + 6 neighbors).
- Haversine distance formula using $R = 6,371,000.0\text{m}$.

## Code Layout
```
ride-hailing-system/
├── docker-compose.yml
├── pom.xml
├── README.md
├── PROJECT.md
├── TEST_INFRA.md
├── TEST_READY.md (published by E2E track)
├── src/
│   ├── main/
│   │   ├── java/com/ridehailing/
│   │   │   ├── model/
│   │   │   │   ├── DriverLocationPing.java
│   │   │   │   ├── RideRequest.java
│   │   │   │   └── RideMatch.java
│   │   │   ├── spatial/
│   │   │   │   ├── H3SpatialIndex.java
│   │   │   │   └── HaversineDistance.java
│   │   │   ├── flink/
│   │   │   │   ├── DriverLocationStreamJob.java
│   │   │   │   ├── GpsValidationFilter.java
│   │   │   │   └── RedisSpatialSink.java
│   │   │   ├── dispatch/
│   │   │   │   ├── RideMatchingService.java
│   │   │   │   └── CandidateFinder.java
│   │   │   └── util/
│   │   │       ├── JsonSerde.java
│   │   │       └── RedisPoolManager.java
│   │   └── resources/
│   │       ├── application.properties
│   │       └── simplelogger.properties
│   └── test/
│       └── java/com/ridehailing/
│           ├── H3SpatialIndexTest.java
│           ├── HaversineDistanceTest.java
│           └── DispatchLogicTest.java
└── simulation/
    ├── simulate_and_verify.py
    └── verify.sh
```
