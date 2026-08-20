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
