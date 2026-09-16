# Real-Time Ride-Hailing Driver Ingestion & Proximity Dispatch System

A lightweight, production-modeled streaming architecture demonstrating real-time driver GPS ingestion, Uber H3 spatial indexing, in-memory Redis caching, and proximity dispatch matching with Apache Kafka and Apache Flink.

---

## 1. System Architecture

```
[ Driver Simulators ] ---> Kafka Topic: driver-locations
                                    │
                                    ▼
                     [ Apache Flink Streaming Pipeline ]
                         - Validate GPS bounds
                         - Compute Uber H3 Hex (Res 8: ~460m)
                         - Sink to Redis
                                    │
                                    ▼
                         [ Redis In-Memory State ]
                         - Set:  cell:<h3_hex>:drivers
                         - Hash: driver:<id> (lat, lon, status, TTL: 15s)
                                    ▲
                                    │ (Query 7-cell k-ring=1)
[ Rider Request ] ----> Kafka Topic: ride-requests
                                    │
                                    ▼
                      [ Proximity Matching Service ]
                         - Compute pickup H3 cell + 6 neighbors
                         - Filter AVAILABLE drivers
                         - Calculate Haversine distance
                         - Select closest driver & reserve
                                    │
                                    ▼
                        Kafka Topic: ride-matches
```

---

## 2. Key Engineering Concepts

1. **Uber H3 Spatial Indexing (Resolution 8)**:
   - Divides the globe into hexagonal cells. At Resolution 8, each hexagon has an edge length of $\approx 460\text{ meters}$ and an area of $\approx 0.737\text{ km}^2$.
   - Hexagons provide invariant distance to all 6 adjacent neighbors (unlike squares which have diagonal distortion).
   - In code: [`H3SpatialIndex.java`](src/main/java/com/ridehailing/spatial/H3SpatialIndex.java) wraps `com.uber.h3core.H3Core` (`latLngToCellAddress` and `gridDisk(cell, 1)`).

2. **Stream Ingestion & State Maintenance (Apache Flink)**:
   - [`DriverLocationStreamJob.java`](src/main/java/com/ridehailing/flink/DriverLocationStreamJob.java) consumes Kafka topic `driver-locations`.
   - Filters invalid coordinates ([`GpsValidationFilter.java`](src/main/java/com/ridehailing/flink/GpsValidationFilter.java)).
   - Updates Redis sets and hashes ([`RedisSpatialSink.java`](src/main/java/com/ridehailing/flink/RedisSpatialSink.java)):
     - When a driver transitions between hexagons, it executes `SREM cell:<old>:drivers` and `SADD cell:<new>:drivers`.
     - Sets a 15-second TTL (`EXPIRE driver:<id> 15`) so offline/stale drivers automatically vanish.

3. **Proximity Dispatch Matcher**:
   - [`RideMatchingService.java`](src/main/java/com/ridehailing/dispatch/RideMatchingService.java) consumes Kafka topic `ride-requests`.
   - Looks up active drivers across the pickup cell and all 6 neighbor cells (7 cells total $\approx 5\text{ km}^2$).
   - Calculates the exact Haversine distance in meters ([`DistanceCalculator.java`](src/main/java/com/ridehailing/spatial/DistanceCalculator.java)).
   - Dispatches the closest available driver, updates their status to `OFFERED`, and emits the match to `ride-matches`.

---

