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

## 3. Project Structure

```
ride-hailing-system/
├── docker-compose.yml              # Single-node Kafka 3.8 (KRaft) & Redis 7
├── pom.xml                         # Maven build with Flink, H3, Jedis, Kafka
├── scripts/
│   ├── start-infra.sh              # Spins up Kafka & Redis in Docker Compose
│   ├── run-flink-job.sh            # Runs the Flink driver location streaming job
│   ├── run-matcher.sh              # Runs the Ride Matching dispatch service
│   ├── run-demo.sh                 # Executes the interactive SF simulation
│   └── demo-simulation.py          # Python simulation script with visual logs
└── src/
    ├── main/java/com/ridehailing/
    │   ├── model/
    │   │   ├── DriverLocationPing.java
    │   │   ├── RideRequest.java
    │   │   └── RideMatch.java
    │   ├── spatial/
    │   │   ├── H3SpatialIndex.java      # Uber H3 Hexagon calculations
    │   │   └── DistanceCalculator.java  # Haversine distance formula
    │   ├── flink/
    │   │   ├── DriverLocationStreamJob.java
    │   │   ├── DriverLocationDeserializationSchema.java
    │   │   ├── GpsValidationFilter.java
    │   │   └── RedisSpatialSink.java
    │   ├── dispatch/
    │   │   ├── RideMatchingService.java
    │   │   └── CandidateFinder.java
    │   └── util/
    │       ├── JsonUtil.java
    │       └── RedisPoolManager.java
    └── test/java/com/ridehailing/
        └── spatial/
            ├── H3SpatialIndexTest.java
            └── DistanceCalculatorTest.java
```

---

## 4. Quick Start: Running the System

### Step 1: Start Infrastructure (Kafka & Redis)
```bash
./scripts/start-infra.sh
```
*Starts Kafka (KRaft mode, port 9092) and Redis (port 6379) in Docker Compose, and automatically provisions topics `driver-locations`, `ride-requests`, and `ride-matches`.*

### Step 2: Build the Java Components
```bash
mvn clean package -DskipTests
```
*Builds the shaded executable jar at `target/ride-hailing-system-1.0.0.jar`.*

### Step 3: Run the Apache Flink Streaming Job (Terminal 1)
```bash
./scripts/run-flink-job.sh
```
*Listens to `driver-locations`, calculates H3 cell resolution 8, and updates Redis spatial indexes in real time.*

