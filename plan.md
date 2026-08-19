# Real-Time Ride-Hailing System: Implementation Plan

> **Scope**: Focused strictly on **Driver Location Ping** and **User Ride Request** matching.
> **Tech Stack**: Apache Kafka, Apache Flink (Java 17/21+), Redis (Spatial State), Uber H3, Docker Compose.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        D_SIM["Driver Simulator\n(lat, lon, status)"] -->|every 3s| K_LOC["Kafka Topic:\ndriver-locations"]
        R_SIM["Rider Simulator\n(pickup lat, lon)"] --> K_REQ["Kafka Topic:\nride-requests"]
    end

    subgraph FlinkStream["2. Stream Processing (Apache Flink)"]
        K_LOC --> F_INGEST["Source: Kafka Consumer"]
        F_INGEST --> F_CLEAN["Filter & Validate GPS"]
        F_CLEAN --> F_H3["MapFunction: Compute H3 Cell\n(Uber H3 Res 8 ~460m)"]
        F_H3 --> F_SINK["Redis Sink:\nUpdate Spatial Index"]
    end

    subgraph SpatialState["3. In-Memory Spatial State (Redis)"]
        F_SINK --> R_CELLS["Set: cell:{h3_cell}:drivers\n(Active Drivers per Cell)"]
        F_SINK --> R_DRIVER["Hash: driver:{driver_id}\n(lat, lon, status, last_ping)"]
    end

    subgraph Dispatcher["4. Dispatch & Proximity Matcher"]
        K_REQ --> D_INGEST["Source: Ride Requests"]
        D_INGEST --> D_H3["Compute Pickup H3 Cell\n+ 6 Adjacent Neighbors (k-ring=1)"]
        D_H3 --> D_QUERY["Lookup Active Drivers in Redis"]
        R_CELLS -.-> D_QUERY
        R_DRIVER -.-> D_QUERY
        D_QUERY --> D_CALC["Sort by Haversine Distance\nPick Closest Driver"]
        D_CALC --> D_OFFER["Kafka Producer"]
    end

    subgraph Output["5. Output Layer"]
        D_OFFER --> K_MATCH["Kafka Topic:\nride-matches"]
        K_MATCH --> MATCH_VIEW["Match Consumer / Monitor"]
    end
```

---

## 2. Component Specifications & Schemas

### A. Data Models (Java POJOs / JSON)

1. **`DriverLocationPing`**
   ```json
   {
     "driverId": "driver_101",
     "latitude": 37.7749,
     "longitude": -122.4194,
     "status": "AVAILABLE",
     "bearing": 90.0,
     "timestamp": 1718000000000
   }
   ```

2. **`RideRequest`**
   ```json
   {
     "requestId": "req_501",
     "riderId": "rider_201",
     "pickupLat": 37.7752,
     "pickupLon": -122.4180,
     "timestamp": 1718000005000
   }
   ```

3. **`RideMatch`**
   ```json
   {
     "requestId": "req_501",
     "riderId": "rider_201",
     "driverId": "driver_101",
     "driverLat": 37.7749,
     "driverLon": -122.4194,
     "pickupLat": 37.7752,
     "pickupLon": -122.4180,
     "distanceMeters": 126.5,
     "status": "OFFERED",
