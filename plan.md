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
