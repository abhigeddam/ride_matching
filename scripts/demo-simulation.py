#!/usr/bin/env python3
"""
Interactive Learning Demo: Real-Time Ride-Hailing Simulation
Simulates moving drivers in San Francisco, publishes pings to Kafka,
monitors Redis H3 spatial indexing, requests a ride, and displays proximity matching.
"""

import json
import math
import time
import sys
import redis
from kafka import KafkaProducer, KafkaConsumer
import h3

BOOTSTRAP_SERVERS = "localhost:9092"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

TOPIC_LOCATIONS = "driver-locations"
TOPIC_REQUESTS = "ride-requests"
TOPIC_MATCHES = "ride-matches"

# 5 Drivers moving in San Francisco
DRIVERS = [
    {"id": "driver_sf_1", "name": "Marcus (Tesla Model 3)", "lat": 37.7879, "lon": -122.4074, "bearing": 90.0, "status": "AVAILABLE"},  # Union Square
    {"id": "driver_sf_2", "name": "Elena (Toyota Prius)",   "lat": 37.7915, "lon": -122.3990, "bearing": 180.0, "status": "AVAILABLE"}, # Financial District
    {"id": "driver_sf_3", "name": "David (Hyundai Ioniq)",  "lat": 37.7812, "lon": -122.4032, "bearing": 270.0, "status": "AVAILABLE"}, # SoMa
    {"id": "driver_sf_4", "name": "Sofia (Honda Civic)",    "lat": 37.7600, "lon": -122.4200, "bearing": 45.0,  "status": "AVAILABLE"}, # Mission (farther)
    {"id": "driver_sf_5", "name": "Lucas (Ford Mustang)",   "lat": 37.7795, "lon": -122.4180, "bearing": 0.0,   "status": "AVAILABLE"}  # Civic Center
]

# Rider requesting a pickup near Powell St Station
RIDER = {
    "requestId": f"req_{int(time.time() * 1000) % 100000}",
    "riderId": "rider_sarah",
    "pickupLat": 37.7844,
    "pickupLon": -122.4079,
    "timestamp": int(time.time() * 1000)
}

def print_header(title):
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)

def main():
    print_header("REAL-TIME RIDE-HAILING INTERACTIVE SIMULATION")
    print("Connecting to Kafka (localhost:9092) and Redis (localhost:6379)...")

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        r.ping()
        print(" Connected to Redis successfully.")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        sys.exit(1)

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print(" Connected to Kafka broker successfully.")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        sys.exit(1)

    # 1. Emit Driver Location Pings
    print_header("STEP 1: EMITTING DRIVER LOCATION PINGS")
    print(f"Publishing {len(DRIVERS)} driver GPS telemetry pings to topic '{TOPIC_LOCATIONS}'...\n")

    for d in DRIVERS:
        # Compute expected Uber H3 cell at resolution 8
        cell = h3.latlng_to_cell(d["lat"], d["lon"], 8)
        payload = {
            "driverId": d["id"],
            "latitude": d["lat"],
            "longitude": d["lon"],
            "status": d["status"],
            "bearing": d["bearing"],
            "timestamp": int(time.time() * 1000)
        }
        producer.send(TOPIC_LOCATIONS, value=payload)
        print(f"  🚗 [{d['id']}] {d['name']:25} -> ({d['lat']:.4f}, {d['lon']:.4f}) | H3 Hex: {cell}")

    producer.flush()
    print("\nWaiting 2 seconds for Apache Flink to process stream and populate Redis...")
    time.sleep(2)

    # 2. Inspect Redis Spatial State
    print_header("STEP 2: INSPECTING IN-MEMORY REDIS SPATIAL STATE")
    rider_cell = h3.latlng_to_cell(RIDER["pickupLat"], RIDER["pickupLon"], 8)
    neighbors = list(h3.grid_disk(rider_cell, 1))
    print(f"Rider Pickup H3 Cell (Res 8): {rider_cell}")
    print(f"Proximity Search Ring (k=1):   {len(neighbors)} hexagonal cells (~460m edge length each)\n")

    found_drivers_count = 0
    for cell in neighbors:
        cell_key = f"cell:{cell}:drivers"
        driver_ids = r.smembers(cell_key)
        is_origin = " (Rider Cell)" if cell == rider_cell else ""
        if driver_ids:
            print(f"  📍 Hex {cell}{is_origin}: {len(driver_ids)} driver(s) -> {list(driver_ids)}")
            for did in driver_ids:
                h = r.hgetall(f"driver:{did}")
                ttl = r.ttl(f"driver:{did}")
                print(f"     └─ {did}: status={h.get('status')}, last_lat={h.get('latitude')}, TTL={ttl}s")
                found_drivers_count += 1
        else:
            print(f"  ▫️ Hex {cell}{is_origin}: empty")

    # 3. Submit Ride Request
    print_header("STEP 3: SUBMITTING RIDER REQUEST")
    print(f"Rider '{RIDER['riderId']}' requesting ride at Market & Powell St:")
    print(f"  Pickup Location: ({RIDER['pickupLat']}, {RIDER['pickupLon']})")
    print(f"  Sending to Kafka topic: '{TOPIC_REQUESTS}'...")

    consumer = KafkaConsumer(
        TOPIC_MATCHES,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=5000
    )

    t0 = time.time()
    producer.send(TOPIC_REQUESTS, value=RIDER)
    producer.flush()

    # 4. Await Proximity Match
    print_header("STEP 4: AWAITING DISPATCH PROXIMITY MATCH FROM KAFKA")
    print(f"Listening on '{TOPIC_MATCHES}' (timeout: 5 seconds)...")

    matched = False
    for msg in consumer:
        match = msg.value
        if match.get("requestId") == RIDER["requestId"]:
            elapsed_ms = (time.time() - t0) * 1000
            matched = True
            print("\n  🎯 MATCH FOUND!")
            print(f"  -------------------------------------------------------------")
            print(f"  Request ID:       {match.get('requestId')}")
            print(f"  Rider ID:         {match.get('riderId')}")
            print(f"  Matched Driver:   {match.get('driverId')}")
            print(f"  Driver Location:  ({match.get('driverLat')}, {match.get('driverLon')})")
            print(f"  Pickup Location:  ({match.get('pickupLat')}, {match.get('pickupLon')})")
            print(f"  Distance:         {match.get('distanceMeters'):.1f} meters")
            print(f"  Match Status:     {match.get('status')}")
            print(f"  Dispatch Latency: {elapsed_ms:.1f} ms")
            print(f"  -------------------------------------------------------------")
            break

    if not matched:
        print("\n  ⚠️ No match received within 5s.")
        print("  Make sure the Dispatch Matcher is running (`./scripts/run-matcher.sh`).")

    consumer.close()
    producer.close()
    print_header("DEMO COMPLETE")

if __name__ == "__main__":
    main()
