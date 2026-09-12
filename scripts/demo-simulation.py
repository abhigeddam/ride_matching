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
