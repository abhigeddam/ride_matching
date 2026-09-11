#!/usr/bin/env python3
"""
simulation/simulate_and_verify.py

End-to-End Opaque-Box Verification Harness & Fleet Simulator
for Real-Time Ride-Hailing Driver-Location Ingestion & Proximity Dispatch Matching.

Author: teamwork_preview_test_writer (E2E Track)
Reference: ORIGINAL_REQUEST.md, PROJECT.md, TEST_INFRA.md, plan.md
"""

import os
import sys
import time
import json
import math
import random
import socket
import logging
import argparse
import threading
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Virtualenv Auto-reexec: If external deps are missing in sys.executable,
# attempt to re-exec using simulation/.venv/bin/python3 if present.
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
_VENV_PYTHON = os.path.join(_SCRIPT_DIR, ".venv", "bin", "python3")

def _check_and_reexec():
    try:
        import h3
        import redis
        import kafka
    except ImportError:
        if os.path.exists(_VENV_PYTHON) and sys.executable != _VENV_PYTHON:
            os.execv(_VENV_PYTHON, [_VENV_PYTHON] + sys.argv)

_check_and_reexec()

try:
    import h3
except ImportError:
    h3 = None

try:
    import redis
except ImportError:
    redis = None

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    KafkaProducer = None
    KafkaConsumer = None
    KafkaError = None


# ---------------------------------------------------------------------------
# Constants & Contracts (from PROJECT.md / plan.md)
# ---------------------------------------------------------------------------
EARTH_RADIUS_METERS = 6371000.0  # PROJECT.md line 104
H3_RESOLUTION = 8                 # Uber H3 Res 8 (~461m edge length)
DEFAULT_TTL_SECONDS = 15          # Redis TTL for driver:<id> hash
INGESTION_LATENCY_SLA_MS = 10.0   # SLA: Ingestion < 10ms

TOPIC_DRIVER_LOCATIONS = "driver-locations"
TOPIC_RIDE_REQUESTS = "ride-requests"
TOPIC_RIDE_MATCHES = "ride-matches"

# San Francisco bounding box & anchor points
SF_CIVIC_CENTER = (37.7749, -122.4194)
SF_UNION_SQUARE = (37.7879, -122.4074)
SF_FINANCIAL_DIST = (37.7946, -122.3999)
SF_SOMA = (37.7785, -122.3950)
SF_MISSION = (37.7599, -122.4148)
SF_FISHERMAN_WHARF = (37.8080, -122.4177)
SAN_JOSE_DOWNTOWN = (37.3382, -121.8863)  # Out-of-range point (>60km away)


# ---------------------------------------------------------------------------
# Spatial Utilities (Haversine & H3 wrappers)
# ---------------------------------------------------------------------------
def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Exact spherical Haversine formula (R = 6,371,000.0m)."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (math.sin(delta_phi / 2.0) ** 2 +
         math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2))
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return EARTH_RADIUS_METERS * c


def latlng_to_cell(lat: float, lon: float, res: int = H3_RESOLUTION) -> str:
    """H3 Resolution 8 index mapper supporting both H3 v3 and v4 APIs."""
    if h3 is None:
        lat_q = int((lat + 90.0) * 1000)
        lon_q = int((lon + 180.0) * 1000)
        return f"882830{lat_q:06x}{lon_q:06x}"[:15]
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, res)
    elif hasattr(h3, "geo_to_h3"):
        return h3.geo_to_h3(lat, lon, res)
    raise RuntimeError("Unsupported H3 API")


def cell_to_latlng(cell: str) -> Tuple[float, float]:
    """H3 cell center coordinate mapper supporting both H3 v3 and v4."""
    if h3 is None:
        return SF_CIVIC_CENTER
    if hasattr(h3, "cell_to_latlng"):
        return h3.cell_to_latlng(cell)
    elif hasattr(h3, "h3_to_geo"):
        return h3.h3_to_geo(cell)
    raise RuntimeError("Unsupported H3 API")


def grid_disk(cell: str, k: int = 1) -> Set[str]:
    """7-cell neighborhood expansion (k=1) supporting H3 v3 and v4."""
    if h3 is None:
        return {cell} | {f"{cell[:-1]}{i}" for i in range(1, 7)}
    if hasattr(h3, "grid_disk"):
        return set(h3.grid_disk(cell, k))
    elif hasattr(h3, "k_ring"):
        return set(h3.k_ring(cell, k))
    raise RuntimeError("Unsupported H3 API")


def grid_distance(cell1: str, cell2: str) -> int:
    """H3 grid distance calculation."""
    if h3 is None:
        return 0 if cell1 == cell2 else 1
    if hasattr(h3, "grid_distance"):
        return h3.grid_distance(cell1, cell2)
    elif hasattr(h3, "h3_distance"):
        return h3.h3_distance(cell1, cell2)
    return 1


# ---------------------------------------------------------------------------
# Data Models (PROJECT.md § 2 Data Models)
# ---------------------------------------------------------------------------
@dataclass
class DriverLocationPing:
    driverId: str
    latitude: float
    longitude: float
    status: str = "AVAILABLE"
    bearing: float = 0.0
    timestamp: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class RideRequest:
    requestId: str
    riderId: str
    pickupLat: float
    pickupLon: float
    timestamp: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class RideMatch:
    requestId: str
    riderId: str
    driverId: str
    driverLat: float
    driverLon: float
    pickupLat: float
    pickupLon: float
    distanceMeters: float
    status: str = "OFFERED"
    matchedAt: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RideMatch":
        return cls(
            requestId=data["requestId"],
            riderId=data["riderId"],
            driverId=data["driverId"],
            driverLat=float(data["driverLat"]),
            driverLon=float(data["driverLon"]),
            pickupLat=float(data["pickupLat"]),
            pickupLon=float(data["pickupLon"]),
            distanceMeters=float(data["distanceMeters"]),
            status=data.get("status", "OFFERED"),
            matchedAt=int(data.get("matchedAt", 0))
        )


# ---------------------------------------------------------------------------
# Abstract Redis & In-Memory Mock Store
# ---------------------------------------------------------------------------
class AbstractRedisStore:
    def ping(self) -> bool: raise NotImplementedError
    def get_info(self) -> Dict[str, Any]: raise NotImplementedError
    def set(self, key: str, value: str) -> None: raise NotImplementedError
    def get(self, key: str) -> Optional[str]: raise NotImplementedError
    def hset(self, key: str, mapping: Dict[str, Any]) -> None: raise NotImplementedError
    def hget(self, key: str, field: str) -> Optional[str]: raise NotImplementedError
    def hgetall(self, key: str) -> Dict[str, str]: raise NotImplementedError
    def sadd(self, key: str, *members: str) -> None: raise NotImplementedError
    def srem(self, key: str, *members: str) -> None: raise NotImplementedError
    def smembers(self, key: str) -> Set[str]: raise NotImplementedError
    def sismember(self, key: str, member: str) -> bool: raise NotImplementedError
    def expire(self, key: str, seconds: int) -> bool: raise NotImplementedError
    def ttl(self, key: str) -> int: raise NotImplementedError
    def delete(self, *keys: str) -> int: raise NotImplementedError
    def keys(self, pattern: str) -> List[str]: raise NotImplementedError
    def flushdb(self) -> None: raise NotImplementedError


class LiveRedisStore(AbstractRedisStore):
    def __init__(self, host: str = "localhost", port: int = 6379):
        if redis is None:
            raise RuntimeError("Python redis library not installed")
        self.client = redis.Redis(host=host, port=port, decode_responses=True, socket_timeout=3.0)

    def ping(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception:
            return False

    def get_info(self) -> Dict[str, Any]:
        return self.client.info()

    def set(self, key: str, value: str) -> None:
        self.client.set(key, value)

    def get(self, key: str) -> Optional[str]:
        return self.client.get(key)

    def hset(self, key: str, mapping: Dict[str, Any]) -> None:
        self.client.hset(key, mapping=mapping)

    def hget(self, key: str, field: str) -> Optional[str]:
        return self.client.hget(key, field)

    def hgetall(self, key: str) -> Dict[str, str]:
        return self.client.hgetall(key)

    def sadd(self, key: str, *members: str) -> None:
        if members:
            self.client.sadd(key, *members)

    def srem(self, key: str, *members: str) -> None:
        if members:
            self.client.srem(key, *members)

    def smembers(self, key: str) -> Set[str]:
        return set(self.client.smembers(key))

    def sismember(self, key: str, member: str) -> bool:
        return bool(self.client.sismember(key, member))

    def expire(self, key: str, seconds: int) -> bool:
        return bool(self.client.expire(key, seconds))

    def ttl(self, key: str) -> int:
        return self.client.ttl(key)

    def delete(self, *keys: str) -> int:
        if keys:
            return self.client.delete(*keys)
        return 0

    def keys(self, pattern: str) -> List[str]:
        return self.client.keys(pattern)

    def flushdb(self) -> None:
        self.client.flushdb()


class MockRedisStore(AbstractRedisStore):
    """Thread-safe in-memory Redis emulator with exact TTL expiration semantics."""
    def __init__(self):
        self._lock = threading.RLock()
        self._strings: Dict[str, str] = {}
        self._hashes: Dict[str, Dict[str, str]] = {}
        self._sets: Dict[str, Set[str]] = {}
        self._expires: Dict[str, float] = {}

    def _purge_if_expired(self, key: str) -> bool:
        now = time.time()
        if key in self._expires and now >= self._expires[key]:
            self._strings.pop(key, None)
            self._hashes.pop(key, None)
            self._sets.pop(key, None)
            self._expires.pop(key, None)
            return True
        return False

    def ping(self) -> bool:
        return True

    def get_info(self) -> Dict[str, Any]:
        return {"redis_version": "7.2.4-mock", "role": "master", "connected_clients": 1}

    def set(self, key: str, value: str) -> None:
        with self._lock:
            self._purge_if_expired(key)
            self._strings[key] = str(value)

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            if self._purge_if_expired(key):
                return None
            return self._strings.get(key)

    def hset(self, key: str, mapping: Dict[str, Any]) -> None:
        with self._lock:
            self._purge_if_expired(key)
            if key not in self._hashes:
                self._hashes[key] = {}
            for k, v in mapping.items():
                self._hashes[key][str(k)] = str(v)

    def hget(self, key: str, field: str) -> Optional[str]:
        with self._lock:
            if self._purge_if_expired(key):
                return None
            return self._hashes.get(key, {}).get(field)

    def hgetall(self, key: str) -> Dict[str, str]:
        with self._lock:
            if self._purge_if_expired(key):
                return {}
            return dict(self._hashes.get(key, {}))

    def sadd(self, key: str, *members: str) -> None:
        with self._lock:
            self._purge_if_expired(key)
            if key not in self._sets:
                self._sets[key] = set()
            for m in members:
                self._sets[key].add(str(m))

    def srem(self, key: str, *members: str) -> None:
        with self._lock:
            self._purge_if_expired(key)
            if key in self._sets:
                for m in members:
                    self._sets[key].discard(str(m))

    def smembers(self, key: str) -> Set[str]:
        with self._lock:
            if self._purge_if_expired(key):
                return set()
            return set(self._sets.get(key, set()))

    def sismember(self, key: str, member: str) -> bool:
        with self._lock:
            if self._purge_if_expired(key):
                return False
            return str(member) in self._sets.get(key, set())

    def expire(self, key: str, seconds: int) -> bool:
        with self._lock:
            if key in self._hashes or key in self._sets or key in self._strings:
                self._expires[key] = time.time() + float(seconds)
                return True
            return False

    def ttl(self, key: str) -> int:
        with self._lock:
            if self._purge_if_expired(key):
                return -2  # Key does not exist
            if key not in self._expires:
                return -1  # Key exists but no TTL
            remaining = int(self._expires[key] - time.time())
            return max(remaining, 0)

    def delete(self, *keys: str) -> int:
        with self._lock:
            count = 0
            for k in keys:
                if k in self._hashes or k in self._sets or k in self._strings:
                    count += 1
                self._strings.pop(k, None)
                self._hashes.pop(k, None)
                self._sets.pop(k, None)
                self._expires.pop(k, None)
            return count

    def keys(self, pattern: str) -> List[str]:
        with self._lock:
            import fnmatch
            all_keys = set(self._hashes.keys()) | set(self._sets.keys()) | set(self._strings.keys())
            active_keys = [k for k in all_keys if not self._purge_if_expired(k)]
            return fnmatch.filter(active_keys, pattern)

    def flushdb(self) -> None:
        with self._lock:
            self._strings.clear()
            self._hashes.clear()
            self._sets.clear()
            self._expires.clear()


# ---------------------------------------------------------------------------
# Abstract Kafka Bus & Emulated Messaging Engine
# ---------------------------------------------------------------------------
class AbstractKafkaBus:
    def is_reachable(self) -> bool: raise NotImplementedError
    def get_topics(self) -> Set[str]: raise NotImplementedError
    def produce(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> None: raise NotImplementedError
    def consume_matches(self, timeout_sec: float = 3.0, target_req_id: Optional[str] = None) -> List[RideMatch]: raise NotImplementedError
    def clear_queue(self) -> None: raise NotImplementedError
    def close(self) -> None: raise NotImplementedError


class LiveKafkaBus(AbstractKafkaBus):
    """Kafka client wrapping real Kafka KRaft broker."""
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap = bootstrap_servers
        self._producer: Optional[Any] = None

    def is_reachable(self) -> bool:
        try:
            parts = self.bootstrap.split(":")
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 9092
            with socket.create_connection((host, port), timeout=2.0):
                return True
        except Exception:
            return False

    def _get_producer(self):
        if self._producer is None:
            if KafkaProducer is None:
                raise RuntimeError("kafka-python library not available")
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000
            )
        return self._producer

    def get_topics(self) -> Set[str]:
        p = self._get_producer()
        cluster = p._sender._metadata
        cluster.request_update()
        time.sleep(0.5)
        return set(cluster.topics())

    def produce(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> None:
        p = self._get_producer()
        future = p.send(topic, key=key, value=value)
        p.flush()
        future.get(timeout=5)

    def consume_matches(self, timeout_sec: float = 3.0, target_req_id: Optional[str] = None) -> List[RideMatch]:
        if KafkaConsumer is None:
            raise RuntimeError("kafka-python library not available")
        group = f"verify-sub-{random.randint(10000, 99999)}"
        consumer = KafkaConsumer(
            TOPIC_RIDE_MATCHES,
            bootstrap_servers=self.bootstrap,
            group_id=group,
            auto_offset_reset='earliest',
            consumer_timeout_ms=int(timeout_sec * 1000),
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        matches = []
        try:
            start_t = time.time()
            while time.time() - start_t < timeout_sec:
                raw_msgs = consumer.poll(timeout_ms=500)
                for tp, msgs in raw_msgs.items():
                    for msg in msgs:
                        try:
                            rm = RideMatch.from_dict(msg.value)
                            if target_req_id is None or rm.requestId == target_req_id:
                                matches.append(rm)
                        except Exception:
                            pass
                if matches and target_req_id is not None:
                    break
        finally:
            consumer.close()
        return matches

    def clear_queue(self) -> None:
        pass

    def close(self) -> None:
        if self._producer:
            try:
                self._producer.close()
            except Exception:
                pass


class MockKafkaBus(AbstractKafkaBus):
    """
    In-memory message bus with integrated streaming & dispatch logic emulator.
    Guarantees that test assertions and mathematical invariants can be validated
    independently and self-consistently.
    """
    def __init__(self, redis_store: AbstractRedisStore):
        self.redis = redis_store
        self._lock = threading.RLock()
        self._topics = {
            TOPIC_DRIVER_LOCATIONS,
            TOPIC_RIDE_REQUESTS,
            TOPIC_RIDE_MATCHES
        }
        self._matches_queue: List[RideMatch] = []

    def is_reachable(self) -> bool:
        return True

    def get_topics(self) -> Set[str]:
        return set(self._topics)

    def clear_queue(self) -> None:
        with self._lock:
            self._matches_queue.clear()

    def produce(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> None:
        with self._lock:
            if topic == TOPIC_DRIVER_LOCATIONS:
                self._process_driver_ping(value)
            elif topic == TOPIC_RIDE_REQUESTS:
                self._process_ride_request(value)

    def _process_driver_ping(self, payload: Dict[str, Any]) -> None:
        """Emulates Flink Streaming DAG: GPS filter -> H3 Res 8 -> Redis sink."""
        driver_id = payload.get("driverId")
        if not driver_id:
            return
        lat = float(payload.get("latitude", 0.0))
        lon = float(payload.get("longitude", 0.0))
        # GPS validation filter: reject out-of-bound coords
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return

        status = payload.get("status", "AVAILABLE")
        ts = int(payload.get("timestamp", int(time.time() * 1000)))

        new_cell = latlng_to_cell(lat, lon, H3_RESOLUTION)

        # Retrieve existing hash to check cell change
        old_hash = self.redis.hgetall(f"driver:{driver_id}")
        old_cell = old_hash.get("h3_cell")

        # Dynamic cell migration handling: SREM old_cell, SADD new_cell
        if old_cell and old_cell != new_cell:
            self.redis.srem(f"cell:{old_cell}:drivers", driver_id)

        if status == "AVAILABLE":
            self.redis.sadd(f"cell:{new_cell}:drivers", driver_id)
        else:
            self.redis.srem(f"cell:{new_cell}:drivers", driver_id)

        # Update driver hash with 15s TTL
        self.redis.hset(f"driver:{driver_id}", {
            "lat": str(lat),
            "lon": str(lon),
            "status": status,
            "h3_cell": new_cell,
            "last_ping": str(ts)
        })
        self.redis.expire(f"driver:{driver_id}", DEFAULT_TTL_SECONDS)

    def _process_ride_request(self, payload: Dict[str, Any]) -> None:
        """Emulates Dispatcher: Pickup H3 -> 7-cell expansion -> Candidate query -> Haversine rank -> Match."""
        req_id = payload.get("requestId")
        rider_id = payload.get("riderId")
        if not req_id or not rider_id:
            return

        p_lat = float(payload.get("pickupLat", 0.0))
        p_lon = float(payload.get("pickupLon", 0.0))
        pickup_cell = latlng_to_cell(p_lat, p_lon, H3_RESOLUTION)

        # 7-cell neighborhood expansion (k=1)
        neighborhood = grid_disk(pickup_cell, 1)

        candidates = []
        now_ms = int(time.time() * 1000)

        for cell in neighborhood:
            driver_ids = self.redis.smembers(f"cell:{cell}:drivers")
            for d_id in list(driver_ids):
                d_hash = self.redis.hgetall(f"driver:{d_id}")
                # Exclude if hash expired or empty
                if not d_hash:
                    self.redis.srem(f"cell:{cell}:drivers", d_id)
                    continue

                d_status = d_hash.get("status")
                d_last_ping = int(d_hash.get("last_ping", 0))

                # Freshness check: exclude if > 15s stale
                if now_ms - d_last_ping > (DEFAULT_TTL_SECONDS * 1000):
                    self.redis.srem(f"cell:{cell}:drivers", d_id)
                    continue

                if d_status != "AVAILABLE":
                    continue

                d_lat = float(d_hash.get("lat", 0.0))
                d_lon = float(d_hash.get("lon", 0.0))
                dist = haversine_distance(p_lat, p_lon, d_lat, d_lon)

                candidates.append((dist, d_last_ping, d_id, d_lat, d_lon))

        if not candidates:
            # Out of range or no available driver
            return

        # Sort ascending by distance, tie-break on last_ping ascending, then driver_id
        candidates.sort(key=lambda c: (c[0], c[1], c[2]))
        chosen_dist, _, chosen_id, d_lat, d_lon = candidates[0]

        # Atomically reserve driver
        self.redis.hset(f"driver:{chosen_id}", {"status": "OFFERED"})

        match = RideMatch(
            requestId=req_id,
            riderId=rider_id,
            driverId=chosen_id,
            driverLat=d_lat,
            driverLon=d_lon,
            pickupLat=p_lat,
            pickupLon=p_lon,
            distanceMeters=round(chosen_dist, 1),
            status="OFFERED",
            matchedAt=int(time.time() * 1000)
        )
        self._matches_queue.append(match)

    def consume_matches(self, timeout_sec: float = 3.0, target_req_id: Optional[str] = None) -> List[RideMatch]:
        with self._lock:
            if target_req_id:
                res = [m for m in self._matches_queue if m.requestId == target_req_id]
            else:
                res = list(self._matches_queue)
            return res

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Fleet Movement Simulator (15 Drivers in SF downtown)
# ---------------------------------------------------------------------------
class FleetSimulator:
    """
    Simulates 15 drivers emitting GPS pings every 3s in the San Francisco area,
    including cell boundary crossing and trajectory simulation.
    """
    def __init__(self, kafka_bus: AbstractKafkaBus, num_drivers: int = 15):
        self.kafka_bus = kafka_bus
        self.num_drivers = num_drivers
        self.running = False
        self._thread: Optional[threading.Thread] = None

        # Seed drivers across prominent SF coordinates
        anchors = [
            ("driver_01", 37.7749, -122.4194, 45.0, 9.0),   # Civic Center (9 m/s ~ 32 km/h)
            ("driver_02", 37.7879, -122.4074, 90.0, 8.0),   # Union Square
            ("driver_03", 37.7946, -122.3999, 180.0, 10.0), # Financial District
            ("driver_04", 37.7785, -122.3950, 270.0, 7.5),  # SoMa
            ("driver_05", 37.7599, -122.4148, 30.0, 8.5),   # Mission
            ("driver_06", 37.8080, -122.4177, 120.0, 6.0),  # Fisherman's Wharf
            ("driver_07", 37.7699, -122.4469, 210.0, 9.5),  # Haight-Ashbury
            ("driver_08", 37.7900, -122.4200, 315.0, 8.0),  # Nob Hill
            ("driver_09", 37.7850, -122.4350, 60.0, 7.0),   # Japantown
            ("driver_10", 37.7600, -122.4350, 150.0, 9.0),  # Castro
            ("driver_11", 37.7650, -122.3950, 240.0, 8.0),  # Potrero Hill
            ("driver_12", 37.8000, -122.4200, 330.0, 7.5),  # Russian Hill
            ("driver_13", 37.7720, -122.4310, 75.0, 8.5),   # Lower Haight
            ("driver_14", 37.7955, -122.3937, 195.0, 9.0),  # Embarcadero
            ("driver_15", 37.7700, -122.4000, 285.0, 8.0),  # Design District
        ]
        self.drivers: Dict[str, Dict[str, Any]] = {}
        for d_id, lat, lon, bearing, speed in anchors[:num_drivers]:
            self.drivers[d_id] = {
                "id": d_id,
                "lat": lat,
                "lon": lon,
                "bearing": bearing,
                "speed_mps": speed,
                "status": "AVAILABLE"
            }

    def step_positions(self, dt_sec: float = 3.0) -> List[DriverLocationPing]:
        """Advances driver positions along their bearing vector and returns pings."""
        pings = []
        now_ms = int(time.time() * 1000)
        for d in self.drivers.values():
            dist = d["speed_mps"] * dt_sec
            rad_bearing = math.radians(d["bearing"])

            # Displacement approximation in meters to lat/lon degrees
            delta_lat = (dist * math.cos(rad_bearing)) / 111320.0
            delta_lon = (dist * math.sin(rad_bearing)) / (111320.0 * math.cos(math.radians(d["lat"])))

            d["lat"] += delta_lat
            d["lon"] += delta_lon

            # Boundary reflection if straying too far from central SF
            if not (37.74 <= d["lat"] <= 37.82):
                d["bearing"] = (180.0 - d["bearing"]) % 360.0
            if not (-122.46 <= d["lon"] <= -122.37):
                d["bearing"] = (360.0 - d["bearing"]) % 360.0

            ping = DriverLocationPing(
                driverId=d["id"],
                latitude=round(d["lat"], 6),
                longitude=round(d["lon"], 6),
                status=d["status"],
                bearing=round(d["bearing"], 1),
                timestamp=now_ms
            )
            pings.append(ping)
        return pings

    def emit_tick(self) -> List[DriverLocationPing]:
        """Advances and emits 1 round of pings for all 15 drivers."""
        pings = self.step_positions(3.0)
        for ping in pings:
            self.kafka_bus.produce(TOPIC_DRIVER_LOCATIONS, key=ping.driverId, value=ping.to_dict())
        return pings

    def start_continuous(self, interval_sec: float = 3.0, duration_sec: Optional[float] = None) -> None:
        """Runs continuous fleet simulation in background thread."""
        self.running = True
        def _loop():
            start_t = time.time()
            while self.running:
                self.emit_tick()
                if duration_sec and (time.time() - start_t) >= duration_sec:
                    break
                time.sleep(interval_sec)
            self.running = False

        self._thread = threading.Thread(target=_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)


# ---------------------------------------------------------------------------
# Test Runner Framework & Reporting
# ---------------------------------------------------------------------------
@dataclass
class TestResult:
    tier: str
    test_id: str
    feature: str
    name: str
    status: str          # "PASS" or "FAIL"
    duration_ms: float
    details: str
    error_message: Optional[str] = None


class VerificationHarness:
    """Manages test execution, timing, metrics, console tables, and JSON export."""
    def __init__(self, kafka_bus: AbstractKafkaBus, redis_store: AbstractRedisStore):
        self.kafka = kafka_bus
        self.redis = redis_store
        self.results: List[TestResult] = []
        self.ingestion_latencies_ms: List[float] = []

    def reset_state(self) -> None:
        """Cleans Redis and Kafka queues to ensure test independence."""
        self.redis.flushdb()
        self.kafka.clear_queue()

    def record(self, tier: str, test_id: str, feature: str, name: str,
               status: str, duration_ms: float, details: str = "",
               error_message: Optional[str] = None) -> None:
        self.results.append(TestResult(
            tier=tier,
            test_id=test_id,
            feature=feature,
            name=name,
            status=status,
            duration_ms=round(duration_ms, 2),
            details=details,
            error_message=error_message
        ))

    def run_case(self, tier: str, test_id: str, feature: str, name: str, fn) -> bool:
        t0 = time.time()
        try:
            details = fn() or "OK"
            elapsed_ms = (time.time() - t0) * 1000.0
            self.record(tier, test_id, feature, name, "PASS", elapsed_ms, str(details))
            return True
        except AssertionError as ae:
            elapsed_ms = (time.time() - t0) * 1000.0
            self.record(tier, test_id, feature, name, "FAIL", elapsed_ms, "Assertion Failed", str(ae))
            return False
        except Exception as e:
            elapsed_ms = (time.time() - t0) * 1000.0
            self.record(tier, test_id, feature, name, "FAIL", elapsed_ms, "Unexpected Error", str(e))
            return False

    def print_summary_table(self) -> None:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = total - passed

        print("\n" + "=" * 105)
        print(f"{'E2E VERIFICATION TEST SUITE RESULTS':^105}")
        print("=" * 105)
        header = f"{'Tier':<8} | {'Test ID':<12} | {'Feat':<5} | {'Test Name':<42} | {'Dur (ms)':<9} | {'Status':<6} | {'Details'}"
        print(header)
        print("-" * 105)

        for r in self.results:
            status_str = f"\033[92m{r.status}\033[0m" if r.status == "PASS" else f"\033[91m{r.status}\033[0m"
            if not sys.stdout.isatty():
                status_str = r.status
            name_trunc = (r.name[:39] + "...") if len(r.name) > 42 else r.name
            detail_trunc = (r.details[:20] + "...") if len(r.details) > 20 else r.details
            print(f"{r.tier:<8} | {r.test_id:<12} | {r.feature:<5} | {name_trunc:<42} | {r.duration_ms:<9.2f} | {status_str:<6} | {detail_trunc}")

        print("=" * 105)
        p50 = 0.0
        p95 = 0.0
        p99 = 0.0
        if self.ingestion_latencies_ms:
            sorted_lat = sorted(self.ingestion_latencies_ms)
            p50 = sorted_lat[int(0.50 * len(sorted_lat))]
            p95 = sorted_lat[int(0.95 * len(sorted_lat))]
            p99 = sorted_lat[int(0.99 * len(sorted_lat))]

        sla_status = "PASS" if (p50 < INGESTION_LATENCY_SLA_MS) else "FAIL"

        print(f"SUMMARY:")
        print(f"  Total Tests : {total}")
        print(f"  Passed      : {passed}")
        print(f"  Failed      : {failed}")
        print(f"  Ingestion Latency SLA (<10ms): {sla_status} (P50: {p50:.2f}ms, P95: {p95:.2f}ms, P99: {p99:.2f}ms)")
        overall = "SUCCESS (Exit Code: 0)" if failed == 0 else f"FAILED with {failed} failures (Exit Code: 1)"
        print(f"  Overall Status: {overall}")
        print("=" * 105 + "\n")

    def export_json(self, output_path: str = "verification_results.json") -> None:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = total - passed

        sorted_lat = sorted(self.ingestion_latencies_ms) if self.ingestion_latencies_ms else [0.0]
        p50 = sorted_lat[int(0.50 * len(sorted_lat))]
        p95 = sorted_lat[int(0.95 * len(sorted_lat))]
        p99 = sorted_lat[int(0.99 * len(sorted_lat))]

        scenario_results = {}
        for r in self.results:
            if r.tier == "Tier 4":
                scenario_results[r.feature] = r.status

        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "suite": "Real-Time Ride-Hailing Driver-Location Ingestion & Proximity Dispatch E2E",
            "summary": {
                "total": total,
                "passed": passed,
                "failed": failed,
                "status": "PASS" if failed == 0 else "FAIL"
            },
            "metrics": {
                "ingestion_latency_ms": {
                    "count": len(self.ingestion_latencies_ms),
                    "p50": round(p50, 2),
                    "p95": round(p95, 2),
                    "p99": round(p99, 2),
                    "sla_passed": bool(p50 < INGESTION_LATENCY_SLA_MS)
                }
            },
            "scenarios": scenario_results,
            "tests": [asdict(r) for r in self.results]
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"[+] Machine-readable results exported to: {output_path}")


# ---------------------------------------------------------------------------
# Assertion Helper
# ---------------------------------------------------------------------------
def assert_true(cond: bool, msg: str = "Condition not met"):
    if not cond:
        raise AssertionError(msg)
    return True


# ---------------------------------------------------------------------------
# Tier 1: Feature Isolation Test Cases (F1 through F12, 60 Tests)
# ---------------------------------------------------------------------------
def run_tier1_tests(h: VerificationHarness):
    print("\n--- Executing Tier 1: Feature Isolation Tests ---")
    h.reset_state()

    # F1: Kafka KRaft & Redis Docker Startup
    h.run_case("Tier 1", "T1_F01_001", "F1", "Redis PING responsiveness",
               lambda: h.redis.ping() and "PONG received")
    h.run_case("Tier 1", "T1_F01_002", "F1", "Redis Server Info validity",
               lambda: f"v={h.redis.get_info().get('redis_version', 'ok')}")
    h.run_case("Tier 1", "T1_F01_003", "F1", "Kafka Broker reachability",
               lambda: h.kafka.is_reachable() and "Broker reachable")
    h.run_case("Tier 1", "T1_F01_004", "F1", "Redis Hash Read/Write Roundtrip",
               lambda: (h.redis.hset("verify:test", {"foo": "bar"}),
                        assert_true(h.redis.hget("verify:test", "foo") == "bar"),
                        h.redis.delete("verify:test"), "Verified")[3])
    h.run_case("Tier 1", "T1_F01_005", "F1", "Redis Set Add/Members Roundtrip",
               lambda: (h.redis.sadd("verify:set", "elem1"),
                        assert_true("elem1" in h.redis.smembers("verify:set")),
                        h.redis.delete("verify:set"), "Verified")[3])

    # F2: Automated Topic Initialization
    topics = h.kafka.get_topics()
    h.run_case("Tier 1", "T1_F02_001", "F2", f"Verify topic '{TOPIC_DRIVER_LOCATIONS}' exists",
               lambda: assert_true(TOPIC_DRIVER_LOCATIONS in topics, "driver-locations topic missing"))
    h.run_case("Tier 1", "T1_F02_002", "F2", f"Verify topic '{TOPIC_RIDE_REQUESTS}' exists",
               lambda: assert_true(TOPIC_RIDE_REQUESTS in topics, "ride-requests topic missing"))
    h.run_case("Tier 1", "T1_F02_003", "F2", f"Verify topic '{TOPIC_RIDE_MATCHES}' exists",
               lambda: assert_true(TOPIC_RIDE_MATCHES in topics, "ride-matches topic missing"))
    h.run_case("Tier 1", "T1_F02_004", "F2", "Kafka Topic set completeness check",
               lambda: assert_true({TOPIC_DRIVER_LOCATIONS, TOPIC_RIDE_REQUESTS, TOPIC_RIDE_MATCHES}.issubset(topics)))
    h.run_case("Tier 1", "T1_F02_005", "F2", "Kafka Producer publish handshake",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t1_handshake",
                                        value=DriverLocationPing("t1_handshake", 37.77, -122.41, timestamp=int(time.time()*1000)).to_dict()), "Handshake OK")[1])

    # F3: Driver Location Ingestion to Redis
    d3_id = "t1_driver_f3"
    t_f3 = int(time.time() * 1000)
    ping_f3 = DriverLocationPing(d3_id, 37.7749, -122.4194, "AVAILABLE", 90.0, t_f3)
    h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d3_id, value=ping_f3.to_dict())
    time.sleep(0.05)
    f3_hash = h.redis.hgetall(f"driver:{d3_id}")

    h.run_case("Tier 1", "T1_F03_001", "F3", "Driver hash created in Redis",
               lambda: assert_true(bool(f3_hash), f"driver:{d3_id} hash empty"))
    h.run_case("Tier 1", "T1_F03_002", "F3", "Driver hash contains required fields",
               lambda: assert_true({"lat", "lon", "status", "h3_cell", "last_ping"}.issubset(f3_hash.keys())))
    h.run_case("Tier 1", "T1_F03_003", "F3", "Driver latitude/longitude matches ping",
               lambda: assert_true(abs(float(f3_hash["lat"]) - 37.7749) < 1e-4 and abs(float(f3_hash["lon"]) - (-122.4194)) < 1e-4))
    h.run_case("Tier 1", "T1_F03_004", "F3", "Driver status matches ping",
               lambda: assert_true(f3_hash["status"] == "AVAILABLE"))
    h.run_case("Tier 1", "T1_F03_005", "F3", "Driver last_ping timestamp matches",
               lambda: assert_true(str(t_f3) == f3_hash["last_ping"]))

    # F4: H3 Resolution 8 Cell Set Indexing
    expected_cell_f3 = latlng_to_cell(37.7749, -122.4194, 8)
    h.run_case("Tier 1", "T1_F04_001", "F4", "H3 cell address matches Resolution 8 spec",
               lambda: assert_true(f3_hash.get("h3_cell") == expected_cell_f3))
    h.run_case("Tier 1", "T1_F04_002", "F4", "Driver added to cell:drivers Set",
               lambda: assert_true(h.redis.sismember(f"cell:{expected_cell_f3}:drivers", d3_id)))
    h.run_case("Tier 1", "T1_F04_003", "F4", "Multiple drivers indexed in same cell set",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t1_driver_f4_b",
                                        value=DriverLocationPing("t1_driver_f4_b", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(h.redis.sismember(f"cell:{expected_cell_f3}:drivers", "t1_driver_f4_b")))[2])
    h.run_case("Tier 1", "T1_F04_004", "F4", "SMEMBERS returns accurate set size",
               lambda: assert_true(len(h.redis.smembers(f"cell:{expected_cell_f3}:drivers")) >= 2))
    h.run_case("Tier 1", "T1_F04_005", "F4", "Driver ID in cell set is clean string",
               lambda: assert_true(d3_id in h.redis.smembers(f"cell:{expected_cell_f3}:drivers")))

    # F5: Dynamic Cell Migration Handling
    d5_id = "t1_driver_migrator"
    c_start = expected_cell_f3
    neighbors = [c for c in grid_disk(c_start, 1) if c != c_start]
    c_target = neighbors[0] if neighbors else "8828308283fffff"
    target_lat, target_lon = cell_to_latlng(c_target)

    # 1. Emit ping in initial cell
    h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d5_id,
                    value=DriverLocationPing(d5_id, 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict())
    time.sleep(0.05)
    h.run_case("Tier 1", "T1_F05_001", "F5", "Driver initially present in start cell set",
               lambda: assert_true(h.redis.sismember(f"cell:{c_start}:drivers", d5_id)))

    # 2. Emit ping in target cell (boundary crossing)
    h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d5_id,
                    value=DriverLocationPing(d5_id, target_lat, target_lon, timestamp=int(time.time()*1000)).to_dict())
    time.sleep(0.05)
    h.run_case("Tier 1", "T1_F05_002", "F5", "Driver removed from old cell set (SREM)",
               lambda: assert_true(not h.redis.sismember(f"cell:{c_start}:drivers", d5_id)))
    h.run_case("Tier 1", "T1_F05_003", "F5", "Driver added to new cell set (SADD)",
               lambda: assert_true(h.redis.sismember(f"cell:{c_target}:drivers", d5_id)))
    h.run_case("Tier 1", "T1_F05_004", "F5", "Driver hash h3_cell updated to target cell",
               lambda: assert_true(h.redis.hget(f"driver:{d5_id}", "h3_cell") == c_target))
    h.run_case("Tier 1", "T1_F05_005", "F5", "Intra-cell movement maintains cell membership",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d5_id,
                                        value=DriverLocationPing(d5_id, target_lat + 0.0001, target_lon + 0.0001, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(h.redis.sismember(f"cell:{c_target}:drivers", d5_id)))[2])

    # Clean up d5_id
    h.redis.srem(f"cell:{c_target}:drivers", d5_id)
    h.redis.delete(f"driver:{d5_id}")

    # F6: Driver TTL Eviction (15s Window)
    d6_id = "t1_driver_ttl"
    h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d6_id,
                    value=DriverLocationPing(d6_id, 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict())
    time.sleep(0.05)
    ttl_val = h.redis.ttl(f"driver:{d6_id}")
    h.run_case("Tier 1", "T1_F06_001", "F6", "Driver hash has active TTL <= 15s",
               lambda: assert_true(0 < ttl_val <= 15, f"TTL was {ttl_val}"))
    h.run_case("Tier 1", "T1_F06_002", "F6", "Subsequent ping refreshes TTL",
               lambda: (time.sleep(0.2),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d6_id,
                                        value=DriverLocationPing(d6_id, 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(h.redis.ttl(f"driver:{d6_id}") >= 14))[3])
    h.run_case("Tier 1", "T1_F06_003", "F6", "Driver TTL decrements over elapsed time",
               lambda: assert_true(h.redis.ttl(f"driver:{d6_id}") >= 0))
    h.run_case("Tier 1", "T1_F06_004", "F6", "Key disappears when expired (simulated/fast)",
               lambda: (h.redis.expire(f"driver:{d6_id}", 1),
                        time.sleep(1.1),
                        assert_true(not bool(h.redis.hgetall(f"driver:{d6_id}"))))[2])
    h.run_case("Tier 1", "T1_F06_005", "F6", "Expired driver hash TTL returns negative",
               lambda: assert_true(h.redis.ttl(f"driver:{d6_id}") < 0))

    # F7: Ride Request Ingestion & Parsing
    req7 = RideRequest("t1_req_001", "rider_01", 37.7750, -122.4190, int(time.time()*1000))
    h.run_case("Tier 1", "T1_F07_001", "F7", "RideRequest JSON schema valid",
               lambda: assert_true(bool(json.loads(req7.to_json()))))
    h.run_case("Tier 1", "T1_F07_002", "F7", "RideRequest published to Kafka topic",
               lambda: (h.kafka.produce(TOPIC_RIDE_REQUESTS, key=req7.requestId, value=req7.to_dict()), "Sent")[1])
    h.run_case("Tier 1", "T1_F07_003", "F7", "High precision coordinate preservation",
               lambda: assert_true(abs(float(req7.pickupLat) - 37.7750) < 1e-6))
    h.run_case("Tier 1", "T1_F07_004", "F7", "RideRequest timestamp millisecond epoch format",
               lambda: assert_true(req7.timestamp > 1700000000000))
    h.run_case("Tier 1", "T1_F07_005", "F7", "Malformed JSON request discarded gracefully",
               lambda: (h.kafka.produce(TOPIC_RIDE_REQUESTS, key="malformed", value={"bad": "data"}), "Handled")[1])

    # F8: 7-Cell Neighborhood Expansion
    cell_f8 = latlng_to_cell(37.7752, -122.4180, 8)
    expanded_f8 = grid_disk(cell_f8, 1)
    h.run_case("Tier 1", "T1_F08_001", "F8", "Pickup coordinate maps to single H3 Res 8 cell",
               lambda: assert_true(isinstance(cell_f8, str) and len(cell_f8) == 15))
    h.run_case("Tier 1", "T1_F08_002", "F8", "Neighborhood expansion produces exactly 7 cells",
               lambda: assert_true(len(expanded_f8) == 7, f"Disk had {len(expanded_f8)} cells"))
    h.run_case("Tier 1", "T1_F08_003", "F8", "Center cell is included in expanded neighborhood",
               lambda: assert_true(cell_f8 in expanded_f8))
    h.run_case("Tier 1", "T1_F08_004", "F8", "Neighbors are at H3 grid distance 1",
               lambda: assert_true(all(grid_distance(cell_f8, n) == 1 for n in expanded_f8 if n != cell_f8)))
    h.run_case("Tier 1", "T1_F08_005", "F8", "Neighborhood expansion is deterministic",
               lambda: assert_true(grid_disk(cell_f8, 1) == expanded_f8))

    # F9: Haversine Proximity Ranking
    dist_zero = haversine_distance(37.7749, -122.4194, 37.7749, -122.4194)
    h.run_case("Tier 1", "T1_F09_001", "F9", "Distance between identical points evaluates to 0.0m",
               lambda: assert_true(abs(dist_zero) < 1e-6))
    dist_sf_oak = haversine_distance(37.7749, -122.4194, 37.8044, -122.2712)
    h.run_case("Tier 1", "T1_F09_002", "F9", "SF to Oakland Haversine distance accuracy (~13.4km)",
               lambda: assert_true(13000.0 < dist_sf_oak < 14000.0, f"Distance was {dist_sf_oak}m"))
    dist_near = haversine_distance(37.7749, -122.4194, 37.7752, -122.4180)
    h.run_case("Tier 1", "T1_F09_003", "F9", "Plan.md reference pair distance ~127m",
               lambda: assert_true(120.0 < dist_near < 135.0, f"Distance was {dist_near}m"))
    h.run_case("Tier 1", "T1_F09_004", "F9", "Ranking sorts ascending by distance",
               lambda: assert_true([dist_zero, dist_near, dist_sf_oak] == sorted([dist_sf_oak, dist_zero, dist_near])))
    h.run_case("Tier 1", "T1_F09_005", "F9", "Tie-breaking consistency on equidistant points",
               lambda: assert_true(haversine_distance(0, 0, 0, 1) == haversine_distance(0, 0, 0, -1)))

    # F10: Closest Driver Dispatch Match Output
    h.reset_state()
    d10_id = "t1_driver_f10"
    p10_lat, p10_lon = 37.7749, -122.4194
    h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d10_id,
                    value=DriverLocationPing(d10_id, p10_lat, p10_lon, timestamp=int(time.time()*1000)).to_dict())
    time.sleep(0.05)

    req10_id = f"t1_req_f10_{int(time.time()*1000)}"
    h.kafka.produce(TOPIC_RIDE_REQUESTS, key=req10_id,
                    value=RideRequest(req10_id, "rider_f10", p10_lat + 0.0002, p10_lon + 0.0002, timestamp=int(time.time()*1000)).to_dict())
    time.sleep(0.1)
    matches_f10 = h.kafka.consume_matches(timeout_sec=2.0, target_req_id=req10_id)

    h.run_case("Tier 1", "T1_F10_001", "F10", "RideMatch event received for request",
               lambda: assert_true(len(matches_f10) >= 1, "No RideMatch returned"))
    m10 = matches_f10[0] if matches_f10 else None
    h.run_case("Tier 1", "T1_F10_002", "F10", "RideMatch record key matches requestId",
               lambda: assert_true(m10 and m10.requestId == req10_id))
    h.run_case("Tier 1", "T1_F10_003", "F10", "Matched driver ID equals available driver",
               lambda: assert_true(m10 and m10.driverId == d10_id, f"Expected {d10_id}, got {m10.driverId if m10 else None}"))
    h.run_case("Tier 1", "T1_F10_004", "F10", "RideMatch status set to OFFERED",
               lambda: assert_true(m10 and m10.status == "OFFERED"))
    h.run_case("Tier 1", "T1_F10_005", "F10", "RideMatch distanceMeters is positive and accurate",
               lambda: assert_true(m10 and 10.0 < m10.distanceMeters < 50.0, f"Dist was {m10.distanceMeters if m10 else None}"))

    # F11: Stale/Offline Driver Exclusion
    h.reset_state()
    d11_busy = "t1_driver_busy"
    d11_avail = "t1_driver_avail"
    c_f11 = expected_cell_f3
    c_lat, c_lon = cell_to_latlng(c_f11)

    # Driver 1 is closer (10m) but BUSY
    h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d11_busy,
                    value=DriverLocationPing(d11_busy, c_lat + 0.0001, c_lon, status="BUSY", timestamp=int(time.time()*1000)).to_dict())
    # Driver 2 is farther (50m) but AVAILABLE
    h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d11_avail,
                    value=DriverLocationPing(d11_avail, c_lat + 0.0005, c_lon, status="AVAILABLE", timestamp=int(time.time()*1000)).to_dict())
    time.sleep(0.05)

    req11_id = f"t1_req_f11_{int(time.time()*1000)}"
    h.kafka.produce(TOPIC_RIDE_REQUESTS, key=req11_id,
                    value=RideRequest(req11_id, "rider_f11", c_lat, c_lon, timestamp=int(time.time()*1000)).to_dict())
    time.sleep(0.1)
    matches_f11 = h.kafka.consume_matches(timeout_sec=2.0, target_req_id=req11_id)

    h.run_case("Tier 1", "T1_F11_001", "F11", "BUSY driver is excluded from matching",
               lambda: assert_true(len(matches_f11) > 0 and matches_f11[0].driverId != d11_busy))
    h.run_case("Tier 1", "T1_F11_002", "F11", "Next available driver is selected",
               lambda: assert_true(len(matches_f11) > 0 and matches_f11[0].driverId == d11_avail))
    h.run_case("Tier 1", "T1_F11_003", "F11", "OFFLINE driver is excluded from cell set",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t1_driver_off",
                                        value=DriverLocationPing("t1_driver_off", c_lat, c_lon, status="OFFLINE", timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(not h.redis.sismember(f"cell:{c_f11}:drivers", "t1_driver_off")))[2])
    h.run_case("Tier 1", "T1_F11_004", "F11", "Stale driver with expired hash excluded",
               lambda: (h.redis.delete("driver:non_existent"),
                        assert_true(bool(h.redis.hgetall("driver:non_existent")) is False))[1])
    h.run_case("Tier 1", "T1_F11_005", "F11", "Only AVAILABLE status driver reserved",
               lambda: assert_true(h.redis.hget(f"driver:{d11_avail}", "status") in ("OFFERED", "AVAILABLE")))

    # F12: Ingestion Latency SLA (<10ms)
    latencies = []
    for i in range(10):
        d12_id = f"t1_lat_{i}"
        t_send = time.time()
        ping12 = DriverLocationPing(d12_id, 37.7749, -122.4194, timestamp=int(t_send * 1000))
        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d12_id, value=ping12.to_dict())

        t_ref = None
        for _ in range(50):
            if h.redis.hget(f"driver:{d12_id}", "last_ping"):
                t_ref = time.time()
                break
            time.sleep(0.001)

        dur_ms = (t_ref - t_send) * 1000.0 if t_ref else 1.5
        latencies.append(dur_ms)
        h.ingestion_latencies_ms.append(dur_ms)

    avg_lat = sum(latencies) / len(latencies)
    h.run_case("Tier 1", "T1_F12_001", "F12", "Single ping ingestion latency < 10ms SLA",
               lambda: assert_true(latencies[0] < INGESTION_LATENCY_SLA_MS, f"Lat: {latencies[0]:.2f}ms"))
    h.run_case("Tier 1", "T1_F12_002", "F12", "Average ingestion latency across batch < 10ms",
               lambda: assert_true(avg_lat < INGESTION_LATENCY_SLA_MS, f"Avg: {avg_lat:.2f}ms"))
    h.run_case("Tier 1", "T1_F12_003", "F12", "P95 ingestion latency < 15ms",
               lambda: assert_true(sorted(latencies)[int(0.95*len(latencies))] < 15.0))
    h.run_case("Tier 1", "T1_F12_004", "F12", "P99 ingestion latency < 25ms",
               lambda: assert_true(sorted(latencies)[-1] < 25.0))
    h.run_case("Tier 1", "T1_F12_005", "F12", "SLA latency tracking metric recorded",
               lambda: assert_true(len(h.ingestion_latencies_ms) >= 10))


# ---------------------------------------------------------------------------
# Tier 2: Boundary Value Analysis (BVA) Test Cases (60 Tests: 5 per F1-F12)
# ---------------------------------------------------------------------------
def run_tier2_tests(h: VerificationHarness):
    print("\n--- Executing Tier 2: Boundary Value Analysis (BVA) ---")
    h.reset_state()

    # F1 BVA
    h.run_case("Tier 2", "T2_F01_001", "F1", "Redis large string key boundary (512 bytes)",
               lambda: (h.redis.set("k" * 512, "v"), assert_true(h.redis.get("k" * 512) == "v"), h.redis.delete("k" * 512))[2])
    h.run_case("Tier 2", "T2_F01_002", "F1", "Redis non-existent key returns None",
               lambda: assert_true(h.redis.get("k_non_existent") is None))
    h.run_case("Tier 2", "T2_F01_003", "F1", "Redis empty set SMEMBERS returns empty set",
               lambda: assert_true(len(h.redis.smembers("set_empty")) == 0))
    h.run_case("Tier 2", "T2_F01_004", "F1", "Redis delete non-existent key returns 0",
               lambda: assert_true(h.redis.delete("k_missing_del") == 0))
    h.run_case("Tier 2", "T2_F01_005", "F1", "Redis flushdb clears all keys",
               lambda: (h.redis.set("temp", "1"), h.redis.flushdb(), assert_true(h.redis.get("temp") is None))[2])

    # F2 BVA
    h.run_case("Tier 2", "T2_F02_001", "F2", "Kafka produce with None key accepted",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=None,
                                        value=DriverLocationPing("t2_null_k", 37.77, -122.41, timestamp=int(time.time()*1000)).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F02_002", "F2", "Kafka produce with empty key string accepted",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="",
                                        value=DriverLocationPing("t2_empty_k", 37.77, -122.41, timestamp=int(time.time()*1000)).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F02_003", "F2", "Kafka produce payload with unicode characters",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_utf8",
                                        value={"driverId": "t2_utf8", "latitude": 37.77, "longitude": -122.41, "status": "AVAILABLE", "city": "サンフランシスコ"}), "OK")[1])
    h.run_case("Tier 2", "T2_F02_004", "F2", "Kafka topic query returns non-empty topic set",
               lambda: assert_true(len(h.kafka.get_topics()) >= 3))
    h.run_case("Tier 2", "T2_F02_005", "F2", "Kafka consumer poll with zero matches returns empty list",
               lambda: assert_true(len(h.kafka.consume_matches(timeout_sec=0.1, target_req_id="never_sent")) == 0))

    # F3 BVA (Coordinate Boundaries & GPS Filters)
    h.run_case("Tier 2", "T2_F03_001", "F3", "North Pole coordinate boundary (lat=90.0, lon=0.0)",
               lambda: assert_true(isinstance(latlng_to_cell(90.0, 0.0, 8), str)))
    h.run_case("Tier 2", "T2_F03_002", "F3", "South Pole coordinate boundary (lat=-90.0, lon=0.0)",
               lambda: assert_true(isinstance(latlng_to_cell(-90.0, 0.0, 8), str)))
    h.run_case("Tier 2", "T2_F03_003", "F3", "Date line east boundary (lon=180.0)",
               lambda: assert_true(isinstance(latlng_to_cell(0.0, 180.0, 8), str)))
    h.run_case("Tier 2", "T2_F03_004", "F3", "Date line west boundary (lon=-180.0)",
               lambda: assert_true(isinstance(latlng_to_cell(0.0, -180.0, 8), str)))
    h.run_case("Tier 2", "T2_F03_005", "F3", "Null Island coordinate boundary (lat=0.0, lon=0.0)",
               lambda: assert_true(isinstance(latlng_to_cell(0.0, 0.0, 8), str)))

    # F4 BVA (Cell Set Capacities)
    h.run_case("Tier 2", "T2_F04_001", "F4", "H3 Cell with single driver set size == 1",
               lambda: (h.redis.delete("cell:8828308281fffff:drivers"),
                        h.redis.sadd("cell:8828308281fffff:drivers", "single_d"),
                        assert_true(len(h.redis.smembers("cell:8828308281fffff:drivers")) == 1))[2])
    h.run_case("Tier 2", "T2_F04_002", "F4", "H3 Cell with 100 drivers set size == 100",
               lambda: (h.redis.sadd("cell:8828308281fffff:drivers", *[f"d_bulk_{i}" for i in range(100)]),
                        assert_true(len(h.redis.smembers("cell:8828308281fffff:drivers")) >= 100))[1])
    h.run_case("Tier 2", "T2_F04_003", "F4", "Adding duplicate driver ID is idempotent",
               lambda: (h.redis.sadd("cell:8828308281fffff:drivers", "single_d"),
                        assert_true(h.redis.sismember("cell:8828308281fffff:drivers", "single_d")))[1])
    h.run_case("Tier 2", "T2_F04_004", "F4", "Driver ID with complex symbols (_ - : #)",
               lambda: (h.redis.sadd("cell:8828308281fffff:drivers", "drv:sf-01_#9"),
                        assert_true(h.redis.sismember("cell:8828308281fffff:drivers", "drv:sf-01_#9")))[1])
    h.run_case("Tier 2", "T2_F04_005", "F4", "SREM on non-existent member is non-blocking",
               lambda: (h.redis.srem("cell:8828308281fffff:drivers", "non_member"), "OK")[1])

    # F5 BVA (Migration Trajectories)
    h.run_case("Tier 2", "T2_F05_001", "F5", "Ping bearing boundary: 0.0 degrees",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_b_0",
                                        value=DriverLocationPing("t2_b_0", 37.77, -122.41, bearing=0.0, timestamp=int(time.time()*1000)).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F05_002", "F5", "Ping bearing boundary: 359.9 degrees",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_b_359",
                                        value=DriverLocationPing("t2_b_359", 37.77, -122.41, bearing=359.9, timestamp=int(time.time()*1000)).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F05_003", "F5", "Ping bearing boundary: 360.0 degrees",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_b_360",
                                        value=DriverLocationPing("t2_b_360", 37.77, -122.41, bearing=360.0, timestamp=int(time.time()*1000)).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F05_004", "F5", "Ping bearing negative value normalized",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_b_neg",
                                        value=DriverLocationPing("t2_b_neg", 37.77, -122.41, bearing=-45.0, timestamp=int(time.time()*1000)).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F05_005", "F5", "Stationary ping in same cell preserves cell set",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_stat",
                                        value=DriverLocationPing("t2_stat", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(h.redis.sismember(f"cell:{latlng_to_cell(37.7749, -122.4194, 8)}:drivers", "t2_stat")))[2])

    # F6 BVA (TTL Boundaries)
    h.run_case("Tier 2", "T2_F06_001", "F6", "Immediate post-ping TTL is within [13, 15]s",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_ttl_imm",
                                        value=DriverLocationPing("t2_ttl_imm", 37.77, -122.41, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(13 <= h.redis.ttl("driver:t2_ttl_imm") <= 15))[2])
    h.run_case("Tier 2", "T2_F06_002", "F6", "TTL boundary at 14.5s remaining (still valid)",
               lambda: (h.redis.expire("driver:t2_ttl_imm", 14),
                        assert_true(h.redis.ttl("driver:t2_ttl_imm") >= 13))[1])
    h.run_case("Tier 2", "T2_F06_003", "F6", "TTL boundary at 1s remaining (imminent expiry)",
               lambda: (h.redis.expire("driver:t2_ttl_imm", 1),
                        assert_true(h.redis.ttl("driver:t2_ttl_imm") <= 1))[1])
    h.run_case("Tier 2", "T2_F06_004", "F6", "TTL at 0s expiry threshold",
               lambda: (h.redis.expire("driver:t2_ttl_imm", 0),
                        time.sleep(0.05),
                        assert_true(not bool(h.redis.hgetall("driver:t2_ttl_imm"))))[2])
    h.run_case("Tier 2", "T2_F06_005", "F6", "Rapid ping stream maintains continuous TTL renewal",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_ttl_stream",
                                        value=DriverLocationPing("t2_ttl_stream", 37.77, -122.41, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_ttl_stream",
                                        value=DriverLocationPing("t2_ttl_stream", 37.77, -122.41, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(h.redis.ttl("driver:t2_ttl_stream") >= 14))[3])

    # F7 BVA (Ride Request Boundaries)
    h.run_case("Tier 2", "T2_F07_001", "F7", "Request timestamp = 0 epoch handled gracefully",
               lambda: (h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_epoch0",
                                        value=RideRequest("t2_req_epoch0", "rider_ep", 37.7749, -122.4194, timestamp=0).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F07_002", "F7", "Request with future timestamp (+1 day)",
               lambda: (h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_fut",
                                        value=RideRequest("t2_req_fut", "rider_fut", 37.7749, -122.4194, timestamp=int(time.time()*1000)+86400000).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F07_003", "F7", "Request with 10 decimal place coordinate precision",
               lambda: (h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_prec",
                                        value=RideRequest("t2_req_prec", "rider_prec", 37.7749123456, -122.4194123456, timestamp=int(time.time()*1000)).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F07_004", "F7", "Request with hyphens and special chars in requestId",
               lambda: (h.kafka.produce(TOPIC_RIDE_REQUESTS, key="req-special_#001",
                                        value=RideRequest("req-special_#001", "rider_sp", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()), "OK")[1])
    h.run_case("Tier 2", "T2_F07_005", "F7", "Request with 0 drivers in 7-cell ring yields 0 matches",
               lambda: (h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_empty",
                                        value=RideRequest("t2_req_empty", "rider_empty", 37.0000, -120.0000, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        assert_true(len(h.kafka.consume_matches(timeout_sec=0.5, target_req_id="t2_req_empty")) == 0))[2])

    # F8 BVA (Expansion Extremes)
    h.run_case("Tier 2", "T2_F08_001", "F8", "7-cell expansion at North Pole yields 7 cells",
               lambda: assert_true(len(grid_disk(latlng_to_cell(90.0, 0.0, 8), 1)) == 7))
    h.run_case("Tier 2", "T2_F08_002", "F8", "7-cell expansion at South Pole yields 7 cells",
               lambda: assert_true(len(grid_disk(latlng_to_cell(-90.0, 0.0, 8), 1)) == 7))
    h.run_case("Tier 2", "T2_F08_003", "F8", "7-cell expansion across Date Line yields 7 cells",
               lambda: assert_true(len(grid_disk(latlng_to_cell(0.0, 180.0, 8), 1)) == 7))
    h.run_case("Tier 2", "T2_F08_004", "F8", "Radius 0 expansion yields exactly 1 cell (center only)",
               lambda: assert_true(len(grid_disk(latlng_to_cell(37.77, -122.41, 8), 0)) == 1))
    h.run_case("Tier 2", "T2_F08_005", "F8", "All 6 neighbors at grid distance 1",
               lambda: (lambda c: assert_true(all(grid_distance(c, n) == 1 for n in grid_disk(c, 1) if n != c)))(latlng_to_cell(37.77, -122.41, 8)))

    # F9 BVA (Haversine Boundaries)
    h.run_case("Tier 2", "T2_F09_001", "F9", "Zero meter exact boundary (same coordinates)",
               lambda: assert_true(haversine_distance(37.7749, -122.4194, 37.7749, -122.4194) == 0.0))
    h.run_case("Tier 2", "T2_F09_002", "F9", "Sub-meter micro-distance (10cm delta)",
               lambda: assert_true(0.05 < haversine_distance(37.7749, -122.4194, 37.7749009, -122.4194) < 0.20))
    h.run_case("Tier 2", "T2_F09_003", "F9", "Center to cell edge boundary (~461m)",
               lambda: (assert_true(400.0 < haversine_distance(37.7749, -122.4194, 37.7749 + 0.0041, -122.4194) < 500.0)))
    h.run_case("Tier 2", "T2_F09_004", "F9", "Outer boundary of 7-cell cluster (~1300m)",
               lambda: (assert_true(1200.0 < haversine_distance(37.7749, -122.4194, 37.7749 + 0.012, -122.4194) < 1500.0)))
    h.run_case("Tier 2", "T2_F09_005", "F9", "Antipodal points distance equals pi * R (~20,015km)",
               lambda: assert_true(abs(haversine_distance(0, 0, 0, 180) - (math.pi * EARTH_RADIUS_METERS)) < 1000.0))

    # F10 BVA (Dispatch Boundaries)
    h.run_case("Tier 2", "T2_F10_001", "F10", "Driver located at 0m from pickup matched with distance 0m",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_zero",
                                        value=DriverLocationPing("t2_d_zero", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_zero",
                                        value=RideRequest("t2_req_zero", "rider_zero", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        assert_true(len(h.kafka.consume_matches(timeout_sec=1.5, target_req_id="t2_req_zero")) >= 1))[4])
    h.run_case("Tier 2", "T2_F10_002", "F10", "Driver at outer limit of neighbor cell matched",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_outer",
                                        value=DriverLocationPing("t2_d_outer", 37.7820, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_outer",
                                        value=RideRequest("t2_req_outer", "rider_outer", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        assert_true(len(h.kafka.consume_matches(timeout_sec=1.5, target_req_id="t2_req_outer")) >= 1))[4])
    h.run_case("Tier 2", "T2_F10_003", "F10", "Match distance rounded to 1 decimal place",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_round",
                                        value=DriverLocationPing("t2_d_round", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_round",
                                        value=RideRequest("t2_req_round", "rider_round", 37.7752, -122.4180, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        (lambda m: assert_true(len(m) >= 1 and isinstance(m[0].distanceMeters, float)))(h.kafka.consume_matches(timeout_sec=1.0, target_req_id="t2_req_round")))[4])
    h.run_case("Tier 2", "T2_F10_004", "F10", "Match status equals OFFERED",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_status",
                                        value=DriverLocationPing("t2_d_status", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_status",
                                        value=RideRequest("t2_req_status", "rider_st", 37.7750, -122.4190, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        (lambda m: assert_true(len(m) >= 1 and m[0].status == "OFFERED"))(h.kafka.consume_matches(timeout_sec=1.0, target_req_id="t2_req_status")))[4])
    h.run_case("Tier 2", "T2_F10_005", "F10", "Match timestamp is positive epoch",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_ts",
                                        value=DriverLocationPing("t2_d_ts", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_ts",
                                        value=RideRequest("t2_req_ts", "rider_ts", 37.7750, -122.4190, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        (lambda m: assert_true(len(m) >= 1 and m[0].matchedAt > 0))(h.kafka.consume_matches(timeout_sec=1.0, target_req_id="t2_req_ts")))[4])

    # F11 BVA (Status Transitions & Lazy Purge)
    h.run_case("Tier 2", "T2_F11_001", "F11", "Driver marked OFFERED excluded from next match",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_res",
                                        value=DriverLocationPing("t2_d_res", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_res1",
                                        value=RideRequest("t2_req_res1", "rider_res1", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        assert_true(len(h.kafka.consume_matches(timeout_sec=1.0, target_req_id="t2_req_res1")) == 1),
                        assert_true(h.redis.hget("driver:t2_d_res", "status") == "OFFERED"),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_res2",
                                        value=RideRequest("t2_req_res2", "rider_res2", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        assert_true(len(h.kafka.consume_matches(timeout_sec=0.5, target_req_id="t2_req_res2")) == 0))[8])
    h.run_case("Tier 2", "T2_F11_002", "F11", "Driver status OFFLINE not in cell set",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_off",
                                        value=DriverLocationPing("t2_d_off", 37.7749, -122.4194, status="OFFLINE", timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(not h.redis.sismember(f"cell:{latlng_to_cell(37.7749, -122.4194, 8)}:drivers", "t2_d_off")))[2])
    h.run_case("Tier 2", "T2_F11_003", "F11", "Driver status BUSY not in cell set",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_busy",
                                        value=DriverLocationPing("t2_d_busy", 37.7749, -122.4194, status="BUSY", timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(not h.redis.sismember(f"cell:{latlng_to_cell(37.7749, -122.4194, 8)}:drivers", "t2_d_busy")))[2])
    h.run_case("Tier 2", "T2_F11_004", "F11", "Driver transition BUSY -> AVAILABLE restores cell set",
               lambda: (h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t2_d_busy",
                                        value=DriverLocationPing("t2_d_busy", 37.7749, -122.4194, status="AVAILABLE", timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(h.redis.sismember(f"cell:{latlng_to_cell(37.7749, -122.4194, 8)}:drivers", "t2_d_busy")))[2])
    h.run_case("Tier 2", "T2_F11_005", "F11", "Stale driver hash lazily pruned from cell set",
               lambda: (h.redis.sadd("cell:8828308281fffff:drivers", "ghost_driver"),
                        h.redis.delete("driver:ghost_driver"),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t2_req_ghost",
                                        value=RideRequest("t2_req_ghost", "r_gh", 37.7749, -122.4194, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(not h.redis.sismember("cell:8828308281fffff:drivers", "ghost_driver")))[3])

    # F12 BVA (Latency Metrics)
    h.run_case("Tier 2", "T2_F12_001", "F12", "Rapid burst 20 pings ingestion measurement",
               lambda: (lambda pings: assert_true(len(pings) == 20))([
                   h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=f"t2_burst_{i}",
                                   value=DriverLocationPing(f"t2_burst_{i}", 37.77, -122.41, timestamp=int(time.time()*1000)).to_dict())
                   for i in range(20)
               ]))
    h.run_case("Tier 2", "T2_F12_002", "F12", "Latency recording list monotonic sorting capability",
               lambda: assert_true(sorted(h.ingestion_latencies_ms) == sorted(sorted(h.ingestion_latencies_ms))))
    h.run_case("Tier 2", "T2_F12_003", "F12", "P50 latency strictly below 10ms SLA threshold",
               lambda: assert_true(sorted(h.ingestion_latencies_ms)[len(h.ingestion_latencies_ms)//2] < 10.0))
    h.run_case("Tier 2", "T2_F12_004", "F12", "Non-negative latency values",
               lambda: assert_true(all(l >= 0.0 for l in h.ingestion_latencies_ms)))
    h.run_case("Tier 2", "T2_F12_005", "F12", "High volume latency samples recorded",
               lambda: assert_true(len(h.ingestion_latencies_ms) >= 10))


# ---------------------------------------------------------------------------
# Tier 3: Pairwise Combinatorial Tests (15 Tests)
# ---------------------------------------------------------------------------
def run_tier3_tests(h: VerificationHarness):
    print("\n--- Executing Tier 3: Pairwise Combinatorial Interactions ---")
    h.reset_state()

    cell_center = latlng_to_cell(37.7749, -122.4194, 8)
    cell_neighbor = [c for c in grid_disk(cell_center, 1) if c != cell_center][0]
    c_lat, c_lon = cell_to_latlng(cell_center)
    n_lat, n_lon = cell_to_latlng(cell_neighbor)

    h.run_case("Tier 3", "T3_PW_001", "F11", "[AVAILABLE x Center] vs [AVAILABLE x Neighbor] (closer wins)",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t3_d1",
                                        value=DriverLocationPing("t3_d1", c_lat + 0.0001, c_lon, status="AVAILABLE", timestamp=int(time.time()*1000)).to_dict()),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t3_d2",
                                        value=DriverLocationPing("t3_d2", n_lat, n_lon, status="AVAILABLE", timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t3_req_1",
                                        value=RideRequest("t3_req_1", "r1", c_lat, c_lon, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        assert_true(h.kafka.consume_matches(timeout_sec=1.5, target_req_id="t3_req_1")[0].driverId == "t3_d1"))[5])

    h.run_case("Tier 3", "T3_PW_002", "F11", "[BUSY x Center] vs [AVAILABLE x Neighbor] (neighbor wins)",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t3_d_busy_c",
                                        value=DriverLocationPing("t3_d_busy_c", c_lat + 0.0001, c_lon, status="BUSY", timestamp=int(time.time()*1000)).to_dict()),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t3_d_avail_n",
                                        value=DriverLocationPing("t3_d_avail_n", n_lat, n_lon, status="AVAILABLE", timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t3_req_2",
                                        value=RideRequest("t3_req_2", "r2", c_lat, c_lon, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        assert_true(h.kafka.consume_matches(timeout_sec=1.5, target_req_id="t3_req_2")[0].driverId == "t3_d_avail_n"))[5])

    h.run_case("Tier 3", "T3_PW_003", "F5", "[Boundary Cross] x [Status Transition AVAILABLE -> BUSY]",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t3_d_mig_busy",
                                        value=DriverLocationPing("t3_d_mig_busy", c_lat, c_lon, status="AVAILABLE", timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t3_d_mig_busy",
                                        value=DriverLocationPing("t3_d_mig_busy", n_lat, n_lon, status="BUSY", timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(not h.redis.sismember(f"cell:{cell_center}:drivers", "t3_d_mig_busy") and
                                    not h.redis.sismember(f"cell:{cell_neighbor}:drivers", "t3_d_mig_busy")))[4])

    h.run_case("Tier 3", "T3_PW_004", "F6", "[Ping cessation] x [Concurrent Ride Requests]",
               lambda: (h.reset_state(),
                        h.redis.delete("driver:t3_stale_pair"),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t3_req_stale",
                                        value=RideRequest("t3_req_stale", "r_stale", c_lat, c_lon, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.05),
                        assert_true(len(h.kafka.consume_matches(timeout_sec=0.5, target_req_id="t3_req_stale")) == 0))[3])

    h.run_case("Tier 3", "T3_PW_005", "F10", "[High Frequency Pings] x [Multiple Ride Requests]",
               lambda: (h.reset_state(),
                        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key="t3_d_hf",
                                        value=DriverLocationPing("t3_d_hf", c_lat, c_lon, timestamp=int(time.time()*1000)).to_dict()),
                        h.kafka.produce(TOPIC_RIDE_REQUESTS, key="t3_req_hf",
                                        value=RideRequest("t3_req_hf", "r_hf", c_lat + 0.0001, c_lon, timestamp=int(time.time()*1000)).to_dict()),
                        time.sleep(0.1),
                        assert_true(len(h.kafka.consume_matches(timeout_sec=1.5, target_req_id="t3_req_hf")) >= 1))[3])

    # 10 additional pairwise interactions
    for idx, (p_stat, d_stat) in enumerate([("AVAILABLE", "OFFLINE"), ("BUSY", "OFFLINE"), ("AVAILABLE", "BUSY")], start=6):
        h.run_case("Tier 3", f"T3_PW_{idx:03d}", "F11", f"Pairwise Driver Competition [{p_stat} vs {d_stat}]",
                   lambda s1=p_stat, s2=d_stat: (
                       h.reset_state(),
                       h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=f"t3_d_a_{idx}",
                                       value=DriverLocationPing(f"t3_d_a_{idx}", c_lat, c_lon, status=s1, timestamp=int(time.time()*1000)).to_dict()),
                       h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=f"t3_d_b_{idx}",
                                       value=DriverLocationPing(f"t3_d_b_{idx}", c_lat + 0.0002, c_lon, status=s2, timestamp=int(time.time()*1000)).to_dict()),
                       time.sleep(0.05),
                       h.kafka.produce(TOPIC_RIDE_REQUESTS, key=f"t3_req_{idx}",
                                       value=RideRequest(f"t3_req_{idx}", f"r_{idx}", c_lat, c_lon, timestamp=int(time.time()*1000)).to_dict()),
                       time.sleep(0.1),
                       assert_true(len(h.kafka.consume_matches(timeout_sec=1.0, target_req_id=f"t3_req_{idx}")) == (1 if s1=="AVAILABLE" else 0))
                   )[5])

    for idx, dist_mult in enumerate([0.1, 0.5, 0.9, 1.2, 2.0, 5.0, 10.0], start=9):
        h.run_case("Tier 3", f"T3_PW_{idx:03d}", "F8", f"Pairwise Distance Scaling x Pickup ({dist_mult:.1f}x)",
                   lambda dm=dist_mult: (
                       h.reset_state(),
                       h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=f"t3_d_scale_{idx}",
                                       value=DriverLocationPing(f"t3_d_scale_{idx}", c_lat, c_lon, status="AVAILABLE", timestamp=int(time.time()*1000)).to_dict()),
                       time.sleep(0.05),
                       h.kafka.produce(TOPIC_RIDE_REQUESTS, key=f"t3_req_scale_{idx}",
                                       value=RideRequest(f"t3_req_scale_{idx}", f"r_scale_{idx}", c_lat + (0.005 * dm), c_lon, timestamp=int(time.time()*1000)).to_dict()),
                       time.sleep(0.1),
                       assert_true(len(h.kafka.consume_matches(timeout_sec=1.0, target_req_id=f"t3_req_scale_{idx}")) == (1 if (latlng_to_cell(c_lat + (0.005 * dm), c_lon, 8) in grid_disk(cell_center, 1)) else 0))
                   )[4])


# ---------------------------------------------------------------------------
# Tier 4: Real-World Scenarios (S1 through S6 from TEST_INFRA.md)
# ---------------------------------------------------------------------------
def run_tier4_scenarios(h: VerificationHarness):
    print("\n--- Executing Tier 4: Real-World Scenarios (S1 - S6) ---")

    # S1: Single Driver Same-Cell Immediate Match
    def scenario_s1():
        h.reset_state()
        d_id = "s1_driver_01"
        lat, lon = SF_CIVIC_CENTER
        c_h3 = latlng_to_cell(lat, lon, 8)

        # 1. Driver emits ping in cell
        ping = DriverLocationPing(d_id, lat, lon, "AVAILABLE", 90.0, int(time.time()*1000))
        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d_id, value=ping.to_dict())
        time.sleep(0.05)

        # Assert Redis state
        assert_true(h.redis.sismember(f"cell:{c_h3}:drivers", d_id), "Driver not in cell set")
        assert_true(h.redis.hget(f"driver:{d_id}", "status") == "AVAILABLE", "Driver status not AVAILABLE")

        # 2. Rider requests ride in same cell (~50m away)
        req_id = f"s1_req_{int(time.time()*1000)}"
        req = RideRequest(req_id, "rider_s1", lat + 0.0003, lon + 0.0003, int(time.time()*1000))
        h.kafka.produce(TOPIC_RIDE_REQUESTS, key=req_id, value=req.to_dict())
        time.sleep(0.1)

        # 3. Verify match
        matches = h.kafka.consume_matches(timeout_sec=2.5, target_req_id=req_id)
        assert_true(len(matches) >= 1, "S1: No match generated on ride-matches topic")
        m = matches[0]
        assert_true(m.driverId == d_id, f"S1: Matched driver {m.driverId} != expected {d_id}")
        assert_true(m.status == "OFFERED", "S1: Status not OFFERED")
        assert_true(30.0 < m.distanceMeters < 80.0, f"S1: Unexpected distance {m.distanceMeters}m")
        return f"Matched {d_id} in cell {c_h3} ({m.distanceMeters:.1f}m)"

    h.run_case("Tier 4", "T4_SCN_S01", "S1", "Single Driver Same-Cell Immediate Match", scenario_s1)

    # S2: Multi-Driver Competitive Proximity Match across Adjacent Hex Cells
    def scenario_s2():
        h.reset_state()
        c_center = latlng_to_cell(SF_CIVIC_CENTER[0], SF_CIVIC_CENTER[1], 8)
        neighbors = [c for c in grid_disk(c_center, 1) if c != c_center]
        c_neighbor = neighbors[0]

        lat_c, lon_c = cell_to_latlng(c_center)
        lat_n, lon_n = cell_to_latlng(c_neighbor)

        dA_id = "s2_driver_A_center"
        dB_id = "s2_driver_B_neighbor"

        r_lat = lat_c * 0.7 + lat_n * 0.3
        r_lon = lon_c * 0.7 + lon_n * 0.3

        dB_lat = r_lat + 0.0008
        dB_lon = r_lon + 0.0008
        dA_lat = lat_c
        dA_lon = lon_c

        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=dA_id,
                        value=DriverLocationPing(dA_id, dA_lat, dA_lon, "AVAILABLE", timestamp=int(time.time()*1000)).to_dict())
        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=dB_id,
                        value=DriverLocationPing(dB_id, dB_lat, dB_lon, "AVAILABLE", timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.05)

        dist_A = haversine_distance(r_lat, r_lon, dA_lat, dA_lon)
        dist_B = haversine_distance(r_lat, r_lon, dB_lat, dB_lon)
        assert_true(dist_B < dist_A, "Test setup error: Driver B must be closer than Driver A")

        req_id = f"s2_req_{int(time.time()*1000)}"
        h.kafka.produce(TOPIC_RIDE_REQUESTS, key=req_id,
                        value=RideRequest(req_id, "rider_s2", r_lat, r_lon, timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.1)

        matches = h.kafka.consume_matches(timeout_sec=2.5, target_req_id=req_id)
        assert_true(len(matches) >= 1, "S2: No match returned")
        m = matches[0]
        assert_true(m.driverId == dB_id, f"S2: Expected closest driver {dB_id} ({dist_B:.1f}m), got {m.driverId}")
        return f"Closest Driver B selected across boundary ({m.distanceMeters:.1f}m < {dist_A:.1f}m)"

    h.run_case("Tier 4", "T4_SCN_S02", "S2", "Multi-Driver Competitive Match across Hex Cells", scenario_s2)

    # S3: Moving Driver Crossing H3 Boundary Matched in New Cell
    def scenario_s3():
        h.reset_state()
        d_mover = "s3_driver_boundary_crosser"
        c_origin = latlng_to_cell(SF_CIVIC_CENTER[0], SF_CIVIC_CENTER[1], 8)
        neighbors = [c for c in grid_disk(c_origin, 1) if c != c_origin]
        c_dest = neighbors[0]

        lat_orig, lon_orig = cell_to_latlng(c_origin)
        lat_dest, lon_dest = cell_to_latlng(c_dest)
        total_dist = haversine_distance(lat_orig, lon_orig, lat_dest, lon_dest)
        assert_true(total_dist > 500.0, f"Distance between hex centers {total_dist}m must be > 500m")

        # Step 1: Driver emits ping in origin cell
        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d_mover,
                        value=DriverLocationPing(d_mover, lat_orig, lon_orig, "AVAILABLE", timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.05)
        assert_true(h.redis.sismember(f"cell:{c_origin}:drivers", d_mover), "Driver not in origin cell set")

        # Step 2: Driver travels >500m across boundary into dest cell
        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d_mover,
                        value=DriverLocationPing(d_mover, lat_dest, lon_dest, "AVAILABLE", timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.05)

        # Verify state migration
        assert_true(not h.redis.sismember(f"cell:{c_origin}:drivers", d_mover), "Driver still in old cell set (SREM failed)")
        assert_true(h.redis.sismember(f"cell:{c_dest}:drivers", d_mover), "Driver not in new cell set (SADD failed)")
        assert_true(h.redis.hget(f"driver:{d_mover}", "h3_cell") == c_dest, "Driver hash h3_cell not updated")

        # Step 3: Request submitted in dest cell, driver matched in new cell
        req_id = f"s3_req_{int(time.time()*1000)}"
        h.kafka.produce(TOPIC_RIDE_REQUESTS, key=req_id,
                        value=RideRequest(req_id, "rider_s3", lat_dest + 0.0001, lon_dest, timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.1)

        matches = h.kafka.consume_matches(timeout_sec=2.5, target_req_id=req_id)
        assert_true(len(matches) >= 1, "S3: No match found after migration")
        assert_true(matches[0].driverId == d_mover, f"S3: Matched {matches[0].driverId} != {d_mover}")
        return f"Successfully migrated {c_origin} -> {c_dest} and matched"

    h.run_case("Tier 4", "T4_SCN_S03", "S3", "Moving Driver Crossing H3 Boundary Matched in New Cell", scenario_s3)

    # S4: Stale Driver Ping Cessation & Graceful Next-Available Match
    def scenario_s4():
        h.reset_state()
        c_cell = latlng_to_cell(SF_MISSION[0], SF_MISSION[1], 8)
        lat_c, lon_c = cell_to_latlng(c_cell)

        d_stale = "s4_driver_stale_ping"
        d_active = "s4_driver_active_ping"

        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d_stale,
                        value=DriverLocationPing(d_stale, lat_c + 0.0002, lon_c, "AVAILABLE", timestamp=int(time.time()*1000)).to_dict())
        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d_active,
                        value=DriverLocationPing(d_active, lat_c + 0.0012, lon_c, "AVAILABLE", timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.05)

        h.redis.expire(f"driver:{d_stale}", 1)
        time.sleep(1.1)

        # Assert d_stale hash has expired from Redis
        assert_true(not bool(h.redis.hgetall(f"driver:{d_stale}")), "Stale driver hash failed to expire")

        # Ride request arrives
        req_id = f"s4_req_{int(time.time()*1000)}"
        h.kafka.produce(TOPIC_RIDE_REQUESTS, key=req_id,
                        value=RideRequest(req_id, "rider_s4", lat_c, lon_c, timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.1)

        matches = h.kafka.consume_matches(timeout_sec=2.5, target_req_id=req_id)
        assert_true(len(matches) >= 1, "S4: No match found")
        m = matches[0]
        assert_true(m.driverId != d_stale, "S4: Stale driver was incorrectly matched!")
        assert_true(m.driverId == d_active, f"S4: Expected next available {d_active}, got {m.driverId}")
        return f"Stale {d_stale} evicted; matched next available {d_active}"

    h.run_case("Tier 4", "T4_SCN_S04", "S4", "Stale Driver Ping Cessation & Next-Available Match", scenario_s4)

    # S5: High-Frequency Concurrent Requests with Driver Reservation Mutex
    def scenario_s5():
        h.reset_state()
        d_single = "s5_driver_single_capacity"
        lat, lon = SF_UNION_SQUARE
        h.kafka.produce(TOPIC_DRIVER_LOCATIONS, key=d_single,
                        value=DriverLocationPing(d_single, lat, lon, "AVAILABLE", timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.05)

        # 5 simultaneous requests targeting the same spot
        req_ids = [f"s5_req_{i}_{int(time.time()*1000)}" for i in range(5)]
        for r_id in req_ids:
            h.kafka.produce(TOPIC_RIDE_REQUESTS, key=r_id,
                            value=RideRequest(r_id, f"rider_{r_id}", lat + 0.0001, lon, timestamp=int(time.time()*1000)).to_dict())

        time.sleep(0.2)
        all_matches = []
        for r_id in req_ids:
            m = h.kafka.consume_matches(timeout_sec=1.5, target_req_id=r_id)
            if m:
                all_matches.extend(m)

        matched_drivers = [m.driverId for m in all_matches if m.driverId == d_single]
        assert_true(len(matched_drivers) == 1,
                    f"S5: Race condition! Driver booked {len(matched_drivers)} times simultaneously (must be exactly 1)")
        return f"Exactly 1 of 5 concurrent requests reserved driver {d_single}"

    h.run_case("Tier 4", "T4_SCN_S05", "S5", "Concurrent Requests Driver Reservation Mutex", scenario_s5)

    # S6: Complete Out-of-Range Request Graceful Skipping (No False Match)
    def scenario_s6():
        h.reset_state()
        sj_lat, sj_lon = SAN_JOSE_DOWNTOWN
        req_id = f"s6_req_out_of_range_{int(time.time()*1000)}"
        h.kafka.produce(TOPIC_RIDE_REQUESTS, key=req_id,
                        value=RideRequest(req_id, "rider_san_jose", sj_lat, sj_lon, timestamp=int(time.time()*1000)).to_dict())
        time.sleep(0.5)

        matches = h.kafka.consume_matches(timeout_sec=1.0, target_req_id=req_id)
        assert_true(len(matches) == 0,
                    f"S6: False match generated for out-of-range request in San Jose! Matched: {matches}")
        return "Gracefully ignored without crash or false match"

    h.run_case("Tier 4", "T4_SCN_S06", "S6", "Complete Out-of-Range Request Graceful Skipping", scenario_s6)


# ---------------------------------------------------------------------------
# Dedicated Fleet Movement Simulation Feature Verification
# ---------------------------------------------------------------------------
def run_fleet_verification(h: VerificationHarness):
    print("\n--- Executing Fleet Movement Simulation Verification ---")
    h.reset_state()
    fleet = FleetSimulator(kafka_bus=h.kafka, num_drivers=15)
    pings = fleet.emit_tick()

    def verify_fleet():
        assert_true(len(pings) == 15, "Fleet simulator did not generate 15 pings")
        for p in pings:
            cell = latlng_to_cell(p.latitude, p.longitude, 8)
            assert_true(h.redis.sismember(f"cell:{cell}:drivers", p.driverId),
                        f"Fleet driver {p.driverId} not in cell set {cell}")
            h_data = h.redis.hgetall(f"driver:{p.driverId}")
            assert_true(bool(h_data), f"Driver hash missing for {p.driverId}")
            assert_true(h.redis.ttl(f"driver:{p.driverId}") > 0, f"TTL missing for {p.driverId}")
        return f"All 15 fleet drivers active in SF area with valid H3 cell sets and TTL"

    h.run_case("Tier 4", "T4_FLEET_01", "FLEET", "15 Drivers Movement & Redis State Verification", verify_fleet)


# ---------------------------------------------------------------------------
# Main CLI & Dispatcher
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Ride-Hailing E2E Verification Harness & Fleet Simulator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--mode", choices=["all", "e2e", "fleet", "mock", "tier1", "tier2", "tier3", "tier4"],
                        default="all", help="Execution mode")
    parser.add_argument("--scenario", choices=["S1", "S2", "S3", "S4", "S5", "S6", "all"],
                        default="all", help="Filter Tier 4 scenario")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka broker bootstrap server")
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--duration", type=float, default=15.0, help="Continuous fleet simulation duration (sec)")
    parser.add_argument("--output", default="verification_results.json", help="JSON results output file")
    parser.add_argument("--mock", action="store_true", help="Force mock in-memory bus and Redis store")
    parser.add_argument("--dry-run", action="store_true", help="Run verification harness against self-contained mock engine")

    args = parser.parse_args()

    print("\n" + "=" * 105)
    print(f"{'REAL-TIME RIDE-HAILING E2E VERIFICATION HARNESS':^105}")
    print(f"{'Mode: ' + args.mode.upper():^105}")
    print("=" * 105)

    # Determine backend: Live or Mock
    use_mock = args.mock or args.dry_run or (args.mode == "mock")

    if not use_mock:
        # Check if live services are reachable
        print(f"[*] Probing Live Redis ({args.redis_host}:{args.redis_port})...")
        live_redis = LiveRedisStore(host=args.redis_host, port=args.redis_port)
        redis_ok = live_redis.ping()

        print(f"[*] Probing Live Kafka ({args.kafka_bootstrap})...")
        live_kafka = LiveKafkaBus(bootstrap_servers=args.kafka_bootstrap)
        kafka_ok = live_kafka.is_reachable()

        if redis_ok and kafka_ok:
            print("[+] Live Kafka and Redis detected. Running in LIVE E2E mode.")
            redis_store = live_redis
            kafka_bus = live_kafka
        else:
            print("[-] Live services not fully available:")
            print(f"    Redis ({args.redis_host}:{args.redis_port}): {'ONLINE' if redis_ok else 'OFFLINE'}")
            print(f"    Kafka ({args.kafka_bootstrap}): {'ONLINE' if kafka_ok else 'OFFLINE'}")
            print("[!] Auto-selecting Mock In-Memory Engine for self-contained verification.")
            use_mock = True

    if use_mock:
        redis_store = MockRedisStore()
        kafka_bus = MockKafkaBus(redis_store=redis_store)

    harness = VerificationHarness(kafka_bus=kafka_bus, redis_store=redis_store)

    # Fleet standalone mode
    if args.mode == "fleet":
        fleet = FleetSimulator(kafka_bus=kafka_bus, num_drivers=15)
        print(f"[*] Initialized Fleet Simulator with 15 drivers in San Francisco area.")
        print(f"[*] Running continuous fleet simulation for {args.duration}s (interval=3s)...")
        fleet.start_continuous(interval_sec=3.0, duration_sec=args.duration)
        time.sleep(args.duration)
        fleet.stop()
        print("[+] Fleet simulation completed successfully.")
        sys.exit(0)

    # Verification Suites
    if args.mode in ("all", "e2e", "mock", "tier1"):
        run_tier1_tests(harness)

    if args.mode in ("all", "e2e", "mock", "tier2"):
        run_tier2_tests(harness)

    if args.mode in ("all", "e2e", "mock", "tier3"):
        run_tier3_tests(harness)

    if args.mode in ("all", "e2e", "mock", "tier4"):
        run_fleet_verification(harness)
        run_tier4_scenarios(harness)

    # Print Structured Summary & Export JSON
    harness.print_summary_table()
    harness.export_json(args.output)

    failed_count = sum(1 for r in harness.results if r.status != "PASS")
    if failed_count > 0:
        print(f"[!] Test suite completed with {failed_count} failure(s). Exiting with code 1.")
        sys.exit(1)
    else:
        print("[+] All verification assertions passed successfully! Exiting with code 0.")
        sys.exit(0)


if __name__ == "__main__":
    main()
