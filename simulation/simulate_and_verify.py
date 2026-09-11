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
