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
