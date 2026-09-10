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

