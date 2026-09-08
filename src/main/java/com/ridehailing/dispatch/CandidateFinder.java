package com.ridehailing.dispatch;

import com.ridehailing.spatial.DistanceCalculator;
import com.ridehailing.spatial.H3SpatialIndex;
import redis.clients.jedis.Jedis;

import java.util.*;

public class CandidateFinder {

    public static class Candidate {
        private final String driverId;
        private final double latitude;
        private final double longitude;
        private final double distanceMeters;
        private final String cell;

        public Candidate(String driverId, double latitude, double longitude, double distanceMeters, String cell) {
            this.driverId = driverId;
            this.latitude = latitude;
            this.longitude = longitude;
            this.distanceMeters = distanceMeters;
            this.cell = cell;
        }

        public String getDriverId() {
            return driverId;
        }

        public double getLatitude() {
            return latitude;
        }

        public double getLongitude() {
            return longitude;
        }

        public double getDistanceMeters() {
            return distanceMeters;
        }

        public String getCell() {
            return cell;
        }
    }

    /**
     * Searches Redis for the nearest available driver across the pickup H3 cell and its 6 neighbors (k-ring 1).
     * Automatically evicts stale drivers whose hashes have expired.
     */
    public static Optional<Candidate> findNearestAvailableDriver(Jedis jedis, double pickupLat, double pickupLon) {
        String pickupCell = H3SpatialIndex.geoToH3Address(pickupLat, pickupLon);
        List<String> targetCells = H3SpatialIndex.getKRing(pickupCell, 1);

        Candidate bestCandidate = null;
        Set<String> checkedDriverIds = new HashSet<>();

        for (String cell : targetCells) {
            String cellKey = "cell:" + cell + ":drivers";
            Set<String> driverIds = jedis.smembers(cellKey);
            if (driverIds == null || driverIds.isEmpty()) {
                continue;
            }

            for (String driverId : driverIds) {
                if (!checkedDriverIds.add(driverId)) {
                    continue; // Already evaluated
                }

                String driverKey = "driver:" + driverId;
                Map<String, String> driverData = jedis.hgetAll(driverKey);

                // Check if driver hash has expired or is missing
                if (driverData == null || driverData.isEmpty()) {
                    // Stale eviction: clean up orphaned set reference
                    jedis.srem(cellKey, driverId);
                    continue;
                }

                String status = driverData.get("status");
                if (status == null || !"AVAILABLE".equalsIgnoreCase(status)) {
                    continue;
                }

                try {
                    double driverLat = Double.parseDouble(driverData.get("latitude"));
                    double driverLon = Double.parseDouble(driverData.get("longitude"));

                    double distance = DistanceCalculator.haversineMeters(pickupLat, pickupLon, driverLat, driverLon);

                    if (bestCandidate == null || distance < bestCandidate.getDistanceMeters()) {
                        bestCandidate = new Candidate(driverId, driverLat, driverLon, distance, cell);
                    }
                } catch (NumberFormatException ignored) {}
            }
        }

        return Optional.ofNullable(bestCandidate);
    }
}
