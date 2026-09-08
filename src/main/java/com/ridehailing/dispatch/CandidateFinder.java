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
}
