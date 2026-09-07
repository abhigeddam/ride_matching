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
