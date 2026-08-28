package com.ridehailing.spatial;

public class DistanceCalculator {
    private static final double EARTH_RADIUS_METERS = 6371000.0;

    /**
     * Computes Haversine distance in meters between two lat/lon points.
     */
    public static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double radLat1 = Math.toRadians(lat1);
        double radLat2 = Math.toRadians(lat2);

    }
}
