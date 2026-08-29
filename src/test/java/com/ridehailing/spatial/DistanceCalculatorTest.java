package com.ridehailing.spatial;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DistanceCalculatorTest {

    @Test
    public void testSamePointDistanceIsZero() {
        double lat = 37.7749;
        double lon = -122.4194;
        double dist = DistanceCalculator.haversineMeters(lat, lon, lat, lon);
        assertEquals(0.0, dist, 0.001);
    }

    @Test
    public void testKnownDistance() {
        // SF Ferry Building to SF City Hall (~2.8 km)
        double lat1 = 37.7955;
        double lon1 = -122.3937;
        double lat2 = 37.7793;
        double lon2 = -122.4192;

        double dist = DistanceCalculator.haversineMeters(lat1, lon1, lat2, lon2);
        // Expected roughly ~2850 meters
        assertTrue(dist > 2700 && dist < 3000, "Distance should be ~2.85km but was " + dist);
    }
}
