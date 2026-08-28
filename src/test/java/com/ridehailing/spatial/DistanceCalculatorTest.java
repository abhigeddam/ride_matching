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
}
