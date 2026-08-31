package com.ridehailing.spatial;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class H3SpatialIndexTest {

    @Test
    public void testGeoToH3Address() {
        // San Francisco coordinates
        double lat = 37.7749;
        double lon = -122.4194;

        String cell = H3SpatialIndex.geoToH3Address(lat, lon);
        assertNotNull(cell);
        assertEquals(15, cell.length());
        assertTrue(cell.matches("^[0-9a-fA-F]+$"), "Cell address should be a valid hexadecimal string");
    }

    @Test
    public void testKRingNeighbors() {
        double lat = 37.7749;
        double lon = -122.4194;

        String centerCell = H3SpatialIndex.geoToH3Address(lat, lon);
        List<String> kRing1 = H3SpatialIndex.getKRing(centerCell, 1);

}
