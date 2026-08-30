package com.ridehailing.spatial;

import com.uber.h3core.H3Core;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class H3SpatialIndex implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final int DEFAULT_RESOLUTION = 8; // ~460m edge length

    private static transient H3Core h3;

    public static synchronized H3Core getH3Instance() {
        if (h3 == null) {
            try {
                h3 = H3Core.newInstance();
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize Uber H3Core native library", e);
            }
        }
        return h3;
    }

    public static String geoToH3Address(double lat, double lon) {
        return geoToH3Address(lat, lon, DEFAULT_RESOLUTION);
    }

}
