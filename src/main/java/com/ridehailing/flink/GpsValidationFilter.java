package com.ridehailing.flink;

import com.ridehailing.model.DriverLocationPing;
import org.apache.flink.api.common.functions.FilterFunction;

public class GpsValidationFilter implements FilterFunction<DriverLocationPing> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(DriverLocationPing ping) {
        if (ping == null) {
            return false;
        }
        if (ping.getDriverId() == null || ping.getDriverId().trim().isEmpty()) {
            return false;
        }
        double lat = ping.getLatitude();
        double lon = ping.getLongitude();

        if (lat < -90.0 || lat > 90.0) {
            return false;
        }
        if (lon < -180.0 || lon > 180.0) {
            return false;
        }
}
