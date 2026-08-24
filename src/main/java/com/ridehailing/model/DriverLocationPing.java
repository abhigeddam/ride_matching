package com.ridehailing.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DriverLocationPing implements Serializable {
    private static final long serialVersionUID = 1L;

    private String driverId;
    private double latitude;
    private double longitude;
    private String status; // e.g. "AVAILABLE", "ON_TRIP", "OFFLINE"
    private double bearing;
    private long timestamp;

    public DriverLocationPing() {
    }

    public DriverLocationPing(String driverId, double latitude, double longitude, String status, double bearing, long timestamp) {
        this.driverId = driverId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.status = status;
        this.bearing = bearing;
        this.timestamp = timestamp;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public double getLatitude() {
        return latitude;
    }
}
