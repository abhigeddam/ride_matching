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

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public double getBearing() {
        return bearing;
    }

    public void setBearing(double bearing) {
        this.bearing = bearing;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DriverLocationPing that = (DriverLocationPing) o;
        return Double.compare(that.latitude, latitude) == 0 &&
                Double.compare(that.longitude, longitude) == 0 &&
                Double.compare(that.bearing, bearing) == 0 &&
                timestamp == that.timestamp &&
                Objects.equals(driverId, that.driverId) &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(driverId, latitude, longitude, status, bearing, timestamp);
    }

    @Override
    public String toString() {
        return "DriverLocationPing{" +
                "driverId='" + driverId + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", status='" + status + '\'' +
                ", bearing=" + bearing +
                ", timestamp=" + timestamp +
                '}';
    }
}
