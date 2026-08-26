package com.ridehailing.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RideRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String requestId;
    private String riderId;
    private double pickupLat;
    private double pickupLon;
    private long timestamp;

    public RideRequest() {
    }

    public RideRequest(String requestId, String riderId, double pickupLat, double pickupLon, long timestamp) {
        this.requestId = requestId;
        this.riderId = riderId;
        this.pickupLat = pickupLat;
        this.pickupLon = pickupLon;
        this.timestamp = timestamp;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getRiderId() {
        return riderId;
    }

    public void setRiderId(String riderId) {
        this.riderId = riderId;
    }

    public double getPickupLat() {
        return pickupLat;
    }

    public void setPickupLat(double pickupLat) {
        this.pickupLat = pickupLat;
    }

    public double getPickupLon() {
        return pickupLon;
    }

    public void setPickupLon(double pickupLon) {
        this.pickupLon = pickupLon;
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
        RideRequest that = (RideRequest) o;
        return Double.compare(that.pickupLat, pickupLat) == 0 &&
                Double.compare(that.pickupLon, pickupLon) == 0 &&
                timestamp == that.timestamp &&
                Objects.equals(requestId, that.requestId) &&
                Objects.equals(riderId, that.riderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, riderId, pickupLat, pickupLon, timestamp);
    }

    @Override
    public String toString() {
        return "RideRequest{" +
                "requestId='" + requestId + '\'' +
                ", riderId='" + riderId + '\'' +
                ", pickupLat=" + pickupLat +
                ", pickupLon=" + pickupLon +
                ", timestamp=" + timestamp +
                '}';
    }
}
