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
}
