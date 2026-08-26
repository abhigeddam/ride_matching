package com.ridehailing.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RideMatch implements Serializable {
    private static final long serialVersionUID = 1L;

    private String requestId;
    private String riderId;
    private String driverId;
    private double driverLat;
    private double driverLon;
    private double pickupLat;
    private double pickupLon;
    private double distanceMeters;
    private String status; // "OFFERED", "ACCEPTED", etc.
    private long matchedAt;

    public RideMatch() {
    }

    public RideMatch(String requestId, String riderId, String driverId, double driverLat, double driverLon,
                     double pickupLat, double pickupLon, double distanceMeters, String status, long matchedAt) {
        this.requestId = requestId;
        this.riderId = riderId;
        this.driverId = driverId;
        this.driverLat = driverLat;
        this.driverLon = driverLon;
        this.pickupLat = pickupLat;
        this.pickupLon = pickupLon;
        this.distanceMeters = distanceMeters;
        this.status = status;
        this.matchedAt = matchedAt;
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

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public double getDriverLat() {
        return driverLat;
    }

    public void setDriverLat(double driverLat) {
        this.driverLat = driverLat;
    }

    public double getDriverLon() {
        return driverLon;
    }

    public void setDriverLon(double driverLon) {
        this.driverLon = driverLon;
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

    public double getDistanceMeters() {
        return distanceMeters;
    }

    public void setDistanceMeters(double distanceMeters) {
        this.distanceMeters = distanceMeters;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

}
