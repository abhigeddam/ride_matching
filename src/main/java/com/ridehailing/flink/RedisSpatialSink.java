package com.ridehailing.flink;

import com.ridehailing.model.DriverLocationPing;
import com.ridehailing.spatial.H3SpatialIndex;
import com.ridehailing.util.RedisPoolManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class RedisSpatialSink extends RichSinkFunction<DriverLocationPing> {
    private static final long serialVersionUID = 1L;
    private static final int DRIVER_TTL_SECONDS = 15;

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = RedisPoolManager.getResource();
    }

    @Override
    public void invoke(DriverLocationPing ping, Context context) throws Exception {
        if (ping == null || ping.getDriverId() == null) {
            return;
        }

        String driverId = ping.getDriverId();
        String newCell = H3SpatialIndex.geoToH3Address(ping.getLatitude(), ping.getLongitude());

        String driverKey = "driver:" + driverId;
        String oldCell = jedis.hget(driverKey, "h3_cell");

        // Cell transition: if moved from old cell, remove from old cell's active driver set
        if (oldCell != null && !oldCell.equals(newCell)) {
            jedis.srem("cell:" + oldCell + ":drivers", driverId);
        }

        // Add to new cell's active driver set
        jedis.sadd("cell:" + newCell + ":drivers", driverId);

        // Update driver state hash
        Map<String, String> driverData = new HashMap<>();
        driverData.put("driverId", driverId);
        driverData.put("latitude", String.valueOf(ping.getLatitude()));
        driverData.put("longitude", String.valueOf(ping.getLongitude()));
        driverData.put("status", ping.getStatus() != null ? ping.getStatus() : "AVAILABLE");
        driverData.put("bearing", String.valueOf(ping.getBearing()));
        driverData.put("h3_cell", newCell);
        driverData.put("last_ping", String.valueOf(ping.getTimestamp()));

        jedis.hset(driverKey, driverData);
        jedis.expire(driverKey, DRIVER_TTL_SECONDS);
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
            jedis = null;
        }
        super.close();
    }
}
