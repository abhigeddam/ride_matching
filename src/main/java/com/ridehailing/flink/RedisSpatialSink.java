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
