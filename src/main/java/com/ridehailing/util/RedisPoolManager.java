package com.ridehailing.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class RedisPoolManager {
    private static volatile JedisPool pool;

    public static JedisPool getPool() {
        if (pool == null) {
            synchronized (RedisPoolManager.class) {
                if (pool == null) {
                    String host = System.getenv("REDIS_HOST") != null ? System.getenv("REDIS_HOST") : "localhost";
                    int port = 6379;
                    if (System.getenv("REDIS_PORT") != null) {
                        try {
                            port = Integer.parseInt(System.getenv("REDIS_PORT"));
                        } catch (NumberFormatException ignored) {}
                    }

                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(64);
                    config.setMaxIdle(16);
                    config.setMinIdle(4);
                    config.setTestOnBorrow(true);
                    config.setMaxWait(Duration.ofMillis(3000));

                    pool = new JedisPool(config, host, port, 5000);
                }
            }
        }
        return pool;
    }

    public static Jedis getResource() {
}
