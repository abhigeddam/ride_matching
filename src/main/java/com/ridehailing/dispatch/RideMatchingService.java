package com.ridehailing.dispatch;

import com.ridehailing.model.RideMatch;
import com.ridehailing.model.RideRequest;
import com.ridehailing.util.JsonUtil;
import com.ridehailing.util.RedisPoolManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class RideMatchingService implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RideMatchingService.class);

    private final String kafkaBootstrap;
    private final String requestTopic;
    private final String matchTopic;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public RideMatchingService() {
        this(
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092",
            System.getenv("RIDE_REQUESTS_TOPIC") != null ? System.getenv("RIDE_REQUESTS_TOPIC") : "ride-requests",
            System.getenv("RIDE_MATCHES_TOPIC") != null ? System.getenv("RIDE_MATCHES_TOPIC") : "ride-matches"
        );
    }

    public RideMatchingService(String kafkaBootstrap, String requestTopic, String matchTopic) {
        this.kafkaBootstrap = kafkaBootstrap;
        this.requestTopic = requestTopic;
        this.matchTopic = matchTopic;
}
