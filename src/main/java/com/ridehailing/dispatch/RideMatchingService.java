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

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        LOG.info("Starting RideMatchingService. Kafka: {}, Request Topic: {}, Match Topic: {}",
                kafkaBootstrap, requestTopic, matchTopic);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "ride-matching-service-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(requestTopic));
            LOG.info("Subscribed to {}", requestTopic);

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    processRequest(record.value(), producer);
                }
            }
        } catch (Exception e) {
            LOG.error("RideMatchingService encountered an error", e);
        } finally {
            LOG.info("RideMatchingService stopped.");
        }
    }

    public void processRequest(String requestJson, KafkaProducer<String, String> producer) {
}
