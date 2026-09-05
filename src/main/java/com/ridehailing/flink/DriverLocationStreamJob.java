package com.ridehailing.flink;

import com.ridehailing.model.DriverLocationPing;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverLocationStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(DriverLocationStreamJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting DriverLocationStreamJob (Apache Flink Streaming Pipeline)...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ?
                System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092";
        String topic = System.getenv("DRIVER_LOCATIONS_TOPIC") != null ?
                System.getenv("DRIVER_LOCATIONS_TOPIC") : "driver-locations";

        LOG.info("Configured Kafka Bootstrap: {}, Topic: {}", kafkaBootstrap, topic);
}
