package com.donkey.flink.job;

import com.donkey.flink.entity.CurrencyRate;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * https://github.com/ververica/flink-sql-cookbook/blob/main/joins/03_kafka_join/03_kafka_join.md
 */
public class KafkaJoinJob {

    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 1. Order stream
        KafkaSource<ObjectNode> orderSource = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("order-stream")
                .setGroupId("order-consumer-group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true)))
                .build();

        // Current rate stream
        ConfluentRegistryAvroDeserializationSchema<CurrencyRate> schema = ConfluentRegistryAvroDeserializationSchema.forSpecific(CurrencyRate.class, "");

        KafkaSource<CurrencyRate> currencyRateSource = KafkaSource.<CurrencyRate>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("order-stream")
                .setGroupId("order-consumer-group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(schema))
                .build();


    }

}
