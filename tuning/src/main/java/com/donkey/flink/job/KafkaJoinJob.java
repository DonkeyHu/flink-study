package com.donkey.flink.job;

import com.donkey.flink.entity.CurrencyRate;
import com.donkey.flink.entity.CurrentRateOrder;
import com.donkey.flink.entity.Order;
import com.donkey.flink.function.EnrichOrderCurrentRate;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * https://github.com/ververica/flink-sql-cookbook/blob/main/joins/03_kafka_join/03_kafka_join.md
 */
public class KafkaJoinJob {

    public static void main(String[] args) throws Exception {

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

        DataStreamSource<ObjectNode> orderStream = env.fromSource(orderSource, WatermarkStrategy.noWatermarks(), "order-stream");
        DataStreamSource<CurrencyRate> currencyRateStream = env.fromSource(currencyRateSource, WatermarkStrategy.noWatermarks(), "currencyRate-stream");

        // 2. Transformation
        KeyedStream<Order, String> orderKeyedStream = orderStream.map(node -> {
            String orderId = node.get("value").get("orderId").asText();
            String currencyCode = node.get("value").get("currencyCode").asText();
            Double amount = node.get("value").get("amount").asDouble();
            Long orderTimestamp = node.get("value").get("orderTimestamp").asLong();
            return new Order(orderId, currencyCode, amount, orderTimestamp);
        }).keyBy(Order::getCurrencyCode);

        SingleOutputStreamOperator<CurrentRateOrder> enrichedStream = orderKeyedStream.intervalJoin(currencyRateStream.keyBy(CurrencyRate::getCurrencyCode))
                .between(Time.seconds(-30), Time.seconds(5))
                .process(new EnrichOrderCurrentRate());

        // 3. sink
        AvroSerializationSchema<CurrentRateOrder> serializationSchema = ConfluentRegistryAvroSerializationSchema.forSpecific(CurrentRateOrder.class);
        KafkaSink<CurrentRateOrder> sink = KafkaSink.<CurrentRateOrder>builder()
                .setBootstrapServers("")
                .setRecordSerializer(KafkaRecordSerializationSchema.<CurrentRateOrder>builder()
                        .setTopic("")
                        .setValueSerializationSchema(serializationSchema)
                        .build())
                .build();

        enrichedStream.sinkTo(sink);

        env.execute("KafkaJoinJob");
    }

}
