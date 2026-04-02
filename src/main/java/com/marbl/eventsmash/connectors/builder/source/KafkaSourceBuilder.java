package com.marbl.eventsmash.connectors.builder.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceBuilder<T> {

    private final StreamExecutionEnvironment env;
    private String bootstrapServers = "127.0.0.1:9092";
    private String topic;
    private String groupId;
    private int parallelism = 1;
    private OffsetsInitializer offsetsInitializer = OffsetsInitializer.earliest();
    private KafkaRecordDeserializationSchema<T> deserializer;
    private WatermarkStrategy<T> watermarkStrategy = WatermarkStrategy.noWatermarks();


    private KafkaSourceBuilder(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public static <T> KafkaSourceBuilder<T> of(StreamExecutionEnvironment env) {
        return new KafkaSourceBuilder<>(env);
    }

    public KafkaSourceBuilder<T> topic(String topic) {
        this.topic = topic;
        return this;
    }

    public KafkaSourceBuilder<T> groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public KafkaSourceBuilder<T> parallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public KafkaSourceBuilder<T> deserializer(KafkaRecordDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public KafkaSourceBuilder<T> fromLatest() {
        this.offsetsInitializer = OffsetsInitializer.latest();
        return this;
    }

    public KafkaSourceBuilder<T> bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public KafkaSourceBuilder<T> watermarkStrategy(WatermarkStrategy<T> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }

    public DataStream<T> build(String sourceName) {
        KafkaSource<T> source = KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(deserializer)
                .build();

        return env.fromSource(source, watermarkStrategy, sourceName)
                .setParallelism(parallelism);
    }
}