package com.marbl.eventsmash.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

public class JsonUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static String textOrNull(JsonNode node) {
        return (node != null && !node.isNull()) ? node.textValue() : null;
    }

    public static String textOrDefault(JsonNode node, String defaultValue) {
        return (node != null && !node.isNull()) ? node.textValue() : defaultValue;
    }

    public static @Nullable JsonNode getJsonNode(ConsumerRecord<byte[], byte[]> record) throws IOException {
        if (record.value() == null) return null;
        JsonNode root = mapper.readTree(record.value());
        if (root.isMissingNode() || root.isNull()) return null;
        return root;
    }

    public static @Nullable JsonNode getDebeziumJsonNode(ConsumerRecord<byte[], byte[]> record) throws IOException {
        JsonNode root = mapper.readTree(record.value());
        JsonNode after = root.path("after");

        if (after.isMissingNode() || after.isNull()) {
            return null;
        }
        return after;
    }
}
