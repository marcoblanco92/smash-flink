package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class BaselineEventDeserializer implements KafkaRecordDeserializationSchema<CustomerBaseline> {

    private static final Logger logger = LoggerFactory.getLogger(BaselineEventDeserializer.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CustomerBaseline> out) throws IOException {

        JsonNode node = getJsonNode(record);
        if (node == null) return;

        CustomerBaseline baseline = new CustomerBaseline();

        baseline.setCustomerId(textOrNull(node.get("customerId")));
        baseline.setComputedAt(node.path("computedAt").asLong());
        baseline.setColdStart(node.path("isColdStart").asInt() != 0);

        // ── w30 ──
        baseline.setW30SumAmt(node.path("w30SumAmt").asDouble());
        baseline.setW30Count(node.path("w30Count").asInt());
        baseline.setW30AvgAmt(node.path("w30AvgAmt").asDouble());
        baseline.setW30MaxAmt(node.path("w30MaxAmt").asDouble());
        baseline.setW30StdDev(node.path("w30StdDev").asDouble());
        baseline.setW30WeeklySlope(node.path("w30WeeklySlope").asDouble());
        baseline.setW30WeeklySums(asDoubleArray(node.get("w30WeeklySums")));

        // ── w90 ──
        baseline.setW90SumAmt(node.path("w90SumAmt").asDouble());
        baseline.setW90Count(node.path("w90Count").asInt());
        baseline.setW90AvgAmt(node.path("w90AvgAmt").asDouble());
        baseline.setW90MaxAmt(node.path("w90MaxAmt").asDouble());
        baseline.setW90StdDev(node.path("w90StdDev").asDouble());
        baseline.setW90MonthlySlope(node.path("w90MonthlySlope").asDouble());
        baseline.setW90MonthlySums(asDoubleArray(node.get("w90MonthlySums")));

        // ── w365 ──
        baseline.setW365SumAmt(node.path("w365SumAmt").asDouble());
        baseline.setW365Count(node.path("w365Count").asInt());
        baseline.setW365AvgAmt(node.path("w365AvgAmt").asDouble());
        baseline.setW365MaxAmt(node.path("w365MaxAmt").asDouble());
        baseline.setW365StdDev(node.path("w365StdDev").asDouble());

        // ── Mappe ──
        baseline.setMerchantCatAmounts30d(asDoubleMap(node.get("merchantCatAmounts30d")));
        baseline.setMerchantCatCounts30d(asIntMap(node.get("merchantCatCounts30d")));
        baseline.setChannelCounts30d(asIntMap(node.get("channelCounts30d")));

        baseline.setDistinctCounterparts30d(node.path("distinctCounterparts30d").asInt());
        baseline.setEstimatedMonthlyIncome(node.path("estimatedMonthlyIncome").asDouble());
        baseline.setBalance30dAgo(node.path("balance30dAgo").asDouble());

        logger.info("[BASELINE-DESERIALIZER] Baseline deserializzata | cust={} | computedAt={} | w30Sum={}",
                baseline.getCustomerId(), baseline.getComputedAt(), baseline.getW30SumAmt());

        out.collect(baseline);
    }

    @Override
    public TypeInformation<CustomerBaseline> getProducedType() {
        return TypeInformation.of(CustomerBaseline.class);
    }

    private double[] asDoubleArray(JsonNode arrayNode) {
        if (arrayNode == null || !arrayNode.isArray()) return new double[0];
        double[] arr = new double[arrayNode.size()];
        for (int i = 0; i < arrayNode.size(); i++) {
            arr[i] = arrayNode.get(i).asDouble();
        }
        return arr;
    }

    private Map<String, Double> asDoubleMap(JsonNode mapNode) {
        Map<String, Double> map = new HashMap<>();
        if (mapNode != null && mapNode.isObject()) {
            mapNode.fields().forEachRemaining(entry -> map.put(entry.getKey(), entry.getValue().asDouble()));
        }
        return map;
    }

    private Map<String, Integer> asIntMap(JsonNode mapNode) {
        Map<String, Integer> map = new HashMap<>();
        if (mapNode != null && mapNode.isObject()) {
            mapNode.fields().forEachRemaining(entry -> map.put(entry.getKey(), entry.getValue().asInt()));
        }
        return map;
    }
}