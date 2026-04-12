package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marbl.eventsmash.model.CounterpartProfile;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class BaselineEventDeserializer implements KafkaRecordDeserializationSchema<CustomerBaseline> {

    private static final Logger logger = LoggerFactory.getLogger(BaselineEventDeserializer.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<CustomerBaseline> out) throws IOException {

        JsonNode node = getJsonNode(record);
        if (node == null) return;

        CustomerBaseline baseline = new CustomerBaseline();

        baseline.setCustomerId(textOrNull(node.get("customerId")));
        baseline.setComputedAt(node.path("computedAt").asLong());
        baseline.setColdStart(node.path("isColdStart").asInt() != 0);

        // ── w30 ──────────────────────────────────────────────
        baseline.setW30SumAmt(node.path("w30SumAmt").asDouble());
        baseline.setW30Count(node.path("w30Count").asInt());
        baseline.setW30AvgAmt(node.path("w30AvgAmt").asDouble());
        baseline.setW30MaxAmt(node.path("w30MaxAmt").asDouble());
        baseline.setW30MinAmt(node.path("w30MinAmt").asDouble());
        baseline.setW30StdDev(node.path("w30StdDev").asDouble());
        baseline.setW30WeeklySlope(node.path("w30WeeklySlope").asDouble());
        baseline.setW30WeeklySums(asDoubleArray(node.get("w30WeeklySums")));

        // ── w90 ──────────────────────────────────────────────
        baseline.setW90SumAmt(node.path("w90SumAmt").asDouble());
        baseline.setW90Count(node.path("w90Count").asInt());
        baseline.setW90AvgAmt(node.path("w90AvgAmt").asDouble());
        baseline.setW90MaxAmt(node.path("w90MaxAmt").asDouble());
        baseline.setW90MinAmt(node.path("w90MinAmt").asDouble());
        baseline.setW90StdDev(node.path("w90StdDev").asDouble());
        baseline.setW90MonthlySlope(node.path("w90MonthlySlope").asDouble());
        baseline.setW90MonthlySums(asDoubleArray(node.get("w90MonthlySums")));

        // ── w180 ─────────────────────────────────────────────
        baseline.setW180SumAmt(node.path("w180SumAmt").asDouble());
        baseline.setW180Count(node.path("w180Count").asInt());
        baseline.setW180MonthlySlope(node.path("w180MonthlySlope").asDouble());
        baseline.setW180MonthlySums(asDoubleArray(node.get("w180MonthlySums")));

        // ── w365 ─────────────────────────────────────────────
        baseline.setW365SumAmt(node.path("w365SumAmt").asDouble());
        baseline.setW365Count(node.path("w365Count").asInt());
        baseline.setW365AvgAmt(node.path("w365AvgAmt").asDouble());
        baseline.setW365MaxAmt(node.path("w365MaxAmt").asDouble());
        baseline.setW365MinAmt(node.path("w365MinAmt").asDouble());
        baseline.setW365StdDev(node.path("w365StdDev").asDouble());

        // ── Mappe qualitative ─────────────────────────────────
        baseline.setMerchantCatAmounts30d(asDoubleMap(node.get("merchantCatAmounts30d")));
        baseline.setMerchantCatCounts30d(asIntMap(node.get("merchantCatCounts30d")));
        baseline.setMerchantCatAvgAmounts90d(asDoubleMap(node.get("merchantCatAvgAmounts90d")));
        baseline.setChannelCounts30d(asIntMap(node.get("channelCounts30d")));

        baseline.setDistinctCounterparts30d(node.path("distinctCounterparts30d").asInt());
        baseline.setEstimatedMonthlyIncome(node.path("estimatedMonthlyIncome").asDouble());
        baseline.setBalance30dAgo(node.path("balance30dAgo").asDouble());

        // ── CounterpartProfiles ───────────────────────
        baseline.setCounterparts(asCounterpartMap(node.get("counterparts")));

        logger.info(
                "[BASELINE-DESERIALIZER] cust={} | computedAt={} | coldStart={} " +
                        "| w30Sum={} | w90Slope={} | w180Slope={} | w180Buckets={}",
                baseline.getCustomerId(), baseline.getComputedAt(), baseline.isColdStart(),
                baseline.getW30SumAmt(), baseline.getW90MonthlySlope(),
                baseline.getW180MonthlySlope(),
                Arrays.toString(baseline.getW180MonthlySums())
        );

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
            mapNode.fields().forEachRemaining(e ->
                    map.put(e.getKey(), e.getValue().asDouble()));
        }
        return map;
    }

    private Map<String, Integer> asIntMap(JsonNode mapNode) {
        Map<String, Integer> map = new HashMap<>();
        if (mapNode != null && mapNode.isObject()) {
            mapNode.fields().forEachRemaining(e ->
                    map.put(e.getKey(), e.getValue().asInt()));
        }
        return map;
    }

    private Map<String, CounterpartProfile> asCounterpartMap(JsonNode mapNode) {
        Map<String, CounterpartProfile> result = new HashMap<>();
        if (mapNode == null || !mapNode.isObject()) return result;

        mapNode.fields().forEachRemaining(entry -> {
            JsonNode cp = entry.getValue();

            CounterpartProfile profile = new CounterpartProfile();
            profile.setCounterpartToken(cp.path("counterpartToken").asText(""));
            profile.setDirection(cp.path("direction").asText("OUTBOUND"));
            profile.setRecurring(cp.path("isRecurring").asBoolean(false));
            profile.setSubscription(cp.path("isSubscription").asBoolean(false));
            profile.setPaymentCount12m(cp.path("paymentCount12m").asInt(0));
            profile.setSumAmount12m(cp.path("sumAmount12m").asDouble(0.0));
            profile.setAvgAmount12m(cp.path("avgAmount12m").asDouble(0.0));
            profile.setSumSquared12m(0.0);      // non serializzato — ricalcolato incrementalmente
            profile.setLastAmount(cp.path("lastAmount").asDouble(0.0));
            profile.setMinAmount12m(cp.path("minAmount12m").asDouble(0.0));
            profile.setMaxAmount12m(cp.path("maxAmount12m").asDouble(0.0));
            profile.setAvgIntervalDays(cp.path("avgIntervalDays").asDouble(0.0));
            profile.setStdIntervalDays(0.0);    // non serializzato
            profile.setExpectedNextDate(cp.path("expectedNextDate").asLong(0L));
            profile.setDaysOverdue(0);          // calcolato runtime dal Layer 4
            profile.setLastDate(cp.path("lastDate").asLong(0L));
            profile.setFirstSeenDate(cp.path("firstSeenDate").asLong(0L));
            profile.setMonthsActive(cp.path("monthsActive").asInt(0));
            profile.setConsecutiveMonths(0);    // non serializzato
            profile.setMerchantCategory(cp.path("merchantCategory").asText(""));
            profile.setLastUpdateTs(cp.path("lastUpdateTs").asLong(0L));

            result.put(entry.getKey(), profile);
        });

        return result;
    }
}