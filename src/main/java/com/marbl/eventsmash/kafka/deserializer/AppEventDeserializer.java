package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.source.AppEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Instant;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class AppEventDeserializer implements KafkaRecordDeserializationSchema<AppEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<AppEvent> out) throws IOException {

        JsonNode after = getDebeziumJsonNode(record);
        if (after == null) return;

        AppEvent event = new AppEvent();
        event.setEventId(textOrNull(after.get("event_id")));
        event.setCustomerId(textOrNull(after.get("customer_id")));
        event.setEventType(textOrNull(after.get("event_type")));
        event.setScreenName(textOrNull(after.get("screen_name")));
        event.setSessionId(textOrNull(after.get("session_id")));

        JsonNode sessionDurationNode = after.get("session_duration_s");
        if (sessionDurationNode != null && !sessionDurationNode.isNull()) {
            event.setSessionDurationS(sessionDurationNode.intValue());
        }

        JsonNode eventTimestampNode = after.get("event_timestamp");
        if (eventTimestampNode != null && !eventTimestampNode.isNull()) {
            event.setEventTimestamp(Instant.parse(eventTimestampNode.textValue()).toEpochMilli());
        }

        event.setDeviceType(textOrNull(after.get("device_type")));

        JsonNode isPushOpenedNode = after.get("is_push_opened");
        if (isPushOpenedNode != null && !isPushOpenedNode.isNull()) {
            event.setIsPushOpened(isPushOpenedNode.booleanValue());
        }

        event.setFeatureCategory(textOrNull(after.get("feature_category")));

        JsonNode screensVisitedNode = after.get("screens_visited_n");
        if (screensVisitedNode != null && !screensVisitedNode.isNull()) {
            event.setScreensVisitedN(screensVisitedNode.shortValue());
        }

        JsonNode isReturnVisitNode = after.get("is_return_visit");
        if (isReturnVisitNode != null && !isReturnVisitNode.isNull()) {
            event.setIsReturnVisit(isReturnVisitNode.booleanValue());
        }

        out.collect(event);
    }

    @Override
    public TypeInformation<AppEvent> getProducedType() {
        return TypeInformation.of(AppEvent.class);
    }
}