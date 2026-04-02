package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.source.MarketDataEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class MarketDataEventDeserializer implements KafkaRecordDeserializationSchema<MarketDataEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<MarketDataEvent> out) throws IOException {

        JsonNode after = getDebeziumJsonNode(record);
        if (after == null) return;

        MarketDataEvent event = new MarketDataEvent();
        event.setRecordId(textOrNull(after.get("record_id")));
        event.setDataType(textOrNull(after.get("data_type")));
        event.setMetricName(textOrNull(after.get("metric_name")));

        JsonNode valueNode = after.get("value");
        if (valueNode != null && !valueNode.isNull()) {
            event.setValue(valueNode.decimalValue());
        }

        JsonNode prevValueNode = after.get("previous_value");
        if (prevValueNode != null && !prevValueNode.isNull()) {
            event.setPreviousValue(prevValueNode.decimalValue());
        }

        JsonNode recordedAtNode = after.get("recorded_at");
        if (recordedAtNode != null && !recordedAtNode.isNull()) {
            event.setRecordedAt(recordedAtNode.longValue());
        }

        event.setSource(textOrNull(after.get("source")));

        out.collect(event);
    }

    @Override
    public TypeInformation<MarketDataEvent> getProducedType() {
        return TypeInformation.of(MarketDataEvent.class);
    }
}