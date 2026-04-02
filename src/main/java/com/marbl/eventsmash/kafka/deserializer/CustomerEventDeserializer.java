package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.source.CustomerEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class CustomerEventDeserializer implements KafkaRecordDeserializationSchema<CustomerEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CustomerEvent> out) throws IOException {

        JsonNode after = getDebeziumJsonNode(record);
        if (after == null) return;

        CustomerEvent event = new CustomerEvent();
        event.setCustomerId(textOrNull(after.get("customer_id")));
        event.setSegment(textOrNull(after.get("segment")));
        event.setPatternType(textOrNull(after.get("pattern_type")));

        JsonNode triggerDateNode = after.get("pattern_trigger_date");
        if (triggerDateNode != null && !triggerDateNode.isNull()) {
            event.setPatternTriggerDate(triggerDateNode.intValue());
        }

        event.setActivePattern(textOrNull(after.get("active_pattern")));
        event.setRiskClass(textOrNull(after.get("risk_class")));

        JsonNode clvScoreNode = after.get("clv_score");
        if (clvScoreNode != null && !clvScoreNode.isNull()) {
            event.setClvScore(clvScoreNode.decimalValue());
        }

        event.setRelationshipMgr(textOrNull(after.get("relationship_mgr")));

        JsonNode onboardingDateNode = after.get("onboarding_date");
        if (onboardingDateNode != null && !onboardingDateNode.isNull()) {
            event.setOnboardingDate(onboardingDateNode.intValue());
        }

        JsonNode isActiveNode = after.get("is_active");
        if (isActiveNode != null && !isActiveNode.isNull()) {
            event.setIsActive(isActiveNode.booleanValue());
        }

        JsonNode createdAtNode = after.get("created_at");
        if (createdAtNode != null && !createdAtNode.isNull()) {
            event.setCreatedAt(createdAtNode.longValue());
        }

        out.collect(event);
    }

    @Override
    public TypeInformation<CustomerEvent> getProducedType() {
        return TypeInformation.of(CustomerEvent.class);
    }
}