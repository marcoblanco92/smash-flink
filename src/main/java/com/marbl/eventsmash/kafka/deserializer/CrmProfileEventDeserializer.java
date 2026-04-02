package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.source.CrmProfileEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Instant;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class CrmProfileEventDeserializer implements KafkaRecordDeserializationSchema<CrmProfileEvent> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CrmProfileEvent> out) throws IOException {

        JsonNode after = getDebeziumJsonNode(record);
        if (after == null) return;

        CrmProfileEvent event = new CrmProfileEvent();
        event.setProfileId(textOrNull(after.get("profile_id")));
        event.setCustomerId(textOrNull(after.get("customer_id")));
        event.setSegment(textOrNull(after.get("segment")));
        event.setProductsHeld(textOrNull(after.get("products_held")));

        JsonNode hasMortgageNode = after.get("has_mortgage");
        if (hasMortgageNode != null && !hasMortgageNode.isNull()) {
            event.setHasMortgage(hasMortgageNode.booleanValue());
        }

        JsonNode hasInvestmentsNode = after.get("has_investments");
        if (hasInvestmentsNode != null && !hasInvestmentsNode.isNull()) {
            event.setHasInvestments(hasInvestmentsNode.booleanValue());
        }

        JsonNode clvScoreNode = after.get("clv_score");
        if (clvScoreNode != null && !clvScoreNode.isNull()) {
            event.setClvScore(clvScoreNode.decimalValue());
        }

        JsonNode churnRiskNode = after.get("churn_risk_score");
        if (churnRiskNode != null && !churnRiskNode.isNull()) {
            event.setChurnRiskScore(churnRiskNode.decimalValue());
        }

        event.setRelationshipMgr(textOrNull(after.get("relationship_mgr")));

        JsonNode lastContactNode = after.get("last_contact_date");
        if (lastContactNode != null && !lastContactNode.isNull()) {
            event.setLastContactDate(lastContactNode.intValue());
        }

        event.setPreferredChannel(textOrNull(after.get("preferred_channel")));

        JsonNode pushOptInNode = after.get("push_opt_in");
        if (pushOptInNode != null && !pushOptInNode.isNull()) {
            event.setPushOptIn(pushOptInNode.booleanValue());
        }

        JsonNode avgSessionNode = after.get("avg_session_duration_30d");
        if (avgSessionNode != null && !avgSessionNode.isNull()) {
            event.setAvgSessionDuration30d(avgSessionNode.intValue());
        }

        JsonNode pushIgnoreNode = after.get("push_ignore_streak");
        if (pushIgnoreNode != null && !pushIgnoreNode.isNull()) {
            event.setPushIgnoreStreak(pushIgnoreNode.shortValue());
        }

        JsonNode daysSinceNode = after.get("days_since_last_contact");
        if (daysSinceNode != null && !daysSinceNode.isNull()) {
            event.setDaysSinceLastContact(daysSinceNode.intValue());
        }

        JsonNode productUsageNode = after.get("product_usage_score");
        if (productUsageNode != null && !productUsageNode.isNull()) {
            event.setProductUsageScore(productUsageNode.decimalValue());
        }

        JsonNode updatedAtNode = after.get("updated_at");
        if (updatedAtNode != null && !updatedAtNode.isNull()) {
            event.setUpdatedAt(Instant.parse(updatedAtNode.textValue()).toEpochMilli());
        }

        out.collect(event);
    }

    @Override
    public TypeInformation<CrmProfileEvent> getProducedType() {
        return TypeInformation.of(CrmProfileEvent.class);
    }
}