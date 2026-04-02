package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.source.CardEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class CardEventDeserializer implements KafkaRecordDeserializationSchema<CardEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CardEvent> out) throws IOException {

        JsonNode after = getDebeziumJsonNode(record);
        if (after == null) return;

        CardEvent event = new CardEvent();

        event.setCardId(textOrNull(after.get("card_id")));
        event.setCustomerId(textOrNull(after.get("customer_id")));
        event.setAccountId(textOrNull(after.get("account_id")));
        event.setCardType(textOrNull(after.get("card_type")));
        event.setCardToken(textOrNull(after.get("card_token")));  // già mascherato da SMT

        // plafond_limit — null per debit e prepaid
        JsonNode plafondLimitNode = after.get("plafond_limit");
        if (plafondLimitNode != null && !plafondLimitNode.isNull()) {
            event.setPlafondLimit(new BigDecimal(plafondLimitNode.asText()));
        }

        // plafond_used — sempre presente, default 0
        JsonNode plafondUsedNode = after.get("plafond_used");
        if (plafondUsedNode != null && !plafondUsedNode.isNull()) {
            event.setPlafondUsed(new BigDecimal(plafondUsedNode.asText()));
        }

        // billing_cycle_day — null per debit e prepaid
        JsonNode billingCycleNode = after.get("billing_cycle_day");
        if (billingCycleNode != null && !billingCycleNode.isNull()) {
            event.setBillingCycleDay(billingCycleNode.shortValue());
        }

        event.setStatus(textOrNull(after.get("status")));

        // issued_date — epoch days (Debezium serializza DATE come int)
        JsonNode issuedDateNode = after.get("issued_date");
        if (issuedDateNode != null && !issuedDateNode.isNull()) {
            event.setIssuedDate(issuedDateNode.intValue());
        }

        // expiry_date — epoch days, nullable
        JsonNode expiryDateNode = after.get("expiry_date");
        if (expiryDateNode != null && !expiryDateNode.isNull()) {
            event.setExpiryDate(expiryDateNode.intValue());
        }

        // updated_at — epoch millis (Debezium serializza TIMESTAMPTZ come long)
        JsonNode updatedAtNode = after.get("updated_at");
        if (updatedAtNode != null && !updatedAtNode.isNull()) {
            event.setUpdatedAt(Instant.parse(updatedAtNode.textValue()).toEpochMilli());
        }

        out.collect(event);
    }

    @Override
    public TypeInformation<CardEvent> getProducedType() {
        return TypeInformation.of(CardEvent.class);
    }
}