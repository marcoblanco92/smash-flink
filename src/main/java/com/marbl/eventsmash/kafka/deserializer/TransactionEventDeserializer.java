package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.source.TransactionEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class TransactionEventDeserializer implements KafkaRecordDeserializationSchema<TransactionEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TransactionEvent> out) throws IOException {

        JsonNode after = getDebeziumJsonNode(record);
        if (after == null) return;

        TransactionEvent event = new TransactionEvent();

        event.setTransactionId(textOrNull(after.get("transaction_id")));
        event.setAccountId(textOrNull(after.get("account_id")));
        event.setCustomerId(textOrNull(after.get("customer_id")));

        JsonNode amountNode = after.get("amount");
        if (amountNode != null && !amountNode.isNull()) {
            event.setAmount(amountNode.decimalValue());
        }

        event.setCurrency(textOrDefault(after.get("currency"), "EUR"));
        event.setMerchantCategory(textOrNull(after.get("merchant_category")));
        event.setChannel(textOrNull(after.get("channel")));
        event.setCounterpartToken(textOrNull(after.get("counterpart")));

        // card_id — null per wire/sepa_dd/instant
        event.setCardId(textOrNull(after.get("card_id")));

        JsonNode tsNode = after.get("transaction_date");
        if (tsNode != null && !tsNode.isNull()) {
            event.setTransactionTimestamp(Instant.parse(tsNode.textValue()).toEpochMilli());
        }

        JsonNode valueDateNode = after.get("value_date");
        if (valueDateNode != null && !valueDateNode.isNull()) {
            event.setValueDate(valueDateNode.intValue());
        }

        JsonNode recurringNode = after.get("is_recurring");
        if (recurringNode != null && !recurringNode.isNull()) {
            event.setIsRecurring(recurringNode.booleanValue());
        }

        event.setPatternPhase(textOrNull(after.get("pattern_phase")));

        out.collect(event);
    }

    @Override
    public TypeInformation<TransactionEvent> getProducedType() {
        return TypeInformation.of(TransactionEvent.class);
    }
}