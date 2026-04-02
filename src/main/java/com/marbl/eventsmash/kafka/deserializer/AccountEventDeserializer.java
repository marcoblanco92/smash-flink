package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.source.AccountEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class AccountEventDeserializer implements KafkaRecordDeserializationSchema<AccountEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<AccountEvent> out) throws IOException {

        JsonNode after = getDebeziumJsonNode(record);
        if (after == null) return;

        AccountEvent event = new AccountEvent();
        event.setAccountId(textOrNull(after.get("account_id")));
        event.setCustomerId(textOrNull(after.get("customer_id")));
        event.setAccountType(textOrNull(after.get("account_type")));
        event.setIbanToken(textOrNull(after.get("iban")));
        event.setCurrency(textOrDefault(after.get("currency"), "EUR"));

        JsonNode balanceNode = after.get("current_balance");
        if (balanceNode != null && !balanceNode.isNull()) {
            event.setCurrentBalance(balanceNode.decimalValue());
        }

        JsonNode openedDateNode = after.get("opened_date");
        if (openedDateNode != null && !openedDateNode.isNull()) {
            event.setOpenedDate(openedDateNode.intValue());
        }

        event.setStatus(textOrNull(after.get("status")));

        JsonNode overdraftNode = after.get("overdraft_limit");
        if (overdraftNode != null && !overdraftNode.isNull()) {
            event.setOverdraftLimit(overdraftNode.decimalValue());
        }

        JsonNode updatedAtNode = after.get("updated_at");
        if (updatedAtNode != null && !updatedAtNode.isNull()) {
            event.setUpdatedAt(updatedAtNode.longValue());
        }

        out.collect(event);
    }

    @Override
    public TypeInformation<AccountEvent> getProducedType() {
        return TypeInformation.of(AccountEvent.class);
    }
}