package com.marbl.eventsmash.kafka.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.source.LoanEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

import static com.marbl.eventsmash.utils.JsonUtils.*;

public class LoanEventDeserializer implements KafkaRecordDeserializationSchema<LoanEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<LoanEvent> out) throws IOException {

        JsonNode after = getDebeziumJsonNode(record);
        if (after == null) return;

        LoanEvent event = new LoanEvent();
        event.setLoanId(textOrNull(after.get("loan_id")));
        event.setCustomerId(textOrNull(after.get("customer_id")));
        event.setLoanType(textOrNull(after.get("loan_type")));

        JsonNode principalNode = after.get("principal_amount");
        if (principalNode != null && !principalNode.isNull()) {
            event.setPrincipalAmount(principalNode.decimalValue());
        }

        JsonNode outstandingNode = after.get("outstanding_balance");
        if (outstandingNode != null && !outstandingNode.isNull()) {
            event.setOutstandingBalance(outstandingNode.decimalValue());
        }

        JsonNode interestNode = after.get("interest_rate");
        if (interestNode != null && !interestNode.isNull()) {
            event.setInterestRate(interestNode.decimalValue());
        }

        JsonNode startDateNode = after.get("start_date");
        if (startDateNode != null && !startDateNode.isNull()) {
            event.setStartDate(startDateNode.intValue());
        }

        JsonNode maturityDateNode = after.get("maturity_date");
        if (maturityDateNode != null && !maturityDateNode.isNull()) {
            event.setMaturityDate(maturityDateNode.intValue());
        }

        JsonNode nextDueNode = after.get("next_due_date");
        if (nextDueNode != null && !nextDueNode.isNull()) {
            event.setNextDueDate(nextDueNode.intValue());
        }

        JsonNode daysPastDueNode = after.get("days_past_due");
        if (daysPastDueNode != null && !daysPastDueNode.isNull()) {
            event.setDaysPastDue(daysPastDueNode.intValue());
        }

        JsonNode creditUsageNode = after.get("credit_line_usage_pct");
        if (creditUsageNode != null && !creditUsageNode.isNull()) {
            event.setCreditLineUsagePct(creditUsageNode.decimalValue());
        }

        JsonNode avgDelayNode = after.get("avg_payment_delay_days");
        if (avgDelayNode != null && !avgDelayNode.isNull()) {
            event.setAvgPaymentDelayDays(avgDelayNode.intValue());
        }

        event.setStatus(textOrNull(after.get("status")));
        event.setCollateralType(textOrNull(after.get("collateral_type")));

        JsonNode updatedAtNode = after.get("updated_at");
        if (updatedAtNode != null && !updatedAtNode.isNull()) {
            event.setUpdatedAt(updatedAtNode.longValue());
        }

        out.collect(event);
    }

    @Override
    public TypeInformation<LoanEvent> getProducedType() {
        return TypeInformation.of(LoanEvent.class);
    }
}