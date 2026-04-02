package com.marbl.eventsmash.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Serializzatore Kafka per PreEnrichedEvent.
 * Key: customerId — ordinamento per cliente sulla stessa partizione
 * Value: JSON completo con evento + profilo snapshot + pattern CEP + campi AI
 * Topic: events.enriched
 */
public class PreEnrichedEventSerializer
        implements KafkaRecordSerializationSchema<PreEnrichedEvent> {

    private static final String TOPIC = "events.enriched";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(PreEnrichedEvent event,
                                                     KafkaSinkContext context,
                                                     Long timestamp) {
        try {
            byte[] key   = event.getCustomerId() != null
                    ? event.getCustomerId().getBytes()
                    : null;
            byte[] value = MAPPER.writeValueAsBytes(event);
            return new ProducerRecord<>(TOPIC, key, value);
        } catch (Exception e) {
            throw new RuntimeException("Errore serializzazione PreEnrichedEvent", e);
        }
    }
}