package com.marbl.eventsmash.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Serializzatore Kafka per EnrichedEvent.
 * Key: customerId (per garantire ordinamento per cliente sulla stessa partizione)
 * Value: JSON dell'evento completo
 * Topic: events.enriched
 */
public class EnrichedEventSerializer
        implements KafkaRecordSerializationSchema<EnrichedEvent> {

    private static final String TOPIC = "events.enriched";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(EnrichedEvent event,
                                                     KafkaSinkContext context,
                                                     Long timestamp) {
        try {
            byte[] key   = event.getCustomerId() != null
                    ? event.getCustomerId().getBytes()
                    : null;
            byte[] value = MAPPER.writeValueAsBytes(event);
            return new ProducerRecord<>(TOPIC, key, value);
        } catch (Exception e) {
            throw new RuntimeException("Errore serializzazione EnrichedEvent", e);
        }
    }
}