package com.marbl.eventsmash.model.enrich;

import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Wrapper che trasporta l'evento arricchito insieme alla baseline OLAP
 * recuperata in modo asincrono da Hazelcast.
 *
 * Prodotto da BaselineEnrichmentFunction (AsyncIO).
 * Consumato da CustomerProfileFunction che non ha più bisogno di toccare Hazelcast.
 *
 * baseline può essere null in due casi:
 * - Cliente nuovo (cold start) — nessuna baseline in cache
 * - Hazelcast temporaneamente non disponibile — il profilo usa i valori esistenti in RocksDB
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrichedEventWithBaseline implements Serializable {

    private EnrichedEvent    event;
    private CustomerBaseline baseline;   // nullable

    public String getCustomerId() {
        return event != null ? event.getCustomerId() : null;
    }
}