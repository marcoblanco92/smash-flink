package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.CustomerBaselineUpdateFunction;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * CustomerPipeline — scrive SOLO su Hazelcast.
 *
 * Responsabilità unica: aggiornare la cache Hazelcast con ogni nuovo
 * baseline pubblicato da smash-batch su customer.baselines.
 *
 * Non tocca RocksDB. Non emette eventi downstream.
 * BaselineEnrichmentFunction legge da Hazelcast via AsyncIO.
 */
public class CustomerPipeline {

    public static void build(DataStream<CustomerBaseline> baselineStream) {
        baselineStream
                .keyBy(CustomerBaseline::getCustomerId)
                .process(new CustomerBaselineUpdateFunction())
                .name("CustomerPipeline → Hazelcast")
                .setParallelism(12);
    }
}