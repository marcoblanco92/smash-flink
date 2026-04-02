package com.marbl.eventsmash.functions;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.marbl.eventsmash.config.hazelcast.HazelcastConfig;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.enrich.EnrichedEventWithBaseline;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * RichAsyncFunction — recupera la baseline OLAP da Hazelcast in modo non bloccante.
 *
 * Estende RichAsyncFunction (non AsyncFunction) per avere open() e close()
 * chiamati correttamente da Flink sul lifecycle del TaskManager.
 */
public class BaselineEnrichmentFunction
        extends RichAsyncFunction<EnrichedEvent, EnrichedEventWithBaseline> {

    private static final Logger logger = LoggerFactory.getLogger(BaselineEnrichmentFunction.class);

    private transient HazelcastInstance hazelcastInstance;
    private transient IMap<String, CustomerBaseline> hazelcastCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.hazelcastInstance = HazelcastClient.newHazelcastClient(new HazelcastConfig().config());
        this.hazelcastCache    = hazelcastInstance.getMap("customer-baseline-cache");
        logger.info("[BaselineEnrichmentFunction] Hazelcast client inizializzato");
    }

    @Override
    public void asyncInvoke(EnrichedEvent event,
                            ResultFuture<EnrichedEventWithBaseline> resultFuture) {

        String customerId = event.getCustomerId();

        if (customerId == null) {
            resultFuture.complete(Collections.singletonList(
                    new EnrichedEventWithBaseline(event, null)
            ));
            return;
        }

        CompletableFuture<CustomerBaseline> future =
                (CompletableFuture<CustomerBaseline>) hazelcastCache.getAsync(customerId);

        future.whenComplete((baseline, throwable) -> {
            if (throwable != null) {
                logger.warn("[BaselineEnrichmentFunction] Hazelcast error per cust={}: {}",
                        customerId, throwable.getMessage());
                resultFuture.complete(Collections.singletonList(
                        new EnrichedEventWithBaseline(event, null)
                ));
            } else {
                resultFuture.complete(Collections.singletonList(
                        new EnrichedEventWithBaseline(event, baseline)
                ));
            }
        });
    }

    @Override
    public void timeout(EnrichedEvent event,
                        ResultFuture<EnrichedEventWithBaseline> resultFuture) {
        logger.warn("[BaselineEnrichmentFunction] Timeout Hazelcast per cust={}",
                event.getCustomerId());
        resultFuture.complete(Collections.singletonList(
                new EnrichedEventWithBaseline(event, null)
        ));
    }

    @Override
    public void close() throws Exception {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }
}