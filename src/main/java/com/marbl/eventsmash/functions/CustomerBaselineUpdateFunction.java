package com.marbl.eventsmash.functions;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.marbl.eventsmash.config.hazelcast.HazelcastConfig;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomerBaselineUpdateFunction
        extends KeyedProcessFunction<String, CustomerBaseline, Void> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerBaselineUpdateFunction.class);

    private transient HazelcastInstance hazelcastInstance;
    private transient IMap<String, CustomerBaseline> hazelcastCache;

    @Override
    public void open(Configuration parameters) {
        this.hazelcastInstance = HazelcastClient.newHazelcastClient(new HazelcastConfig().config());
        this.hazelcastCache    = hazelcastInstance.getMap("customer-baseline-cache");
        logger.info("[BASELINE-UPDATER] Hazelcast client inizializzato — map: customer-baseline-cache");
    }

    @Override
    public void processElement(CustomerBaseline baseline,
                               Context context,
                               Collector<Void> collector) {

        String customerId = context.getCurrentKey();

        logger.info(
                "[BASELINE-UPDATER] Ricevuta baseline per cust={} | computedAt={} | coldStart={} | w30Sum={} | w30Count={}",
                customerId,
                baseline.getComputedAt(),
                baseline.isColdStart(),
                baseline.getW30SumAmt(),
                baseline.getW30Count()
        );

        hazelcastCache.setAsync(customerId, baseline)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        logger.error("[BASELINE-UPDATER] Errore scrittura Hazelcast per cust={}: {}",
                                customerId, throwable.getMessage());
                    } else {
                        logger.info("[BASELINE-UPDATER] Baseline salvata in Hazelcast per cust={}",
                                customerId);
                    }
                });
    }

    @Override
    public void close() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }
}