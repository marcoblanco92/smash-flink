package com.marbl.eventsmash.config.hazelcast;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.NearCacheConfig;

public class HazelcastConfig {

    private static final String DEFAULT_ADDRESS = "127.0.0.1:5701";
    private static final String CACHE_MAP_NAME  = "customer-baseline-cache";

    public ClientConfig config() {
        String address = System.getenv("HAZELCAST_ADDRESS");
        if (address == null || address.isBlank()) {
            address = DEFAULT_ADDRESS;
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig().addAddress(address);

        // Near-cache: mirror locale in-heap della IMap Hazelcast.
        // Nessun TTL — la baseline vive finché ClickHouse non pubblica
        // un aggiornamento (evento puntuale per cliente).
        // invalidateOnChange garantisce che ogni setAsync() da
        // CustomerBaselineUpdateFunction invalidi la copia locale
        // su tutti i TaskManager.
        NearCacheConfig nearCache = new NearCacheConfig(CACHE_MAP_NAME);
        nearCache.setTimeToLiveSeconds(0);     // nessun TTL
        nearCache.setMaxIdleSeconds(0);        // nessun idle eviction
        nearCache.setInvalidateOnChange(true); // invalida su ogni update dal cluster
        clientConfig.addNearCacheConfig(nearCache);

        return clientConfig;
    }
}