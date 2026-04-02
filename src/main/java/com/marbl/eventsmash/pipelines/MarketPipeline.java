package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.MarketContextUpdateFunction;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.source.MarketDataEvent;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;

public class MarketPipeline {

    public static DataStream<EnrichedEvent> build(DataStream<EnrichedEvent> transactionStream, DataStream<MarketDataEvent> marketDataStream) {

        BroadcastStream<MarketDataEvent> marketBroadcast = marketDataStream
                .broadcast(MarketContextUpdateFunction.MARKET_CONTEXT_DESCRIPTOR);


        return transactionStream
                .connect(marketBroadcast)
                .process(new MarketContextUpdateFunction())
                .setParallelism(12);

    }
}
