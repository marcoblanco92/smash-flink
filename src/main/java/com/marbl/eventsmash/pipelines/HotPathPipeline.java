package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.BaselineEnrichmentFunction;
import com.marbl.eventsmash.functions.CustomerProfileFunction;

import com.marbl.eventsmash.functions.MarketContextUpdateFunction;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.enrich.EnrichedEventWithBaseline;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import com.marbl.eventsmash.model.source.AppEvent;
import com.marbl.eventsmash.model.source.MarketDataEvent;
import com.marbl.eventsmash.model.source.TransactionEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.concurrent.TimeUnit;

public class HotPathPipeline {

    /**
     * Hot path — union transactions + app_events → broadcast market
     *            → AsyncIO baseline lookup → CustomerProfileFunction.
     *
     * Step AsyncIO:
     * - unorderedWait: massima throughput, nessun ordinamento forzato
     * - timeout 100ms: se Hazelcast non risponde, forwarda senza baseline
     * - capacity 1000: fino a 1000 richieste Hazelcast in-flight contemporaneamente
     *
     * CustomerProfileFunction non tocca più Hazelcast:
     * riceve già la baseline nel payload e aggiorna solo RocksDB.
     */
    public static DataStream<PreEnrichedEvent> build(
            DataStream<TransactionEvent> transactionStream,
            DataStream<AppEvent> appEventStream,
            DataStream<MarketDataEvent> marketDataStream
    ) {
        // ── 1. Watermark ──────────────────────────────────────────────────────
        DataStream<TransactionEvent> txnWithWatermark = transactionStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TransactionEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) ->
                                        event.getTransactionTimestamp() != null
                                                ? event.getTransactionTimestamp() : 0L)
                );

        DataStream<AppEvent> appWithWatermark = appEventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AppEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) ->
                                        event.getEventTimestamp() != null
                                                ? event.getEventTimestamp() : 0L)
                );

        // ── 2. Union ──────────────────────────────────────────────────────────
        DataStream<EnrichedEvent> hotStream = txnWithWatermark
                .map(EnrichedEvent::ofTransaction)
                .union(appWithWatermark.map(EnrichedEvent::ofApp));

        // ── 3. Broadcast MarketContext ────────────────────────────────────────
        BroadcastStream<MarketDataEvent> marketBroadcast = marketDataStream
                .broadcast(MarketContextUpdateFunction.MARKET_CONTEXT_DESCRIPTOR);

        DataStream<EnrichedEvent> withMarket = hotStream
                .connect(marketBroadcast)
                .process(new MarketContextUpdateFunction())
                .setParallelism(12);

        // ── 4. AsyncIO — baseline lookup da Hazelcast (non bloccante) ─────────
        // unorderedWait: ogni evento viene arricchito appena Hazelcast risponde
        // senza aspettare gli eventi precedenti — throughput massimo
        DataStream<EnrichedEventWithBaseline> withBaseline = AsyncDataStream
                .unorderedWait(
                        withMarket,
                        new BaselineEnrichmentFunction(),
                        100, TimeUnit.MILLISECONDS,  // timeout per singola richiesta
                        1000                          // max richieste in-flight
                )
                .setParallelism(12);

        // ── 5. CustomerProfileFunction → RocksDB → emette < 300ms ────────────
        return withBaseline
                .keyBy(EnrichedEventWithBaseline::getCustomerId)
                .process(new CustomerProfileFunction())
                .setParallelism(12);
    }
}