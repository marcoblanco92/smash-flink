package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.BaselineEnrichmentFunction;
import com.marbl.eventsmash.functions.CustomerProfileFunction;
import com.marbl.eventsmash.functions.MarketContextUpdateFunction;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.enrich.EnrichedEventWithBaseline;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import com.marbl.eventsmash.model.source.*;
import com.marbl.eventsmash.model.update.ProfileUpdateEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.concurrent.TimeUnit;

public class HotPathPipeline {

    /**
     * Hot path — 8 stream in ingresso (no baseline: gestito da CustomerPipeline → Hazelcast).
     *
     * Flusso baseline:
     *   customer.baselines → CustomerPipeline → CustomerBaselineUpdateFunction → Hazelcast
     *   EnrichedEvent → BaselineEnrichmentFunction (AsyncIO) → legge da Hazelcast → RocksDB
     *
     * CustomerProfileFunction riceve la baseline aggiornata via AsyncIO ad ogni evento,
     * non più come stream laterale. Hazelcast è la fonte autoritativa per le baseline.
     */
    public static DataStream<PreEnrichedEvent> build(
            DataStream<TransactionEvent>  transactionStream,
            DataStream<AppEvent>          appEventStream,
            DataStream<MarketDataEvent>   marketDataStream,
            DataStream<AccountEvent>      accountStream,
            DataStream<CrmProfileEvent>   crmStream,
            DataStream<LoanEvent>         loanStream,
            DataStream<CardEvent>         cardStream,
            DataStream<CustomerEvent>     customerStream
    ) {
        // ── 1. Watermark ──────────────────────────────────────
        DataStream<TransactionEvent> txnWithWatermark = transactionStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TransactionEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((e, ts) ->
                                        e.getTransactionTimestamp() != null
                                                ? e.getTransactionTimestamp() : 0L));

        DataStream<AppEvent> appWithWatermark = appEventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AppEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((e, ts) ->
                                        e.getEventTimestamp() != null
                                                ? e.getEventTimestamp() : 0L));

        // ── 2. Union hot stream ───────────────────────────────
        DataStream<EnrichedEvent> hotStream = txnWithWatermark
                .map(EnrichedEvent::ofTransaction)
                .union(appWithWatermark.map(EnrichedEvent::ofApp));

        // ── 3. Broadcast MarketContext ────────────────────────
        BroadcastStream<MarketDataEvent> marketBroadcast = marketDataStream
                .broadcast(MarketContextUpdateFunction.MARKET_CONTEXT_DESCRIPTOR);

        DataStream<EnrichedEvent> withMarket = hotStream
                .connect(marketBroadcast)
                .process(new MarketContextUpdateFunction())
                .setParallelism(12);

        // ── 4. AsyncIO — baseline lookup da Hazelcast ─────────
        DataStream<EnrichedEventWithBaseline> withBaseline = AsyncDataStream
                .unorderedWait(withMarket, new BaselineEnrichmentFunction(),
                        100, TimeUnit.MILLISECONDS, 1000)
                .setParallelism(12);

        // ── 5. Union stream laterali → ProfileUpdateEvent ─────
        // Baseline escluso: entra nel profilo via AsyncIO (Hazelcast),
        // non più come stream separato in processElement2.
        DataStream<ProfileUpdateEvent> allUpdates =
                accountStream .map(ProfileUpdateEvent::fromAccount) .name("Account → ProfileUpdateEvent")
                        .union(crmStream     .map(ProfileUpdateEvent::fromCrm)     .name("CRM → ProfileUpdateEvent"))
                        .union(loanStream    .map(ProfileUpdateEvent::fromLoan)    .name("Loan → ProfileUpdateEvent"))
                        .union(cardStream    .map(ProfileUpdateEvent::fromCard)    .name("Card → ProfileUpdateEvent"))
                        .union(customerStream.map(ProfileUpdateEvent::fromCustomer).name("Customer → ProfileUpdateEvent"));

        // ── 6. CustomerProfileFunction — unico owner RocksDB ──
        return withBaseline
                .keyBy(EnrichedEventWithBaseline::getCustomerId)
                .connect(allUpdates.keyBy(ProfileUpdateEvent::getCustomerId))
                .process(new CustomerProfileFunction())
                .setParallelism(12);
    }
}