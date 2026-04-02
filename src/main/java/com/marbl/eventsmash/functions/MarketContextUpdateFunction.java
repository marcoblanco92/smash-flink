package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.MarketContext;
import com.marbl.eventsmash.model.source.MarketDataEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class MarketContextUpdateFunction extends BroadcastProcessFunction<EnrichedEvent, MarketDataEvent, EnrichedEvent> {

    public static final MapStateDescriptor<String, MarketContext> MARKET_CONTEXT_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "market-context-state",
                    String.class,
                    MarketContext.class
            );


    @Override
    public void processElement(EnrichedEvent transactionEvent,
                               BroadcastProcessFunction<EnrichedEvent, MarketDataEvent, EnrichedEvent>.ReadOnlyContext readOnlyContext,
                               Collector<EnrichedEvent> collector) throws Exception {

        MarketContext ctx = readOnlyContext.getBroadcastState(MARKET_CONTEXT_DESCRIPTOR).get("current");

        if (ctx == null) {
            collector.collect(transactionEvent);
            return;
        }

        // Per ora passa invariata — il MarketContext verrà usato dal CEP
        collector.collect(transactionEvent);
    }

    @Override
    public void processBroadcastElement(MarketDataEvent marketDataEvent,
                                        BroadcastProcessFunction<EnrichedEvent, MarketDataEvent, EnrichedEvent>.Context context,
                                        Collector<EnrichedEvent> collector) throws Exception {

        BroadcastState<String, MarketContext> state = context.getBroadcastState(MARKET_CONTEXT_DESCRIPTOR);

        MarketContext ctx = state.get("current");
        if (ctx == null) {
            ctx = new MarketContext();
        }

        long incomingTs = marketDataEvent.getRecordedAt() != null ? marketDataEvent.getRecordedAt() : 0L;
        if (incomingTs <= ctx.getUpdatedAt()) {
            return;
        }

        double value    = marketDataEvent.getValue() != null ? marketDataEvent.getValue().doubleValue() : 0.0;
        double previous = marketDataEvent.getPreviousValue() != null ? marketDataEvent.getPreviousValue().doubleValue() : 0.0;
        double delta    = value - previous;

        switch (marketDataEvent.getMetricName()) {
            case "ecb_rate" -> {
                ctx.setEcbRate(value);
                ctx.setEcbRatePrevious(previous);
                ctx.setEcbRateDelta(delta);
                ctx.setEcbRateDirection(delta < 0 ? "falling" : delta > 0 ? "rising" : "stable");
            }
            case "btp_bund_spread" -> {
                ctx.setBtpBundSpread(value);
                ctx.setBtpBundSpreadPrevious(previous);
                ctx.setBtpBundSpreadDelta(delta);
            }
            case "irs_10y" -> {
                ctx.setIrs10y(value);
                ctx.setIrs10yPrevious(previous);
            }
            case "inflation_rate" -> {
                ctx.setInflationRate(value);
                ctx.setInflationRatePrevious(previous);
            }
        }

        ctx.setUpdatedAt(incomingTs);
        state.put("current", ctx);
    }
}
