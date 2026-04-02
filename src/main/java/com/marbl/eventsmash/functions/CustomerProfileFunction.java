package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.CustomerProfile;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.enrich.EnrichedEventWithBaseline;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import com.marbl.eventsmash.model.source.AppEvent;
import com.marbl.eventsmash.model.source.TransactionEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hot path — aggiorna CustomerProfile in RocksDB e valuta regole CEP.
 *
 * Emette PreEnrichedEvent verso events.enriched con:
 * - profileSnapshot: copia del profilo al momento dell'evento
 * - detectedPatterns: lista pattern/insight rilevati dal CEP
 *
 * Il Layer 4 riceve tutto il contesto necessario per ragionare.
 */
public class CustomerProfileFunction
        extends KeyedProcessFunction<String, EnrichedEventWithBaseline, PreEnrichedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerProfileFunction.class);

    private static final long W7_INTERVAL_MS = 7L * 24 * 60 * 60 * 1000;

    private transient ValueState<CustomerProfile> profileState;

    @Override
    public void open(Configuration parameters) throws Exception {
        profileState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("customer-profile", CustomerProfile.class)
        );
    }

    @Override
    public void processElement(EnrichedEventWithBaseline input,
                               Context context,
                               Collector<PreEnrichedEvent> collector) throws Exception {

        String customerId = context.getCurrentKey();

        // 1. Recupero profilo da RocksDB
        CustomerProfile profile = profileState.value();
        if (profile == null) {
            profile = new CustomerProfile();
            profile.setCustomerId(customerId);
        }

        // 2. Timer w7 — registra al primo evento
        if (profile.getW7ResetTs() == 0L) {
            long nextReset = context.timerService().currentProcessingTime() + W7_INTERVAL_MS;
            context.timerService().registerProcessingTimeTimer(nextReset);
            profile.setW7ResetTs(nextReset);
            logger.info("[W7-TIMER] Registrato per cust={} | scatta a {}", customerId, nextReset);
        }

        // 3. Aggiornamento baseline OLAP
        CustomerBaseline baseline = input.getBaseline();
        boolean baselineUpdated = false;
        if (baseline != null && baseline.getComputedAt() > profile.getLastBaselineTs()) {
            profile.updateFromBaseline(baseline);
            baselineUpdated = true;
        }

        // 4. Aggiornamento hot path
        EnrichedEvent event = input.getEvent();
        if (event != null && event.getEventType() != null) {
            switch (event.getEventType()) {
                case "TRANSACTION" -> {
                    TransactionEvent txn = event.getTransaction();
                    if (txn != null) {
                        profile.updateFromTransaction(
                                txn.getAmount() != null ? txn.getAmount().doubleValue() : 0.0,
                                txn.getMerchantCategory(),
                                txn.getChannel(),
                                txn.getCounterpartToken(),
                                txn.getTransactionTimestamp() != null ? txn.getTransactionTimestamp() : 0L
                        );
                    }
                }
                case "APP" -> {
                    AppEvent app = event.getAppEvent();
                    if (app != null) {
                        profile.updateFromAppEvent(
                                app.getScreenName(),
                                app.getFeatureCategory(),
                                app.getIsPushOpened() != null && app.getIsPushOpened(),
                                app.getScreensVisitedN() != null ? app.getScreensVisitedN() : 1,
                                app.getEventTimestamp() != null ? app.getEventTimestamp() : 0L
                        );
                    }
                }
            }
        }

        // 5. Persistenza RocksDB
        profileState.update(profile);

        // 6. Costruisci PreEnrichedEvent con snapshot profilo
        PreEnrichedEvent output = new PreEnrichedEvent();
        output.setEvent(event);
        output.setProfileSnapshot(profile);

        // 7. CEP — valuta pattern e insight
        CepEvaluator.evaluate(output, profile, event);

        // 8. Log debug
        logProfile(profile, event, baselineUpdated, output);

        // 9. Emetti verso events.enriched
        if (event != null) {
            collector.collect(output);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<PreEnrichedEvent> collector) throws Exception {
        CustomerProfile profile = profileState.value();
        if (profile == null) return;

        logger.info("[W7-TIMER] Reset w7 per cust={}", profile.getCustomerId());
        profile.resetW7(timestamp);

        long nextReset = timestamp + W7_INTERVAL_MS;
        ctx.timerService().registerProcessingTimeTimer(nextReset);
        profile.setW7ResetTs(nextReset);
        profileState.update(profile);
    }

    private void logProfile(CustomerProfile p, EnrichedEvent event,
                            boolean baselineUpdated, PreEnrichedEvent output) {
        logger.info(
                """
                [PROFILE] cust={} | event={} | baselineUpdated={} | patterns={}
                  ── w7:  sum={} count={}
                  ── w30: sum={} avg={} slope={}
                  ── w90: sum={} slope={}
                  ── w365: sum={} count={}
                  ── balance: current={} delta={}%
                  ── income: {}
                  ── coldStart={} | wireFromAppPct={}
                  ── mortgage={} | investments={} | pattern={}
                  ── mortgageViews7d={} | investViews7d={} | narrowing={}
                  ── creditLine={}% | dpd={}
                """,
                p.getCustomerId(),
                event != null ? event.getEventType() : "UNKNOWN",
                baselineUpdated,
                output.getDetectedPatterns(),
                String.format("%.2f", p.getW7().getSumAmt()), p.getW7().getCount(),
                String.format("%.2f", p.getW30SumAmt()),
                String.format("%.2f", p.getW30AvgAmt()),
                String.format("%.2f", p.getW30WeeklySlope()),
                String.format("%.2f", p.getW90SumAmt()),
                String.format("%.2f", p.getW90MonthlySlope()),
                String.format("%.2f", p.getW365SumAmt()), p.getW365Count(),
                String.format("%.2f", p.getCurrentBalance()),
                String.format("%.2f", p.balanceDelta30dPct()),
                String.format("%.2f", p.getEstimatedMonthlyIncome()),
                p.isColdStart(),
                String.format("%.2f", p.getWireFromAppPct30d()),
                p.isHasMortgage(), p.isHasInvestments(), p.getActivePattern(),
                p.getMortgageSimViews7d(), p.getInvestmentViews7d(),
                p.hasFeatureNarrowing(),
                String.format("%.2f", p.getCreditLineUsagePct()),
                p.getDaysPastDue()
        );
    }
}