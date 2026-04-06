package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.CardProfileState;
import com.marbl.eventsmash.model.CustomerProfile;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.enrich.EnrichedEventWithBaseline;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import com.marbl.eventsmash.model.source.*;
import com.marbl.eventsmash.model.update.ProfileUpdateEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * CustomerProfileFunction — unico owner dello stato RocksDB.
 *
 * processElement1: hot path (transazioni, app events)
 *   → riceve baseline aggiornata via AsyncIO da Hazelcast
 *   → aggiorna profilo RocksDB
 *   → valuta CEP → emette PreEnrichedEvent
 *
 * processElement2: aggiornamenti laterali (account, crm, loan, card, customer)
 *   → aggiorna solo i campi di propria competenza in RocksDB
 *   → NON emette eventi
 *   → NON gestisce baseline (gestita da CustomerPipeline → Hazelcast)
 */
public class CustomerProfileFunction
        extends KeyedCoProcessFunction<String, EnrichedEventWithBaseline, ProfileUpdateEvent, PreEnrichedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerProfileFunction.class);
    private static final long W7_INTERVAL_MS = 7L * 24 * 60 * 60 * 1000;

    private transient ValueState<CustomerProfile> profileState;

    @Override
    public void open(Configuration parameters) throws Exception {
        profileState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("customer-profile", CustomerProfile.class)
        );
    }

    // =========================================================================
    // processElement1 — HOT PATH
    // =========================================================================

    @Override
    public void processElement1(EnrichedEventWithBaseline input,
                                Context context,
                                Collector<PreEnrichedEvent> collector) throws Exception {

        String customerId = context.getCurrentKey();
        CustomerProfile profile = getOrCreate(customerId);

        // Timer w7
        if (profile.getW7ResetTs() == 0L) {
            long nextReset = context.timerService().currentProcessingTime() + W7_INTERVAL_MS;
            context.timerService().registerProcessingTimeTimer(nextReset);
            profile.setW7ResetTs(nextReset);
        }

        // Baseline da Hazelcast via AsyncIO — sempre aggiornata da CustomerPipeline
        CustomerBaseline baseline = input.getBaseline();
        boolean baselineUpdated = false;
        if (baseline != null && baseline.getComputedAt() > profile.getLastBaselineTs()) {
            profile.updateFromBaseline(baseline);
            baselineUpdated = true;
        }

        // Aggiornamento hot path
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
                                app.getSessionDurationS() != null ? app.getSessionDurationS() : 0,
                                app.getScreensVisitedN() != null ? app.getScreensVisitedN() : 1,
                                app.getEventTimestamp() != null ? app.getEventTimestamp() : 0L
                        );
                    }
                }
            }
        }

        profileState.update(profile);

        PreEnrichedEvent output = new PreEnrichedEvent();
        output.setEvent(event);
        output.setProfileSnapshot(profile);
        CepEvaluator.evaluate(output, profile, event);
        logProfile(profile, event, baselineUpdated, output);

        if (event != null) collector.collect(output);
    }

    // =========================================================================
    // processElement2 — AGGIORNAMENTI LATERALI
    // Gestisce: ACCOUNT, CRM, LOAN, CARD, CUSTOMER
    // NON gestisce BASELINE — delegato a CustomerPipeline → Hazelcast
    // =========================================================================

    @Override
    public void processElement2(ProfileUpdateEvent update,
                                Context context,
                                Collector<PreEnrichedEvent> collector) throws Exception {

        String customerId = context.getCurrentKey();
        CustomerProfile profile = getOrCreate(customerId);

        switch (update.getType()) {

            case ACCOUNT -> {
                AccountEvent e = update.getAccountEvent();
                if (e != null && "active".equals(e.getStatus())) {
                    profile.updateFromAccount(
                            e.getCurrentBalance() != null
                                    ? e.getCurrentBalance().doubleValue() : 0.0
                    );
                    logger.debug("[ACCOUNT] cust={} balance={}", customerId,
                            profile.getCurrentBalance());
                }
            }

            case CRM -> {
                CrmProfileEvent e = update.getCrmProfileEvent();
                if (e != null) {
                    profile.updateFromCrm(
                            e.getSegment(),
                            e.getHasMortgage()    != null && e.getHasMortgage(),
                            e.getHasInvestments() != null && e.getHasInvestments(),
                            e.getClvScore()       != null ? e.getClvScore().doubleValue() : 0.0,
                            e.getPushOptIn()      != null && e.getPushOptIn(),
                            profile.getActivePattern(), // activePattern da CUSTOMER, non da CRM
                            e.getRelationshipMgr() != null,
                            e.getAvgSessionDuration30d() != null ? e.getAvgSessionDuration30d() : 0,
                            e.getPushIgnoreStreak()      != null ? e.getPushIgnoreStreak()      : 0
                    );
                    logger.debug("[CRM] cust={} segment={} hasMortgage={}", customerId,
                            profile.getSegment(), profile.isHasMortgage());
                }
            }

            case LOAN -> {
                LoanEvent e = update.getLoanEvent();
                if (e != null) {
                    profile.updateFromLoan(
                            e.getCreditLineUsagePct() != null ? e.getCreditLineUsagePct().doubleValue() : 0.0,
                            e.getDaysPastDue()         != null ? e.getDaysPastDue()         : 0,
                            e.getAvgPaymentDelayDays() != null ? e.getAvgPaymentDelayDays() : 0
                    );
                    logger.debug("[LOAN] cust={} creditLine={}% dpd={}", customerId,
                            profile.getCreditLineUsagePct(), profile.getDaysPastDue());
                }
            }

            case CARD -> {
                CardEvent e = update.getCardEvent();
                if (e != null && e.getCardId() != null) {
                    CardProfileState cardState = profile.getCardProfiles()
                            .computeIfAbsent(e.getCardId(), id -> new CardProfileState());
                    cardState.setCustomerId(e.getCustomerId());
                    cardState.setCardToken(e.getCardToken());
                    cardState.setCardId(e.getCardId());
                    cardState.setCardType(e.getCardType());
                    cardState.setStatus(e.getStatus());
                    if (e.getPlafondLimit()    != null) cardState.setPlafondLimit(e.getPlafondLimit().doubleValue());
                    if (e.getPlafondUsed()     != null) cardState.setPlafondUsed(e.getPlafondUsed().doubleValue());
                    if (e.getBillingCycleDay() != null) cardState.setBillingCycleDay(e.getBillingCycleDay());
                    // Calcola plafondAvailable
                    cardState.setPlafondAvailable(cardState.getPlafondLimit() - cardState.getPlafondUsed());
                    logger.debug("[CARD] cust={} card={} plafond={}/{}", customerId,
                            e.getCardId(), cardState.getPlafondUsed(), cardState.getPlafondLimit());
                }
            }

            case CUSTOMER -> {
                CustomerEvent e = update.getCustomerEvent();
                if (e != null && Boolean.TRUE.equals(e.getIsActive())) {
                    profile.updateFromCustomer(
                            e.getSegment(),
                            e.getActivePattern() != null ? e.getActivePattern() : "ordinary",
                            e.getRiskClass(),
                            e.getClvScore() != null ? e.getClvScore().doubleValue() : 0.0
                    );
                    logger.debug("[CUSTOMER] cust={} activePattern={} segment={}", customerId,
                            profile.getActivePattern(), profile.getSegment());
                }
            }

            case BASELINE ->
                // La baseline è gestita da CustomerPipeline → Hazelcast → AsyncIO.
                // Se arriva qui è un errore di configurazione.
                    logger.warn("[CustomerProfileFunction] case BASELINE ricevuto in processElement2 " +
                            "— ignorato. Verifica che CustomerPipeline sia attiva in SmashJobFlink.");
        }

        profileState.update(profile);
    }

    // =========================================================================
    // onTimer — reset w7
    // =========================================================================

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<PreEnrichedEvent> collector) throws Exception {
        CustomerProfile profile = profileState.value();
        if (profile == null) return;
        profile.resetW7(timestamp);
        long nextReset = timestamp + W7_INTERVAL_MS;
        ctx.timerService().registerProcessingTimeTimer(nextReset);
        profile.setW7ResetTs(nextReset);
        profileState.update(profile);
        logger.info("[W7-TIMER] Reset w7 per cust={}", profile.getCustomerId());
    }

    // =========================================================================
    // Utility
    // =========================================================================

    private CustomerProfile getOrCreate(String customerId) throws Exception {
        CustomerProfile profile = profileState.value();
        if (profile == null) {
            profile = new CustomerProfile();
            profile.setCustomerId(customerId);
        }
        return profile;
    }

    private void logProfile(CustomerProfile p, EnrichedEvent event,
                            boolean baselineUpdated, PreEnrichedEvent output) {
        logger.info(
                """
                [PROFILE] cust={} | event={} | baselineUpdated={} | patterns={}
                  ── w7:    sum={} count={}
                  ── w30:   sum={} avg={} slope={}
                  ── w90:   sum={} slope={} buckets={}
                  ── w180:  sum={} slope={} buckets={}
                  ── w365:  sum={} count={}
                  ── balance: current={} 30dAgo={}
                  ── income={} | coldStart={}
                  ── mortgage={} | investments={} | pattern={}
                  ── mortgageViews7d={} | investViews7d={}
                  ── creditLine={}% | dpd={}
                """,
                p.getCustomerId(),
                event != null ? event.getEventType() : "UNKNOWN",
                baselineUpdated, output.getDetectedPatterns(),
                String.format("%.2f", p.getW7().getSumAmt()), p.getW7().getCount(),
                String.format("%.2f", p.getW30SumAmt()),
                String.format("%.2f", p.getW30AvgAmt()),
                String.format("%.2f", p.getW30WeeklySlope()),
                String.format("%.2f", p.getW90SumAmt()),
                String.format("%.2f", p.getW90MonthlySlope()),
                Arrays.toString(p.getW90MonthlySums()),
                String.format("%.2f", p.getW180SumAmt()),
                String.format("%.2f", p.getW180MonthlySlope()),
                Arrays.toString(p.getW180MonthlySums()),
                String.format("%.2f", p.getW365SumAmt()), p.getW365Count(),
                String.format("%.2f", p.getCurrentBalance()),
                String.format("%.2f", p.getBalance30dAgo()),
                String.format("%.2f", p.getEstimatedMonthlyIncome()),
                p.isColdStart(),
                p.isHasMortgage(), p.isHasInvestments(), p.getActivePattern(),
                p.getMortgageSimViews7d(), p.getInvestmentViews7d(),
                String.format("%.2f", p.getCreditLineUsagePct()), p.getDaysPastDue()
        );
    }
}