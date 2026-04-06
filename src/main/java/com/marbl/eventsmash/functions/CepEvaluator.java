package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.CardProfileState;
import com.marbl.eventsmash.model.CustomerProfile;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import com.marbl.eventsmash.model.source.TransactionEvent;

import java.util.Map;

/**
 * CepEvaluator — valuta tutte le regole CEP sul CustomerProfile.
 *
 * Popola PreEnrichedEvent.detectedPatterns con i codici dei pattern/insight rilevati.
 * Lista vuota = transazione ordinaria → Reasoning Agent emette SILENT.
 *
 * Pattern predittivi (P-xx): segnali multi-settimana verso Banker Copilot / Risk Sentinel
 * Insight transazionali (I-xx): segnali puntuali verso cliente o banker
 *
 * Soglie configurabili — centralizzate in questa classe per semplicità di tuning.
 */
public class CepEvaluator {

    // ── Soglie Pattern Predittivi ─────────────────────────────
    private static final int    P01_MORTGAGE_SIM_VIEWS_MIN  = 2;
    private static final double P01_BALANCE_DELTA_MIN_PCT   = 30.0;

    private static final double P02_CREDIT_LINE_USAGE_MIN   = 70.0;
    private static final int    P02_DAYS_PAST_DUE_MIN       = 1;

    private static final int    P03_PUSH_IGNORE_STREAK_MIN  = 3;
    private static final double P03_SESSION_DROP_MIN_PCT    = 30.0;

    private static final int    P04_INVEST_VIEWS_MIN        = 2;
    private static final double P04_BALANCE_DELTA_MIN_PCT   = 40.0;

    private static final int    P05_WIRE_COUNT_MIN          = 3;

    // ── Soglie Insight CEP ────────────────────────────────────
    private static final double I07_CAT_INCREASE_PCT        = 20.0;  // +20% vs media
    private static final int    I07_MIN_TXN_COUNT           = 3;

    private static final double I09_CAT_INCREASE_PCT        = 15.0;
    private static final int    I09_MIN_TXN_COUNT           = 3;

    private static final double I10_CAT_DECREASE_PCT        = 15.0;
    private static final int    I10_MIN_TXN_COUNT           = 3;

    private static final double I12_INCOME_RATIO_MIN_PCT    = 30.0;  // categoria > 30% reddito

    private static final double I16_PLAFOND_USAGE_MIN_PCT   = 80.0;  // plafond > 80%
    private static final double I17_DAYS_REMAINING_MAX      = 4.0;   // blocco carta entro 4gg

    // ─────────────────────────────────────────────────────────

    private CepEvaluator() {}

    /**
     * Entry point — valuta tutte le regole e popola detectedPatterns.
     */
    public static void evaluate(PreEnrichedEvent output,
                                CustomerProfile profile,
                                EnrichedEvent event) {

        if (profile == null || profile.isColdStart()) return;

        // Pattern predittivi
        evaluateP01RealEstate(output, profile);
        evaluateP02PmiDeterioration(output, profile);
        evaluateP03PreChurn(output, profile);
        evaluateP04Investment(output, profile);
        evaluateP05WireAnomalo(output, profile, event);

        // Insight CEP (solo su TRANSACTION)
        if (event != null && "TRANSACTION".equals(event.getEventType())) {
            TransactionEvent txn = event.getTransaction();
            evaluateI07CategoryIncrease(output, profile, txn);
            evaluateI09CategoryAboveAvg(output, profile, txn);
            evaluateI10CategoryBelowAvg(output, profile, txn);
            evaluateI12IncomeRatio(output, profile, txn);
            evaluateI16PlafondHigh(output, profile);
            evaluateI17CardBlockRisk(output, profile);
        }
    }

    // ── P-01: Real Estate Intent ──────────────────────────────
    private static void evaluateP01RealEstate(PreEnrichedEvent output,
                                              CustomerProfile p) {
        if (p.isHasMortgage()) return;
        if (!p.getActivePattern().equals("ordinary") &&
                !p.getActivePattern().equals("real_estate")) return;

        boolean simViews   = p.getMortgageSimViews7d() >= P01_MORTGAGE_SIM_VIEWS_MIN;
        boolean balanceDelta = p.balanceDelta30dPct() >= P01_BALANCE_DELTA_MIN_PCT;

        if (simViews && balanceDelta) {
            output.addPattern("P-01");
        }
    }

    // ── P-02: PMI Deterioration ───────────────────────────────
    private static void evaluateP02PmiDeterioration(PreEnrichedEvent output,
                                                    CustomerProfile p) {
        if (!"pmi".equals(p.getSegment())) return;

        boolean creditLineStress = p.getCreditLineUsagePct() >= P02_CREDIT_LINE_USAGE_MIN;
        boolean dpd              = p.getDaysPastDue() >= P02_DAYS_PAST_DUE_MIN;
        boolean paymentDelay     = p.getAvgPaymentDelayDays() > 10;

        // fase 1: solo credit line stress (segnale precoce)
        if (creditLineStress && !dpd) {
            output.addPattern("P-02-PHASE1");
        }
        // fase 2: credit line + DPD o ritardo pagamenti
        if (creditLineStress && (dpd || paymentDelay)) {
            output.addPattern("P-02-PHASE2");
        }
    }

    // ── P-03: Pre-Churn ───────────────────────────────────────
    private static void evaluateP03PreChurn(PreEnrichedEvent output,
                                            CustomerProfile p) {
        boolean narrowing    = p.hasFeatureNarrowing();
        boolean pushIgnore   = p.getPushIgnoreStreak() >= P03_PUSH_IGNORE_STREAK_MIN;
        boolean sessionDrop  = p.getSessionDurationDeltaPct() <= -P03_SESSION_DROP_MIN_PCT;

        // fase 1: distanza emotiva (2 segnali su 3)
        int signals = (narrowing ? 1 : 0) + (pushIgnore ? 1 : 0) + (sessionDrop ? 1 : 0);
        if (signals >= 2) {
            output.addPattern("P-03-PHASE1");
        }
    }

    // ── P-04: Investment Opportunity ─────────────────────────
    private static void evaluateP04Investment(PreEnrichedEvent output,
                                              CustomerProfile p) {
        if (p.isHasInvestments()) return;
        if (!p.getActivePattern().equals("ordinary") &&
                !p.getActivePattern().equals("investment_opportunity")) return;

        boolean investViews  = p.getInvestmentViews7d() >= P04_INVEST_VIEWS_MIN;
        boolean balanceDelta = p.balanceDelta30dPct() >= P04_BALANCE_DELTA_MIN_PCT;

        if (investViews && balanceDelta) {
            output.addPattern("P-04");
        }
    }

    // ── P-05: Wire Anomalo ────────────────────────────────────
    private static void evaluateP05WireAnomalo(PreEnrichedEvent output,
                                               CustomerProfile p,
                                               EnrichedEvent event) {
        if (event == null || !"TRANSACTION".equals(event.getEventType())) return;
        TransactionEvent txn = event.getTransaction();
        if (txn == null) return;

        boolean isWire = "wire".equals(txn.getChannel()) ||
                "instant".equals(txn.getChannel());
        if (!isWire) return;

        if (p.getWireCount30d() >= P05_WIRE_COUNT_MIN) {
            long txnTs = txn.getTransactionTimestamp() != null
                    ? txn.getTransactionTimestamp() : 0L;
            if (p.isWireWithoutDigitalOrigin(txnTs)) {
                output.addPattern("P-05");
            }
        }
    }

    // ── I-07: Aumento spese categoria ────────────────────────
    private static void evaluateI07CategoryIncrease(PreEnrichedEvent output,
                                                    CustomerProfile p,
                                                    TransactionEvent txn) {
        if (txn == null || txn.getMerchantCategory() == null) return;
        String cat = txn.getMerchantCategory();
        // Escludi categorie non rilevanti per variazioni di spesa reale
        if (cat.equals("internal_transfer") || cat.equals("salary_income") ||
                cat.equals("b2b_transfer")  || cat.equals("investment")) return;

        Map<String, Double>  amounts    = p.getMerchantCatAmounts30d();
        Map<String, Integer> counts     = p.getMerchantCatCounts30d();
        Map<String, Double>  avgAmounts = p.getMerchantCatAvgAmounts90d();

        if (amounts == null || counts == null || avgAmounts == null) return;

        double catAmount30d = amounts.getOrDefault(cat, 0.0);
        int    catCount30d  = counts.getOrDefault(cat, 0);
        double catAvg90d    = avgAmounts.getOrDefault(cat, 0.0);

        if (catCount30d < I07_MIN_TXN_COUNT) return;
        if (catAvg90d <= 0) return;

        if (catAmount30d > catAvg90d * (1 + I07_CAT_INCREASE_PCT / 100.0)) {
            output.addPattern("I-07:" + cat);
        }
    }

    // ── I-09: Spese categoria superiori alla media ────────────
    private static void evaluateI09CategoryAboveAvg(PreEnrichedEvent output,
                                                    CustomerProfile p,
                                                    TransactionEvent txn) {
        if (txn == null || txn.getMerchantCategory() == null) return;
        String cat = txn.getMerchantCategory();
        // Escludi categorie non rilevanti per variazioni di spesa reale
        if (cat.equals("internal_transfer") || cat.equals("salary_income") ||
                cat.equals("b2b_transfer")  || cat.equals("investment")) return;

        Map<String, Double>  amounts    = p.getMerchantCatAmounts30d();
        Map<String, Integer> counts     = p.getMerchantCatCounts30d();
        Map<String, Double>  avgAmounts = p.getMerchantCatAvgAmounts90d();

        if (amounts == null || counts == null || avgAmounts == null) return;

        double catAmount30d = amounts.getOrDefault(cat, 0.0);
        int    catCount30d  = counts.getOrDefault(cat, 0);
        double catAvg90d    = avgAmounts.getOrDefault(cat, 0.0);

        if (catCount30d < I09_MIN_TXN_COUNT) return;
        if (catAvg90d <= 0) return;

        if (catAmount30d > catAvg90d * (1 + I09_CAT_INCREASE_PCT / 100.0)) {
            output.addPattern("I-09:" + cat);
        }
    }

    // ── I-10: Spese categoria inferiori alla media ────────────
    private static void evaluateI10CategoryBelowAvg(PreEnrichedEvent output,
                                                    CustomerProfile p,
                                                    TransactionEvent txn) {
        if (txn == null || txn.getMerchantCategory() == null) return;
        String cat = txn.getMerchantCategory();
        // Escludi categorie non rilevanti per variazioni di spesa reale
        if (cat.equals("internal_transfer") || cat.equals("salary_income") ||
                cat.equals("b2b_transfer")  || cat.equals("investment")) return;

        Map<String, Double>  amounts    = p.getMerchantCatAmounts30d();
        Map<String, Integer> counts     = p.getMerchantCatCounts30d();
        Map<String, Double>  avgAmounts = p.getMerchantCatAvgAmounts90d();

        if (amounts == null || counts == null || avgAmounts == null) return;

        double catAmount30d = amounts.getOrDefault(cat, 0.0);
        int    catCount30d  = counts.getOrDefault(cat, 0);
        double catAvg90d    = avgAmounts.getOrDefault(cat, 0.0);

        if (catCount30d < I10_MIN_TXN_COUNT) return;
        if (catAvg90d <= 0) return;

        if (catAmount30d < catAvg90d * (1 - I10_CAT_DECREASE_PCT / 100.0)) {
            output.addPattern("I-10:" + cat);
        }
    }

    // ── I-12: Incidenza categoria su reddito ──────────────────
    private static void evaluateI12IncomeRatio(PreEnrichedEvent output,
                                               CustomerProfile p,
                                               TransactionEvent txn) {
        if (txn == null || txn.getMerchantCategory() == null) return;
        if (p.getEstimatedMonthlyIncome() <= 0) return;

        // escludi categorie non rilevanti
        String cat = txn.getMerchantCategory();
        if (cat.equals("internal_transfer") || cat.equals("investment") ||
                cat.equals("salary_income")     || cat.equals("b2b_transfer")) return;

        Map<String, Double> amounts = p.getMerchantCatAmounts30d();
        if (amounts == null) return;

        double catAmount = amounts.getOrDefault(cat, 0.0);
        double ratio     = (catAmount / p.getEstimatedMonthlyIncome()) * 100.0;

        if (ratio >= I12_INCOME_RATIO_MIN_PCT) {
            output.addPattern("I-12:" + cat + ":" + Math.round(ratio) + "pct");
        }
    }

    // ── I-16: Utilizzo plafond carta alto ─────────────────────
    private static void evaluateI16PlafondHigh(PreEnrichedEvent output,
                                               CustomerProfile p) {
        Map<String, CardProfileState> cards = p.getCardProfiles();
        if (cards == null || cards.isEmpty()) return;

        for (Map.Entry<String, CardProfileState> entry : cards.entrySet()) {
            CardProfileState card = entry.getValue();
            if (card == null || card.getPlafondLimit() <= 0) continue;
            if (!"active".equals(card.getStatus())) continue;
            if (!"credit".equals(card.getCardType())) continue;

            double usagePct = (card.getPlafondUsed() / card.getPlafondLimit()) * 100.0;
            if (usagePct >= I16_PLAFOND_USAGE_MIN_PCT) {
                output.addPattern("I-16:" + entry.getKey());
            }
        }
    }

    // ── I-17: Possibile blocco carta ─────────────────────────
    private static void evaluateI17CardBlockRisk(PreEnrichedEvent output,
                                                 CustomerProfile p) {
        Map<String, CardProfileState> cards = p.getCardProfiles();
        if (cards == null || cards.isEmpty()) return;

        // spesa giornaliera media da w7
        double dailySpend = p.getW7().getSumAmt() / 7.0;
        if (dailySpend <= 0) return;

        for (Map.Entry<String, CardProfileState> entry : cards.entrySet()) {
            CardProfileState card = entry.getValue();
            if (card == null || card.getPlafondLimit() <= 0) continue;
            if (!"active".equals(card.getStatus())) continue;
            if (!"credit".equals(card.getCardType())) continue;

            double residuo       = card.getPlafondAvailable();
            double daysRemaining = residuo / dailySpend;

            if (daysRemaining <= I17_DAYS_REMAINING_MAX) {
                output.addPattern("I-17:" + entry.getKey() + ":" + Math.round(daysRemaining) + "d");
            }
        }
    }
}