package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.CardProfileState;
import com.marbl.eventsmash.model.CounterpartProfile;
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
    private static final double P01_BALANCE_DELTA_MIN_PCT   = 20.0;

    private static final double P02_CREDIT_LINE_USAGE_MIN   = 70.0;
    private static final int    P02_DAYS_PAST_DUE_MIN       = 1;

    private static final int    P03_PUSH_IGNORE_STREAK_MIN  = 3;
    private static final double P03_SESSION_DROP_MIN_PCT    = 30.0;

    private static final int    P04_INVEST_VIEWS_MIN        = 2;
    private static final double P04_BALANCE_DELTA_MIN_PCT   = 40.0;

    private static final int    P05_WIRE_COUNT_MIN          = 3;

    // ── Soglie Insight CEP — categoria ───────────────────────
    private static final double I07_CAT_INCREASE_PCT        = 20.0;
    private static final int    I07_MIN_TXN_COUNT           = 3;

    private static final double I09_CAT_INCREASE_PCT        = 15.0;
    private static final int    I09_MIN_TXN_COUNT           = 3;

    private static final double I10_CAT_DECREASE_PCT        = 15.0;
    private static final int    I10_MIN_TXN_COUNT           = 3;

    private static final double I12_INCOME_RATIO_MIN_PCT    = 30.0;

    // ── Soglie Insight CEP — carte ────────────────────────────
    private static final double I16_PLAFOND_USAGE_MIN_PCT   = 80.0;
    private static final double I17_DAYS_REMAINING_MAX      = 4.0;

    // ── Soglie Insight CEP — counterpart (NUOVO Sprint 6a) ───
    // I-01/02: pagamento OUTBOUND anomalo vs media storica
    private static final double I01_AMOUNT_HIGH_PCT         = 20.0;  // +20% vs avg
    private static final double I02_AMOUNT_LOW_PCT          = 20.0;  // -20% vs avg
    private static final int    I01_MIN_PAYMENT_COUNT       = 6;     // storico minimo 6 transazioni

    // I-03/04: accredito INBOUND anomalo vs media storica
    private static final double I03_AMOUNT_HIGH_PCT         = 20.0;  // +20% vs avg
    private static final double I04_AMOUNT_LOW_PCT          = 20.0;  // -20% vs avg
    private static final int    I03_MIN_PAYMENT_COUNT       = 6;

    // I-13: nuovo abbonamento (merchant non visto negli ultimi 6 mesi)
    private static final int    I13_MIN_PAYMENT_COUNT       = 3;
    private static final long   I13_NEW_MERCHANT_WINDOW_MS  = 180L * 24 * 60 * 60 * 1000; // 6 mesi in ms

    // I-14: aumento costo abbonamento
    private static final double I14_AMOUNT_INCREASE_PCT     = 15.0;  // +15% vs avg
    private static final int    I14_MIN_PAYMENT_COUNT       = 3;

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

            // Insight categoria (invariati)
            evaluateI07CategoryIncrease(output, profile, txn);
            evaluateI09CategoryAboveAvg(output, profile, txn);
            evaluateI10CategoryBelowAvg(output, profile, txn);
            evaluateI12IncomeRatio(output, profile, txn);

            // Insight carte (invariati)
            evaluateI16PlafondHigh(output, profile);
            evaluateI17CardBlockRisk(output, profile);

            // Insight importo (invariato)
            evaluateI18LargeOutlierOutflow(output, profile, txn);

            // Insight counterpart (NUOVO Sprint 6a)
            evaluateI01PaymentHigherThanUsual(output, profile, txn);
            evaluateI02PaymentLowerThanUsual(output, profile, txn);
            evaluateI03CreditHigherThanUsual(output, profile, txn);
            evaluateI04CreditLowerThanUsual(output, profile, txn);
            evaluateI13NewSubscription(output, profile, txn);
            evaluateI14SubscriptionCostIncrease(output, profile, txn);
        }
    }

    // =========================================================================
    // PATTERN PREDITTIVI
    // =========================================================================

    // ── P-01: Real Estate Intent ──────────────────────────────
    private static void evaluateP01RealEstate(PreEnrichedEvent output,
                                              CustomerProfile p) {
        if (p.isHasMortgage()) return;
        if (!p.getActivePattern().equals("ordinary") &&
                !p.getActivePattern().equals("real_estate")) return;

        boolean simViews     = p.getMortgageSimViews7d() >= P01_MORTGAGE_SIM_VIEWS_MIN;
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

        if (creditLineStress && !dpd) {
            output.addPattern("P-02-PHASE1");
        }
        if (creditLineStress && (dpd || paymentDelay)) {
            output.addPattern("P-02-PHASE2");
        }
    }

    // ── P-03: Pre-Churn ───────────────────────────────────────
    private static void evaluateP03PreChurn(PreEnrichedEvent output,
                                            CustomerProfile p) {
        boolean narrowing   = p.hasFeatureNarrowing();
        boolean pushIgnore  = p.getPushIgnoreStreak() >= P03_PUSH_IGNORE_STREAK_MIN;
        boolean sessionDrop = p.getSessionDurationDeltaPct() <= -P03_SESSION_DROP_MIN_PCT;

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

    // =========================================================================
    // INSIGHT — CATEGORIA
    // =========================================================================

    // ── I-07: Aumento spese categoria ────────────────────────
    private static void evaluateI07CategoryIncrease(PreEnrichedEvent output,
                                                    CustomerProfile p,
                                                    TransactionEvent txn) {
        if (txn == null || txn.getMerchantCategory() == null) return;
        String cat = txn.getMerchantCategory();
        if (isCategoryExcluded(cat)) return;

        Map<String, Double>  amounts    = p.getMerchantCatAmounts30d();
        Map<String, Integer> counts     = p.getMerchantCatCounts30d();
        Map<String, Double>  avgAmounts = p.getMerchantCatAvgAmounts90d();
        if (amounts == null || counts == null || avgAmounts == null) return;

        double catAmount30d = amounts.getOrDefault(cat, 0.0);
        int    catCount30d  = counts.getOrDefault(cat, 0);
        double catAvg90d    = avgAmounts.getOrDefault(cat, 0.0);

        if (catCount30d < I07_MIN_TXN_COUNT || catAvg90d <= 0) return;

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
        if (isCategoryExcluded(cat)) return;

        Map<String, Double>  amounts    = p.getMerchantCatAmounts30d();
        Map<String, Integer> counts     = p.getMerchantCatCounts30d();
        Map<String, Double>  avgAmounts = p.getMerchantCatAvgAmounts90d();
        if (amounts == null || counts == null || avgAmounts == null) return;

        double catAmount30d = amounts.getOrDefault(cat, 0.0);
        int    catCount30d  = counts.getOrDefault(cat, 0);
        double catAvg90d    = avgAmounts.getOrDefault(cat, 0.0);

        if (catCount30d < I09_MIN_TXN_COUNT || catAvg90d <= 0) return;

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
        if (isCategoryExcluded(cat)) return;

        Map<String, Double>  amounts    = p.getMerchantCatAmounts30d();
        Map<String, Integer> counts     = p.getMerchantCatCounts30d();
        Map<String, Double>  avgAmounts = p.getMerchantCatAvgAmounts90d();
        if (amounts == null || counts == null || avgAmounts == null) return;

        double catAmount30d = amounts.getOrDefault(cat, 0.0);
        int    catCount30d  = counts.getOrDefault(cat, 0);
        double catAvg90d    = avgAmounts.getOrDefault(cat, 0.0);

        if (catCount30d < I10_MIN_TXN_COUNT || catAvg90d <= 0) return;

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

        String cat = txn.getMerchantCategory();
        if (isCategoryExcluded(cat)) return;

        Map<String, Double> amounts = p.getMerchantCatAmounts30d();
        if (amounts == null) return;

        double catAmount = amounts.getOrDefault(cat, 0.0);
        double ratio     = (catAmount / p.getEstimatedMonthlyIncome()) * 100.0;

        if (ratio >= I12_INCOME_RATIO_MIN_PCT) {
            output.addPattern("I-12:" + cat + ":" + Math.round(ratio) + "pct");
        }
    }

    // =========================================================================
    // INSIGHT — CARTE
    // =========================================================================

    // ── I-16: Utilizzo plafond carta alto ─────────────────────
    private static void evaluateI16PlafondHigh(PreEnrichedEvent output,
                                               CustomerProfile p) {
        Map<String, CardProfileState> cards = p.getCardProfiles();
        if (cards == null || cards.isEmpty()) return;

        for (Map.Entry<String, CardProfileState> entry : cards.entrySet()) {
            CardProfileState card = entry.getValue();
            if (!isCreditCardActive(card)) continue;

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

        double dailySpend = p.getW7().getSumAmt() / 7.0;
        if (dailySpend <= 0) return;

        for (Map.Entry<String, CardProfileState> entry : cards.entrySet()) {
            CardProfileState card = entry.getValue();
            if (!isCreditCardActive(card)) continue;

            double daysRemaining = card.getPlafondAvailable() / dailySpend;
            if (daysRemaining <= I17_DAYS_REMAINING_MAX) {
                output.addPattern("I-17:" + entry.getKey() + ":" + Math.round(daysRemaining) + "d");
            }
        }
    }

    // =========================================================================
    // INSIGHT — IMPORTO
    // =========================================================================

    // ── I-18: Uscita singola anomala ──────────────────────────
    private static void evaluateI18LargeOutlierOutflow(PreEnrichedEvent output,
                                                       CustomerProfile p,
                                                       TransactionEvent txn) {
        if (txn == null || txn.getAmount() == null) return;
        double amount = txn.getAmount().doubleValue();
        if (p.isLargeOutlierOutflow(amount)) {
            output.addPattern("I-18");
        }
    }

    // =========================================================================
    // INSIGHT — COUNTERPART (NUOVO Sprint 6a)
    // =========================================================================

    // ── I-01: Pagamento OUTBOUND più alto del solito ──────────
    //
    // Scatta quando un pagamento verso una controparte nota è superiore
    // del 20% rispetto alla media storica dei pagamenti verso quella stessa
    // controparte. Richiede almeno 6 pagamenti storici (storico 6 mesi).
    // Escludi: giroconti, rate mutuo/finanziamento
    private static void evaluateI01PaymentHigherThanUsual(PreEnrichedEvent output,
                                                          CustomerProfile p,
                                                          TransactionEvent txn) {
        if (txn == null || txn.getAmount() == null) return;
        double amount = txn.getAmount().doubleValue();
        if (amount >= 0) return; // solo uscite

        String token = txn.getCounterpartToken();
        if (token == null || token.isBlank()) return;

        CounterpartProfile cp = getCounterpart(p, token);
        if (cp == null) return;
        if (!"OUTBOUND".equals(cp.getDirection())) return;
        if (cp.getPaymentCount12m() < I01_MIN_PAYMENT_COUNT) return;

        double avg = Math.abs(cp.getAvgAmount12m());
        if (avg <= 0) return;

        // Confronto su valori assoluti — evita confusione su segni negativi
        if (Math.abs(amount) > avg * (1 + I01_AMOUNT_HIGH_PCT / 100.0)) {
            output.addPattern("I-01:" + token);
        }
    }

    // ── I-02: Pagamento OUTBOUND più basso del solito ─────────
    //
    // Variante speculare di I-01 — pagamento inferiore del 20% alla media.
    // Utile per rilevare variazioni tariffarie, sconti inattesi, errori.
    private static void evaluateI02PaymentLowerThanUsual(PreEnrichedEvent output,
                                                         CustomerProfile p,
                                                         TransactionEvent txn) {
        if (txn == null || txn.getAmount() == null) return;
        double amount = txn.getAmount().doubleValue();
        if (amount >= 0) return;

        String token = txn.getCounterpartToken();
        if (token == null || token.isBlank()) return;

        CounterpartProfile cp = getCounterpart(p, token);
        if (cp == null) return;
        if (!"OUTBOUND".equals(cp.getDirection())) return;
        if (cp.getPaymentCount12m() < I01_MIN_PAYMENT_COUNT) return;

        double avg = Math.abs(cp.getAvgAmount12m());
        if (avg <= 0) return;

        if (Math.abs(amount) < avg * (1 - I02_AMOUNT_LOW_PCT / 100.0)) {
            output.addPattern("I-02:" + token);
        }
    }

    // ── I-03: Accredito INBOUND più alto del solito ───────────
    //
    // Scatta quando un accredito da una controparte nota è superiore
    // del 20% rispetto alla media storica degli accrediti da quella stessa
    // controparte. Tipico: stipendio con bonus inatteso, accredito extra.
    private static void evaluateI03CreditHigherThanUsual(PreEnrichedEvent output,
                                                         CustomerProfile p,
                                                         TransactionEvent txn) {
        if (txn == null || txn.getAmount() == null) return;
        double amount = txn.getAmount().doubleValue();
        if (amount <= 0) return; // solo entrate

        String token = txn.getCounterpartToken();
        if (token == null || token.isBlank()) return;

        CounterpartProfile cp = getCounterpart(p, token);
        if (cp == null) return;
        if (!"INBOUND".equals(cp.getDirection())) return;
        if (cp.getPaymentCount12m() < I03_MIN_PAYMENT_COUNT) return;

        double avg = cp.getAvgAmount12m(); // positivo per INBOUND
        if (avg <= 0) return;

        if (amount > avg * (1 + I03_AMOUNT_HIGH_PCT / 100.0)) {
            output.addPattern("I-03:" + token);
        }
    }

    // ── I-04: Accredito INBOUND più basso del solito ─────────
    //
    // Variante speculare di I-03 — accredito inferiore del 20% alla media.
    // Tipico: stipendio ridotto, mancata quota variabile, trattenuta inattesa.
    private static void evaluateI04CreditLowerThanUsual(PreEnrichedEvent output,
                                                        CustomerProfile p,
                                                        TransactionEvent txn) {
        if (txn == null || txn.getAmount() == null) return;
        double amount = txn.getAmount().doubleValue();
        if (amount <= 0) return;

        String token = txn.getCounterpartToken();
        if (token == null || token.isBlank()) return;

        CounterpartProfile cp = getCounterpart(p, token);
        if (cp == null) return;
        if (!"INBOUND".equals(cp.getDirection())) return;
        if (cp.getPaymentCount12m() < I03_MIN_PAYMENT_COUNT) return;

        double avg = cp.getAvgAmount12m();
        if (avg <= 0) return;

        if (amount < avg * (1 - I04_AMOUNT_LOW_PCT / 100.0)) {
            output.addPattern("I-04:" + token);
        }
    }

    // ── I-13: Nuovo abbonamento rilevato ─────────────────────
    //
    // Scatta quando viene rilevata una controparte ricorrente il cui
    // primo pagamento è avvenuto negli ultimi 6 mesi (merchant nuovo).
    // Distingue un abbonamento appena attivato da uno già consolidato.
    // Soglia: isRecurring=true AND firstSeenDate >= now() - 180gg
    private static void evaluateI13NewSubscription(PreEnrichedEvent output,
                                                   CustomerProfile p,
                                                   TransactionEvent txn) {
        if (txn == null || txn.getAmount() == null) return;
        if (txn.getAmount().doubleValue() >= 0) return; // solo uscite

        String token = txn.getCounterpartToken();
        if (token == null || token.isBlank()) return;

        CounterpartProfile cp = getCounterpart(p, token);
        if (cp == null) return;
        if (!cp.isRecurring()) return;
        if (cp.getPaymentCount12m() < I13_MIN_PAYMENT_COUNT) return;

        // Merchant non visto prima degli ultimi 6 mesi
        long nowMs       = System.currentTimeMillis();
        long windowStart = nowMs - I13_NEW_MERCHANT_WINDOW_MS;
        if (cp.getFirstSeenDate() < windowStart) return; // conosciuto da più di 6 mesi

        output.addPattern("I-13:" + token);
    }

    // ── I-14: Aumento costo abbonamento ──────────────────────
    //
    // Scatta quando il pagamento corrente verso un merchant con abbonamento
    // attivo è superiore del 15% rispetto alla media storica dei pagamenti
    // verso quella stessa controparte.
    // Rileva: rinnovi con aumento prezzo, cambio piano tariffario.
    private static void evaluateI14SubscriptionCostIncrease(PreEnrichedEvent output,
                                                            CustomerProfile p,
                                                            TransactionEvent txn) {
        if (txn == null || txn.getAmount() == null) return;
        double amount = txn.getAmount().doubleValue();
        if (amount >= 0) return; // solo uscite

        String token = txn.getCounterpartToken();
        if (token == null || token.isBlank()) return;

        CounterpartProfile cp = getCounterpart(p, token);
        if (cp == null) return;
        if (!cp.isSubscription()) return; // solo abbonamenti veri (isSubscription=true)
        if (cp.getPaymentCount12m() < I14_MIN_PAYMENT_COUNT) return;

        double avg = Math.abs(cp.getAvgAmount12m());
        if (avg <= 0) return;

        if (Math.abs(amount) > avg * (1 + I14_AMOUNT_INCREASE_PCT / 100.0)) {
            output.addPattern("I-14:" + token);
        }
    }

    // =========================================================================
    // UTILITY PRIVATE
    // =========================================================================

    /**
     * Recupera il CounterpartProfile dalla mappa del CustomerProfile.
     * Restituisce null se il profilo non esiste o la mappa è vuota.
     */
    private static CounterpartProfile getCounterpart(CustomerProfile p, String token) {
        Map<String, CounterpartProfile> counterparts = p.getCounterparts();
        if (counterparts == null || counterparts.isEmpty()) return null;
        return counterparts.get(token);
    }

    /**
     * Verifica se la carta è di credito e attiva con plafond valorizzato.
     */
    private static boolean isCreditCardActive(CardProfileState card) {
        if (card == null || card.getPlafondLimit() <= 0) return false;
        if (!"active".equals(card.getStatus())) return false;
        if (!"credit".equals(card.getCardType())) return false;
        return true;
    }

    /**
     * Categorie escluse dagli insight di variazione spesa categoria.
     * Evita falsi positivi su movimenti non comparabili.
     */
    private static boolean isCategoryExcluded(String cat) {
        return cat.equals("internal_transfer") ||
                cat.equals("salary_income")     ||
                cat.equals("b2b_transfer")       ||
                cat.equals("investment");
    }
}