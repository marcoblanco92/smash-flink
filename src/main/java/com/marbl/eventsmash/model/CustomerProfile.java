package com.marbl.eventsmash.model;

import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerProfile implements Serializable {

    // ── Identificatore ──
    private String customerId;
    private boolean isColdStart;

    // ── w7 — finestra real-time calcolata incrementalmente da Flink ──
    private WindowStats w7        = new WindowStats();
    private long        w7ResetTs = 0L;

    // ── Baseline da ClickHouse (w30 con rolling settimanale) ──
    private double   w30SumAmt;
    private int      w30Count;
    private double   w30AvgAmt;
    private double   w30MaxAmt;
    private double   w30StdDev;
    private double[] w30WeeklySums  = new double[4];
    private double   w30WeeklySlope;

    // ── Media per categoria su 90 giorni ──
    // Calcolata da smash-batch: catAmounts90d / catCounts90d
    // Usata dal CEP per confronto "spesa categoria vs media storica"
    // es: merchantCatAvgAmounts90d = {"grocery": 95.50, "transport": 45.00}
    private java.util.Map<String, Double> merchantCatAvgAmounts90d;


    // ── Baseline da ClickHouse (w90 con rolling mensile) ──
    private double   w90SumAmt;
    private int      w90Count;
    private double   w90AvgAmt;
    private double   w90MaxAmt;
    private double   w90StdDev;
    private double[] w90MonthlySums = new double[3];
    private double   w90MonthlySlope;

    // ── Baseline da ClickHouse (w180) ──
//    private double w180SumAmt;
//    private int    w180Count;
//    private double w180AvgAmt;
//    private double w180MaxAmt;
//    private double w180StdDev;

    // ── Baseline da ClickHouse (w365) ──
    private double w365SumAmt;
    private int    w365Count;
    private double w365AvgAmt;
    private double w365MaxAmt;
    private double w365StdDev;

    // ── Composizione qualitativa (da ClickHouse, finestra 30d) ──
    private Map<String, Double>  merchantCatAmounts30d = new HashMap<>();
    private Map<String, Integer> merchantCatCounts30d  = new HashMap<>();
    private Map<String, Integer> channelCounts30d      = new HashMap<>();
    private int    distinctCounterparts30d = 0;
    private double estimatedMonthlyIncome  = 0.0;

    // ── Controparti recenti (w7 — Flink real-time) ──
    private Set<String>                     knownCounterparts7d    = new HashSet<>();
    private int                             distinctCounterparts7d = 0;
    private Map<String, CounterpartProfile> counterpartProfiles    = new HashMap<>();

    // ── Cards (credit_utilization_alert) ──
    private Map<String, CardProfileState> cardProfiles = new HashMap<>();

    // ── Ultimi eventi significativi (max 10) ──
    private Deque<MiniEvent> recentSignificantEvents = new ArrayDeque<>(10);

    // ── Saldi ──
    private double currentBalance = 0.0;
    private double balance30dAgo  = 0.0;

    // ── Loan signals ──
    private double creditLineUsagePct  = 0.0;
    private int    daysPastDue         = 0;
    private int    avgPaymentDelayDays = 0;

    // ── CRM snapshot ──
    private String  segment        = "retail";
    private boolean hasMortgage    = false;
    private boolean hasInvestments = false;
    private double  clvScore       = 0.0;
    private boolean pushOptIn      = true;
    private String  activePattern  = "ordinary";
    private boolean hasRm          = false;

    // ── App behavior signals ──
    private int    avgSessionDuration30d                     = 0;
    private double sessionDurationDeltaPct                   = 0.0;
    private Map<String, Integer> featureCategoryDistribution = new HashMap<>();
    private double screensVisitedAvg7d                       = 0.0;  // EMA incrementale
    private int    pushIgnoreStreak                          = 0;
    private int    mortgageSimViews7d                        = 0;
    private int    investmentViews7d                         = 0;

    // ── Correlazione digitale — segnale antifrode ──────────────────────────
    // Traccia storicamente se i bonifici di questo cliente nascono da app/web.
    // Non richiede join real-time — si accumula nel profilo ad ogni evento.
    // Soglia CEP: se wireFromAppPct30d > 80% e arriva wire senza app event
    // recente (< 30s) → possibile bonifico non autorizzato.
    private int    wireCount30d        = 0;    // totale wire/instant ultimi 30d
    private int    wireFromAppCount30d = 0;    // wire con app event nei 30s precedenti
    private double wireFromAppPct30d   = 0.0;  // ratio — usato dal CEP

    // ── Timestamps ──
    private long lastEventTs         = 0L;
    private long lastAppEventTs      = 0L;   // epoch millis ultimo app event — usato per correlazione digitale
    private long lastBaselineTs      = 0L;
    private long lastProfileUpdateTs = 0L;


    // =========================================================================
    // AGGIORNAMENTO DA BASELINE CLICKHOUSE
    // =========================================================================

    public void updateFromBaseline(CustomerBaseline b) {

        isColdStart    = b.isColdStart();

        w30SumAmt      = b.getW30SumAmt();
        w30Count       = b.getW30Count();
        w30AvgAmt      = b.getW30AvgAmt();
        w30MaxAmt      = b.getW30MaxAmt();
        w30StdDev      = b.getW30StdDev();
        w30WeeklySums  = b.getW30WeeklySums();
        w30WeeklySlope = b.getW30WeeklySlope();

        w90SumAmt       = b.getW90SumAmt();
        w90Count        = b.getW90Count();
        w90AvgAmt       = b.getW90AvgAmt();
        w90MaxAmt       = b.getW90MaxAmt();
        w90StdDev       = b.getW90StdDev();
        w90MonthlySums  = b.getW90MonthlySums();
        w90MonthlySlope = b.getW90MonthlySlope();

//        w180SumAmt = b.getW180SumAmt();
//        w180Count  = b.getW180Count();
//        w180AvgAmt = b.getW180AvgAmt();
//        w180MaxAmt = b.getW180MaxAmt();
//        w180StdDev = b.getW180StdDev();

        w365SumAmt = b.getW365SumAmt();
        w365Count  = b.getW365Count();
        w365AvgAmt = b.getW365AvgAmt();
        w365MaxAmt = b.getW365MaxAmt();
        w365StdDev = b.getW365StdDev();

        if (b.getMerchantCatAmounts30d() != null) merchantCatAmounts30d = b.getMerchantCatAmounts30d();
        if (b.getMerchantCatCounts30d()  != null) merchantCatCounts30d  = b.getMerchantCatCounts30d();
        if (b.getChannelCounts30d()      != null) channelCounts30d      = b.getChannelCounts30d();

        distinctCounterparts30d = b.getDistinctCounterparts30d();
        estimatedMonthlyIncome  = b.getEstimatedMonthlyIncome();
        balance30dAgo           = b.getBalance30dAgo();

        lastBaselineTs      = b.getComputedAt();
        lastProfileUpdateTs = System.currentTimeMillis();
    }


    // =========================================================================
    // AGGIORNAMENTO HOT PATH — TRANSAZIONE
    // =========================================================================

    public void updateFromTransaction(
            double amount,
            String merchantCat,
            String channel,
            String counterpart,
            long   eventTs
    ) {
        double absAmount = Math.abs(amount);

        // w7 incrementale
        w7.update(absAmount, w90AvgAmt);

        // Controparti w7
        if (counterpart != null) {
            knownCounterparts7d.add(counterpart);
            distinctCounterparts7d = knownCounterparts7d.size();
        }

        // Correlazione digitale — aggiorna wireFromAppPct30d
        // Un wire/instant è "da app" se c'è stato un app event negli ultimi 30s
        if ("wire".equals(channel) || "instant".equals(channel)) {
            wireCount30d++;
            long secondsSinceLastApp = lastAppEventTs > 0
                    ? (eventTs - lastAppEventTs) / 1_000L
                    : Long.MAX_VALUE;
            if (secondsSinceLastApp <= 30) {
                wireFromAppCount30d++;
            }
            wireFromAppPct30d = wireCount30d > 0
                    ? (double) wireFromAppCount30d / wireCount30d
                    : 0.0;
        }

        // Evento significativo
        if (isSignificant(absAmount, merchantCat)) {
            addSignificantEvent(new MiniEvent(eventTs, absAmount, merchantCat, channel));
        }

        lastEventTs         = eventTs;
        lastProfileUpdateTs = System.currentTimeMillis();
    }


    // =========================================================================
    // AGGIORNAMENTO CRM
    // =========================================================================

    public void updateFromCrm(
            String  segment,
            boolean hasMortgage,
            boolean hasInvestments,
            double  clvScore,
            boolean pushOptIn,
            String  activePattern,
            boolean hasRm,
            int     avgSessionDuration30d,
            int     pushIgnoreStreak
    ) {
        this.segment               = segment;
        this.hasMortgage           = hasMortgage;
        this.hasInvestments        = hasInvestments;
        this.clvScore              = clvScore;
        this.pushOptIn             = pushOptIn;
        this.activePattern         = activePattern;
        this.hasRm                 = hasRm;
        this.avgSessionDuration30d = avgSessionDuration30d;
        this.pushIgnoreStreak      = pushIgnoreStreak;
        this.lastProfileUpdateTs   = System.currentTimeMillis();
    }


    // =========================================================================
    // AGGIORNAMENTO ACCOUNT
    // =========================================================================

    public void updateFromAccount(double newBalance) {
        this.currentBalance      = newBalance;
        this.lastProfileUpdateTs = System.currentTimeMillis();
    }


    // =========================================================================
    // AGGIORNAMENTO LOAN
    // =========================================================================

    public void updateFromLoan(
            double creditLineUsagePct,
            int    daysPastDue,
            int    avgPaymentDelayDays
    ) {
        this.creditLineUsagePct  = creditLineUsagePct;
        this.daysPastDue         = daysPastDue;
        this.avgPaymentDelayDays = avgPaymentDelayDays;
        this.lastProfileUpdateTs = System.currentTimeMillis();
    }


    // =========================================================================
    // AGGIORNAMENTO APP EVENT
    // =========================================================================

    public void updateFromAppEvent(
            String  screenName,
            String  featureCategory,
            boolean isPushOpened,
            int     screensVisitedN,
            long    eventTs
    ) {
        if (featureCategory != null) {
            featureCategoryDistribution.merge(featureCategory, 1, Integer::sum);
        }

        // EMA incrementale per screensVisitedAvg7d — alpha = 0.1
        if (screensVisitedAvg7d == 0.0) {
            screensVisitedAvg7d = screensVisitedN;
        } else {
            screensVisitedAvg7d = 0.9 * screensVisitedAvg7d + 0.1 * screensVisitedN;
        }

        if (isPushOpened) {
            pushIgnoreStreak = 0;
        }

        if (screenName != null) {
            if (screenName.contains("mutui") || screenName.contains("simulazione")) {
                mortgageSimViews7d++;
            }
            if (screenName.contains("investimenti") || screenName.contains("fondi")) {
                investmentViews7d++;
            }
        }

        lastAppEventTs      = eventTs;  // usato da updateFromTransaction per correlazione digitale
        lastEventTs         = eventTs;
        lastProfileUpdateTs = System.currentTimeMillis();
    }


    // =========================================================================
    // RESET w7
    // =========================================================================

    public void resetW7(long resetTs) {
        w7.reset();
        knownCounterparts7d.clear();
        distinctCounterparts7d = 0;
        mortgageSimViews7d     = 0;
        investmentViews7d      = 0;
        screensVisitedAvg7d    = 0.0;
        wireCount30d           = 0;
        wireFromAppCount30d    = 0;
        wireFromAppPct30d      = 0.0;
        w7ResetTs              = resetTs;
    }


    // =========================================================================
    // METODI DERIVATI — usati dal CEP
    // =========================================================================

    /** Delta saldo corrente vs 30d fa. */
    public double balanceDelta30dPct() {
        if (balance30dAgo == 0) return 0.0;
        return ((currentBalance - balance30dAgo) / balance30dAgo) * 100.0;
    }

    /** Delta spesa w7 vs media settimanale w30. */
    public double spendingDeltaW7vsW30Pct() {
        if (w30AvgAmt == 0) return 0.0;
        double w30Weekly = w30AvgAmt / 4.33;
        return ((w7.getSumAmt() - w30Weekly) / w30Weekly) * 100.0;
    }

    /** Narrowing pre-churn. */
    public boolean hasFeatureNarrowing() {
        int essential   = featureCategoryDistribution.getOrDefault("essential", 0);
        int exploratory = featureCategoryDistribution.getOrDefault("exploratory", 0);
        int commercial  = featureCategoryDistribution.getOrDefault("commercial", 0);
        return essential > 0 && exploratory == 0 && commercial == 0;
    }

    /**
     * True se il bonifico è anomalo: cliente che storicamente usa l'app per i bonifici
     * ma questa transazione non ha un app event nei 30s precedenti.
     * Soglia minima 3 bonifici storici per evitare falsi positivi su nuovi clienti.
     */
    public boolean isWireWithoutDigitalOrigin(long txnTs) {
        if (wireCount30d < 3) return false;
        if (wireFromAppPct30d < 0.80) return false;
        long secondsSinceLastApp = lastAppEventTs > 0
                ? (txnTs - lastAppEventTs) / 1_000L
                : Long.MAX_VALUE;
        return secondsSinceLastApp > 30;
    }


    // =========================================================================
    // UTILITY PRIVATE
    // =========================================================================

    private void addSignificantEvent(MiniEvent event) {
        if (recentSignificantEvents.size() >= 10) {
            recentSignificantEvents.pollFirst();
        }
        recentSignificantEvents.addLast(event);
    }

    private boolean isSignificant(double amount, String merchantCat) {
        boolean aboveThreshold = w90AvgAmt > 0 && amount > 2 * w90AvgAmt;
        boolean watchlistCat   = switch (merchantCat) {
            case "real_estate", "legal_services", "crypto_exchange",
                 "salary_advance", "investment" -> true;
            default -> false;
        };
        return aboveThreshold || watchlistCat;
    }
}