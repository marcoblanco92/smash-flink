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

    // ── Identificatore ────────────────────────────────────────
    private String customerId;
    private boolean isColdStart;

    // ── w7 — real-time Flink ──────────────────────────────────
    private WindowStats w7 = new WindowStats();
    private long w7ResetTs = 0L;

    // ── w30 ──────────────────────────────────────────────────
    private double w30SumAmt;
    private int w30Count;
    private double w30AvgAmt;
    private double w30MaxAmt;
    private double w30MinAmt;
    private double w30StdDev;
    private double[] w30WeeklySums = new double[4];
    private double w30WeeklySlope;

    // ── w90 ──────────────────────────────────────────────────
    private double w90SumAmt;
    private int w90Count;
    private double w90AvgAmt;
    private double w90MaxAmt;
    private double w90MinAmt;
    private double w90StdDev;
    private double[] w90MonthlySums = new double[3];
    private double w90MonthlySlope;

    // ── w180 — NUOVO ─────────────────────────────────────────
    private double w180SumAmt;
    private int w180Count;
    private double[] w180MonthlySums = new double[6]; // [m-5..m0]
    private double w180MonthlySlope;                 // OLS slope su 6 punti

    // ── w365 ─────────────────────────────────────────────────
    private double w365SumAmt;
    private int w365Count;
    private double w365AvgAmt;
    private double w365MaxAmt;
    private double w365MinAmt;
    private double w365StdDev;

    // ── Mappe qualitative (30d) ───────────────────────────────
    private Map<String, Double> merchantCatAvgAmounts90d;
    private Map<String, Double> merchantCatAmounts30d = new HashMap<>();
    private Map<String, Integer> merchantCatCounts30d = new HashMap<>();
    private Map<String, Integer> channelCounts30d = new HashMap<>();
    private int distinctCounterparts30d = 0;
    private double estimatedMonthlyIncome = 0.0;

    // ── Controparti ───────────────────────────────────────────
    private Set<String> knownCounterparts7d = new HashSet<>();
    private int distinctCounterparts7d = 0;
    private Map<String, CounterpartProfile> counterpartProfiles = new HashMap<>();

    // ── Cards ─────────────────────────────────────────────────
    private Map<String, CardProfileState> cardProfiles = new HashMap<>();

    // ── Ultimi eventi significativi ───────────────────────────
    private Deque<MiniEvent> recentSignificantEvents = new ArrayDeque<>(10);

    // ── Saldi ─────────────────────────────────────────────────
    private double currentBalance = 0.0;
    private double balance30dAgo = 0.0;

    // ── Loan signals ──────────────────────────────────────────
    private double creditLineUsagePct = 0.0;
    private int daysPastDue = 0;
    private int avgPaymentDelayDays = 0;

    // ── CRM snapshot ──────────────────────────────────────────
    private String segment = "retail";
    private boolean hasMortgage = false;
    private boolean hasInvestments = false;
    private double clvScore = 0.0;
    private boolean pushOptIn = true;
    private String activePattern = "ordinary";
    private boolean hasRm = false;

    // ── App behavior ──────────────────────────────────────────
    private int avgSessionDuration30d = 0;
    private double sessionDurationDeltaPct = 0.0;
    private Map<String, Integer> featureCategoryDistribution = new HashMap<>();
    private double screensVisitedAvg7d = 0.0;
    private int pushIgnoreStreak = 0;
    private int mortgageSimViews7d = 0;
    private int investmentViews7d = 0;

    // ── Correlazione digitale ─────────────────────────────────
    private int wireCount30d = 0;
    private int wireFromAppCount30d = 0;
    private double wireFromAppPct30d = 0.0;

    // ── Timestamps ────────────────────────────────────────────
    private long lastEventTs = 0L;
    private long lastAppEventTs = 0L;
    private long lastBaselineTs = 0L;
    private long lastProfileUpdateTs = 0L;


    // =========================================================================
    // AGGIORNAMENTO DA BASELINE
    // =========================================================================

    public void updateFromBaseline(CustomerBaseline b) {
        isColdStart = b.isColdStart();

        w30SumAmt = b.getW30SumAmt();
        w30Count = b.getW30Count();
        w30AvgAmt = b.getW30AvgAmt();
        w30MaxAmt = b.getW30MaxAmt();
        w30MinAmt = b.getW30MinAmt();
        w30StdDev = b.getW30StdDev();
        w30WeeklySums = b.getW30WeeklySums();
        w30WeeklySlope = b.getW30WeeklySlope();

        w90SumAmt = b.getW90SumAmt();
        w90Count = b.getW90Count();
        w90AvgAmt = b.getW90AvgAmt();
        w90MaxAmt = b.getW90MaxAmt();
        w90MinAmt = b.getW90MinAmt();
        w90StdDev = b.getW90StdDev();
        w90MonthlySums = b.getW90MonthlySums();
        w90MonthlySlope = b.getW90MonthlySlope();

        // w180
        if (b.getW180MonthlySums() != null) {
            w180SumAmt = b.getW180SumAmt();
            w180Count = b.getW180Count();
            w180MonthlySums = b.getW180MonthlySums();
            w180MonthlySlope = b.getW180MonthlySlope();
        }

        w365SumAmt = b.getW365SumAmt();
        w365Count = b.getW365Count();
        w365AvgAmt = b.getW365AvgAmt();
        w365MaxAmt = b.getW365MaxAmt();
        w365MinAmt = b.getW365MinAmt();
        w365StdDev = b.getW365StdDev();

        if (b.getMerchantCatAvgAmounts90d() != null) merchantCatAvgAmounts90d = b.getMerchantCatAvgAmounts90d();
        if (b.getMerchantCatAmounts30d() != null) merchantCatAmounts30d = b.getMerchantCatAmounts30d();
        if (b.getMerchantCatCounts30d() != null) merchantCatCounts30d = b.getMerchantCatCounts30d();
        if (b.getChannelCounts30d() != null) {
            channelCounts30d = b.getChannelCounts30d();
            // Sincronizza wireCount30d dalla baseline OLAP — fonte autoritativa per contatori canale 30d.
            // Flink usa questo come punto di partenza e incrementa real-time ad ogni nuovo wire/instant.
            int wireFromOlap = channelCounts30d.getOrDefault("wire", 0)
                    + channelCounts30d.getOrDefault("instant", 0);
            if (wireFromOlap > wireCount30d) {
                wireCount30d = wireFromOlap;
            }
        }

        distinctCounterparts30d = b.getDistinctCounterparts30d();
        estimatedMonthlyIncome = b.getEstimatedMonthlyIncome();

        // balance30dAgo: stima da net flow 30d
        // Aggiornato solo se currentBalance è già popolato (dopo il primo AccountEvent)
        if (currentBalance > 0 && w30SumAmt != 0) {
            balance30dAgo = currentBalance - w30SumAmt;
        }

        lastBaselineTs = b.getComputedAt();
        lastProfileUpdateTs = System.currentTimeMillis();
    }


    // =========================================================================
    // AGGIORNAMENTO HOT PATH
    // =========================================================================

    public void updateFromTransaction(double amount, String merchantCat,
                                      String channel, String counterpart, long eventTs) {
        double absAmount = Math.abs(amount);
        w7.update(absAmount, w90AvgAmt);

        // Aggiorna currentBalance incrementalmente
        currentBalance += amount;

        if (counterpart != null) {
            knownCounterparts7d.add(counterpart);
            distinctCounterparts7d = knownCounterparts7d.size();
        }

        if ("wire".equals(channel) || "instant".equals(channel)) {
            wireCount30d++;
            long secondsSinceLastApp = lastAppEventTs > 0
                    ? (eventTs - lastAppEventTs) / 1_000L : Long.MAX_VALUE;
            if (secondsSinceLastApp <= 30) wireFromAppCount30d++;
            wireFromAppPct30d = wireCount30d > 0
                    ? (double) wireFromAppCount30d / wireCount30d : 0.0;
        }

        if (isSignificant(absAmount, merchantCat)) {
            addSignificantEvent(new MiniEvent(eventTs, absAmount, merchantCat, channel));
        }

        lastEventTs = eventTs;
        lastProfileUpdateTs = System.currentTimeMillis();
    }

    public void updateFromCrm(String segment, boolean hasMortgage, boolean hasInvestments,
                              double clvScore, boolean pushOptIn, String activePattern,
                              boolean hasRm, int avgSessionDuration30d, int pushIgnoreStreak) {
        this.segment = segment;
        this.hasMortgage = hasMortgage;
        this.hasInvestments = hasInvestments;
        this.clvScore = clvScore;
        this.pushOptIn = pushOptIn;
        this.activePattern = activePattern;
        this.hasRm = hasRm;
        this.avgSessionDuration30d = avgSessionDuration30d;
        this.pushIgnoreStreak = pushIgnoreStreak;
        this.lastProfileUpdateTs = System.currentTimeMillis();
    }

    public void updateFromAccount(double newBalance) {
        // Usato solo come snapshot iniziale di bootstrap.
        // Dopo il primo aggiornamento, currentBalance è mantenuto
        // incrementalmente da updateFromTransaction().
        if (this.currentBalance == 0.0) {
            this.currentBalance = newBalance;
        }
        this.lastProfileUpdateTs = System.currentTimeMillis();
    }

    public void updateFromLoan(double creditLineUsagePct, int daysPastDue,
                               int avgPaymentDelayDays) {
        this.creditLineUsagePct = creditLineUsagePct;
        this.daysPastDue = daysPastDue;
        this.avgPaymentDelayDays = avgPaymentDelayDays;
        this.lastProfileUpdateTs = System.currentTimeMillis();
    }

    public void updateFromCustomer(String segment, String activePattern,
                                   String riskClass, double clvScore) {
        this.segment = segment;
        this.activePattern = activePattern;
        this.clvScore = clvScore;
        this.lastProfileUpdateTs = System.currentTimeMillis();
    }

    public void updateFromAppEvent(String screenName, String featureCategory,
                                   boolean isPushOpened, int sessionDurationS, int screensVisitedN, long eventTs) {
        if (featureCategory != null) {
            featureCategoryDistribution.merge(featureCategory, 1, Integer::sum);
        }

        if (avgSessionDuration30d > 0 && sessionDurationS > 0) {
            sessionDurationDeltaPct =
                    ((double)(sessionDurationS - avgSessionDuration30d)
                            / avgSessionDuration30d) * 100.0;
        }

        if (screensVisitedAvg7d == 0.0) {
            screensVisitedAvg7d = screensVisitedN;
        } else {
            screensVisitedAvg7d = 0.9 * screensVisitedAvg7d + 0.1 * screensVisitedN;
        }
        if (isPushOpened) pushIgnoreStreak = 0;

        if (screenName != null) {
            if (screenName.contains("mutui") || screenName.contains("simulazione"))
                mortgageSimViews7d++;
            if (screenName.contains("investimenti") || screenName.contains("fondi"))
                investmentViews7d++;
        }

        lastAppEventTs = eventTs;
        lastEventTs = eventTs;
        lastProfileUpdateTs = System.currentTimeMillis();
    }

    public void resetW7(long resetTs) {
        w7.reset();
        knownCounterparts7d.clear();
        distinctCounterparts7d = 0;
        mortgageSimViews7d = 0;
        investmentViews7d = 0;
        screensVisitedAvg7d = 0.0;
        wireCount30d = 0;
        wireFromAppCount30d = 0;
        wireFromAppPct30d = 0.0;
        w7ResetTs = resetTs;
    }


    // =========================================================================
    // METODI DERIVATI — usati dal CEP
    // =========================================================================

    /**
     * Verifica che il trend di accumulo sia valido su una finestra di N bucket mensili.
     * <p>
     * Parametri concordati:
     * - slope minima:          estimatedMonthlyIncome * minSlopeRatio (es. 0.10)
     * - tolleranza negativa:   un mese è "in deviazione" se < slope * 0.70
     * - max deviazioni totali: N / 2 (50% della finestra)
     * - max consecutive:       1 (2 mesi consecutivi in deviazione = trend interrotto)
     *
     * @param buckets       array bucket mensili, ordine cronologico [m-N..m0]
     * @param minSlopeRatio soglia slope minima come % del reddito mensile
     */
    public boolean isAccumulationTrendValid(double[] buckets, double minSlopeRatio) {
        if (buckets == null || buckets.length < 2) return false;
        if (estimatedMonthlyIncome <= 0) return false;

        int n = buckets.length;

        // 1. Slope OLS
        double slope = linearSlope(buckets);

        // 2. Slope minima: almeno X% del reddito mensile per periodo
        double minSlope = estimatedMonthlyIncome * minSlopeRatio;
        if (slope < minSlope) return false;

        // 3. Conta deviazioni: un bucket è in deviazione se < slope * 0.70
        //    (tolleranza 30% in negativo rispetto alla slope media)
        double toleranceLow = slope * 0.70;
        int deviations = 0;
        int consecutiveDevs = 0;
        int maxConsecutiveDevs = 0;

        for (double bucket : buckets) {
            if (bucket < toleranceLow) {
                deviations++;
                consecutiveDevs++;
                maxConsecutiveDevs = Math.max(maxConsecutiveDevs, consecutiveDevs);
            } else {
                consecutiveDevs = 0;
            }
        }

        // 4. Vincoli: max 50% deviazioni, mai 2 consecutive
        int maxDeviations = n / 2;
        if (deviations > maxDeviations) return false;
        if (maxConsecutiveDevs >= 2) return false;

        return true;
    }

    /**
     * Regressione lineare OLS su array di valori equidistanti.
     * Indice 0 = punto più vecchio, indice N-1 = punto più recente.
     */
    public static double linearSlope(double[] values) {
        int n = values.length;
        if (n < 2) return 0.0;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        for (int i = 0; i < n; i++) {
            sumX += i;
            sumY += values[i];
            sumXY += (double) i * values[i];
            sumX2 += (double) i * i;
        }
        double denom = n * sumX2 - sumX * sumX;
        if (denom == 0) return 0.0;
        return (n * sumXY - sumX * sumY) / denom;
    }

    /**
     * Delta saldo corrente vs 30d fa (stima da net flow).
     */
    public double balanceDelta30dPct() {
        if (balance30dAgo == 0) return 0.0;
        return ((currentBalance - balance30dAgo) / Math.abs(balance30dAgo)) * 100.0;
    }

    /**
     * Delta spesa w7 vs media settimanale w30.
     */
    public double spendingDeltaW7vsW30Pct() {
        if (w30AvgAmt == 0) return 0.0;
        double w30Weekly = w30AvgAmt / 4.33;
        return ((w7.getSumAmt() - w30Weekly) / w30Weekly) * 100.0;
    }

    /**
     * Narrowing pre-churn.
     */
    public boolean hasFeatureNarrowing() {
        int essential = featureCategoryDistribution.getOrDefault("essential", 0);
        int exploratory = featureCategoryDistribution.getOrDefault("exploratory", 0);
        int commercial = featureCategoryDistribution.getOrDefault("commercial", 0);
        return essential > 0 && exploratory == 0 && commercial == 0;
    }

    /**
     * Wire anomalo — cliente digitale che fa un bonifico senza app event recente.
     */
    public boolean isWireWithoutDigitalOrigin(long txnTs) {
        if (wireCount30d < 3) return false;
        if (wireFromAppPct30d < 0.80) return false;
        long secondsSinceLastApp = lastAppEventTs > 0
                ? (txnTs - lastAppEventTs) / 1_000L : Long.MAX_VALUE;
        return secondsSinceLastApp > 30;
    }

    /**
     * Uscita singola anomala vs storico 90d.
     */
    public boolean isLargeOutlierOutflow(double amount) {
        if (w90MinAmt == 0.0) return false;
        if (amount >= 0) return false;
        return Math.abs(amount) > Math.abs(w90MinAmt) * 1.5;
    }


    // =========================================================================
    // UTILITY PRIVATE
    // =========================================================================

    private void addSignificantEvent(MiniEvent event) {
        if (recentSignificantEvents.size() >= 10) recentSignificantEvents.pollFirst();
        recentSignificantEvents.addLast(event);
    }

    private boolean isSignificant(double amount, String merchantCat) {
        boolean aboveThreshold = w90AvgAmt > 0 && amount > 2 * w90AvgAmt;
        boolean watchlistCat = switch (merchantCat) {
            case "real_estate", "legal_services", "crypto_exchange",
                 "salary_advance", "investment" -> true;
            default -> false;
        };
        return aboveThreshold || watchlistCat;
    }
}