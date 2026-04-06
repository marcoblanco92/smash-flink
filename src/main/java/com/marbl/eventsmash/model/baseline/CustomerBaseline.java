package com.marbl.eventsmash.model.baseline;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Messaggio pubblicato da smash-batch sul topic customer.baselines.
 *
 * v3 — aggiunge w180: 6 bucket mensili + slope OLS.
 *   Usato da CepEvaluator per rilevare trend di accumulo sostenuto:
 *   - P-01 (mutuo):       trend crescente su 6 mesi (w180)
 *   - P-04 (investimento): trend crescente su 3 mesi (w90)
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerBaseline implements Serializable {

    private String  customerId;
    private long    computedAt;
    private boolean isColdStart;

    // ── w30 ──────────────────────────────────────────────────
    private double   w30SumAmt;
    private int      w30Count;
    private double   w30AvgAmt;
    private double   w30MaxAmt;
    private double   w30MinAmt;
    private double   w30StdDev;
    private double[] w30WeeklySums;    // [w-3, w-2, w-1, w0]
    private double   w30WeeklySlope;

    // ── w90 ──────────────────────────────────────────────────
    private double   w90SumAmt;
    private int      w90Count;
    private double   w90AvgAmt;
    private double   w90MaxAmt;
    private double   w90MinAmt;
    private double   w90StdDev;
    private double[] w90MonthlySums;   // [m-2, m-1, m0]
    private double   w90MonthlySlope;  // OLS slope su 3 punti

    // ── w180 — NUOVO ─────────────────────────────────────────
    private double   w180SumAmt;
    private int      w180Count;
    private double[] w180MonthlySums;  // [m-5, m-4, m-3, m-2, m-1, m0]
    private double   w180MonthlySlope; // OLS slope su 6 punti

    // ── w365 ─────────────────────────────────────────────────
    private double w365SumAmt;
    private int    w365Count;
    private double w365AvgAmt;
    private double w365MaxAmt;
    private double w365MinAmt;
    private double w365StdDev;

    // ── Mappe qualitative (30d) ───────────────────────────────
    private Map<String, Double>  merchantCatAvgAmounts90d;
    private Map<String, Double>  merchantCatAmounts30d;
    private Map<String, Integer> merchantCatCounts30d;
    private Map<String, Integer> channelCounts30d;
    private int    distinctCounterparts30d;
    private double estimatedMonthlyIncome;

    // ── Saldo snapshot ────────────────────────────────────────
    private double balance30dAgo;
}