package com.marbl.eventsmash.model.baseline;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Messaggio pubblicato da ClickHouse sul topic customer.baselines.
 * Prodotto ogni ~15 minuti per ogni customer_id attivo.
 * <p>
 * Contiene le baseline precalcolate per w30, w90, w180, w365.
 * Flink lo riceve nel Baseline Updater e aggiorna il CustomerProfile
 * in RocksDB in modo asincrono — non bloccante sul hot path.
 * <p>
 * w7 NON è presente — calcolato incrementalmente da Flink in real-time.
 * <p>
 * Rolling windows:
 * - w30: 4 sub-bucket settimanali (ring buffer) → slope intra-mese
 * - w90: 3 sub-bucket mensili (ring buffer) → slope trimestrale
 * - w180, w365: aggregati piatti — nessun sub-bucket
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerBaseline implements Serializable {

    private String customerId;
    private long computedAt;      // epoch millis — quando ClickHouse ha calcolato
    private boolean isColdStart;

    // ── w30 — finestra 30 giorni con rolling settimanale ──
    private double w30SumAmt;
    private int w30Count;
    private double w30AvgAmt;
    private double w30MaxAmt;
    private double w30StdDev;
    private double[] w30WeeklySums;    // size 4 — ring buffer [w-3, w-2, w-1, w0]
    private double w30WeeklySlope;     // w0 - w3 → trend intra-mese

    // ── w90 — finestra 90 giorni con rolling mensile ──
    private double w90SumAmt;
    private int w90Count;
    private double w90AvgAmt;
    private double w90MaxAmt;
    private double w90StdDev;
    private double[] w90MonthlySums;   // size 3 — ring buffer [m-2, m-1, m0]
    private double w90MonthlySlope;    // m0 - m2 → trend trimestrale

    // ── Media per categoria su 90 giorni ──
    // Calcolata da smash-batch: catAmounts90d / catCounts90d
    // Usata dal CEP per confronto "spesa categoria vs media storica"
    // es: merchantCatAvgAmounts90d = {"grocery": 95.50, "transport": 45.00}
    private Map<String, Double> merchantCatAvgAmounts90d;

    // ── w180 — finestra 180 giorni, aggregato piatto ──
    private double w180SumAmt;
    private int w180Count;
    private double w180AvgAmt;
    private double w180MaxAmt;
    private double w180StdDev;

    // ── w365 — finestra 365 giorni, aggregato piatto ──
    private double w365SumAmt;
    private int w365Count;
    private double w365AvgAmt;
    private double w365MaxAmt;
    private double w365StdDev;

    // ── Composizione qualitativa (da ClickHouse, finestra 30d) ──
    private java.util.Map<String, Double> merchantCatAmounts30d;  // categoria → importo totale
    private java.util.Map<String, Integer> merchantCatCounts30d;  // categoria → numero transazioni
    private java.util.Map<String, Integer> channelCounts30d;
    private int distinctCounterparts30d;
    private double estimatedMonthlyIncome;  // da INBOUND ricorrenti

    // ── Saldo snapshot ──
    private double balance30dAgo;   // saldo 30 giorni fa — per delta real-time
}