package com.marbl.eventsmash.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Aggregati statistici per la finestra w7 — unica finestra calcolata
 * incrementalmente da Flink in real-time sul hot path.
 *
 * Le finestre w30/w90/w180/w365 sono precalcolate da ClickHouse OLAP
 * (con rolling settimanale per w30 e mensile per w90) e memorizzate
 * nel CustomerProfile come campi flat ricevuti da customer.baselines.
 *
 * Algoritmo Welford per std_dev:
 *   std_dev = sqrt((sumSquared / count) - (sumAmt / count)^2)
 *
 * Reset: ogni 7 giorni — gestito dal timer Flink nel CustomerProfileFunction.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WindowStats implements Serializable {

    private double sumAmt       = 0.0;
    private int    count        = 0;
    private double sumSquared   = 0.0;   // per std_dev Welford
    private double maxAmt       = 0.0;
    private int    countAbove2x = 0;     // transazioni > 2x avg90 da baseline

    public void update(double amount, double avg90) {
        sumAmt     += amount;
        count      += 1;
        sumSquared += amount * amount;
        if (amount > maxAmt) maxAmt = amount;
        if (avg90 > 0 && amount > 2 * avg90) countAbove2x++;
    }

    public double avgAmt() {
        return count > 0 ? sumAmt / count : 0.0;
    }

    public double stdDev() {
        if (count < 2) return 0.0;
        double mean     = sumAmt / count;
        double variance = (sumSquared / count) - (mean * mean);
        return variance > 0 ? Math.sqrt(variance) : 0.0;
    }

    public void reset() {
        sumAmt       = 0.0;
        count        = 0;
        sumSquared   = 0.0;
        maxAmt       = 0.0;
        countAbove2x = 0;
    }
}