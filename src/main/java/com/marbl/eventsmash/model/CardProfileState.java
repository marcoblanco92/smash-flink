package com.marbl.eventsmash.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Stato di una singola carta nel keyed state RocksDB.
 * Memorizzato in CustomerProfile come Map<String, CardProfileState>
 * dove la chiave è il cardToken.
 *
 * Aggiornato da CardProfileUpdateFunction ad ogni evento CDC su cards.
 * Usato da CustomerProfileFunction per calcolare credit_utilization_alert:
 * - usage_pct alta (> soglia dinamica)
 * - days_until_exhaustion ≤ 4 (basato su avg_daily_spend_7d dal CustomerProfile)
 *
 * Dimensionamento: ~100 B × 1-3 carte = ~300 B per cliente
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CardProfileState implements Serializable {

    // ── Identificatori ──
    private String cardToken;
    private String customerId;
    private String cardType;     // credit, debit, prepaid

    // ── Plafond (solo per credit) ──
    private double plafondLimit;
    private double plafondUsed;
    private double plafondAvailable;  // plafondLimit - plafondUsed
    private double usagePct;          // plafondUsed / plafondLimit * 100

    // ── Ciclo di fatturazione ──
    private int billingCycleDay;  // giorno del mese reset plafond (0 = non applicabile)

    // ── Stato carta ──
    private String status;        // active, blocked, expired, cancelled

    // ── Ring buffer spesa giornaliera (ultimi 7 giorni) ──
    // Popolato dalla CustomerProfileFunction ad ogni transazione POS/online con questa carta
    // Per ora inizializzato vuoto — logica implementata in fase CEP
    private double[] dailySpend7d = new double[7];
    private double   avgDailySpend = 0.0;

    // ── Previsione esaurimento ──
    // days_until_exhaustion = plafondAvailable / avgDailySpend
    // Calcolato al momento dell'alert, non memorizzato
    private long lastUpdateTs;
}