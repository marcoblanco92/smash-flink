package com.marbl.eventsmash.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Contesto macroeconomico distribuito via Broadcast State a tutti i TaskManager.
 * Aggiornato ad ogni evento dal topic smash.smash_own.market_data.
 *
 * Non è keyed — è lo stesso per tutti i clienti.
 * Flink lo mantiene in memoria su ogni TaskManager tramite BroadcastState.
 *
 * Utilizzo nei pattern:
 * - ecbRateDirection: tassi in discesa → amplifica investment_window
 * - btpBundSpread:    spread in salita → amplifica pmi_silent_stress
 * - inflationRate:    inflazione alta → contesto per real_estate
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketContext implements Serializable {

    // ── Tassi BCE ──
    private double ecbRate;               // tasso corrente
    private double ecbRatePrevious;       // tasso precedente
    private double ecbRateDelta;          // delta: negativo = in discesa
    private String ecbRateDirection;      // "rising" | "falling" | "stable"

    // ── Spread BTP-Bund ──
    private double btpBundSpread;         // spread corrente in bps
    private double btpBundSpreadPrevious;
    private double btpBundSpreadDelta;

    // ── Curva IRS ──
    private double irs10y;                // tasso IRS 10 anni
    private double irs10yPrevious;

    // ── Inflazione ──
    private double inflationRate;
    private double inflationRatePrevious;

    // ── Timestamp ultimo aggiornamento ──
    private long updatedAt;               // epoch millis

    // ── Metodi derivati per CEP ──

    /**
     * True se i tassi BCE sono in discesa — amplifica investment_window.
     */
    public boolean isRateEnvironmentFavorableForInvestment() {
        return ecbRateDelta < 0;
    }

    /**
     * True se lo spread BTP-Bund è sopra soglia critica — amplifica pmi_silent_stress.
     */
    public boolean isSpreadUnderStress(double thresholdBps) {
        return btpBundSpread > thresholdBps;
    }
}