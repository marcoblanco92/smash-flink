package com.marbl.eventsmash.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Profilo di una singola controparte nel keyed state RocksDB.
 *
 * Chiave composita: customerId + counterpartToken
 * Memorizzato in CustomerProfile come Map<String, CounterpartProfile>
 * dove la chiave è il counterpartToken.
 *
 * Dimensionamento: ~120 B × 20-50 counterpart = 2.4-6 KB per cliente
 *
 * LOGICA NON IMPLEMENTATA — struttura dati predisposta per implementazione futura:
 * - Detection ricorrenza (CV intervallo < 0.30 → is_recurring)
 * - Detection abbonamento (CV importo < 0.15 → is_subscription)
 * - Timer Flink per pagamenti mancanti (expected_next_date + std_interval × 1.5)
 * - Aggiornamento incrementale Welford per std_dev
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CounterpartProfile implements Serializable {

    // ── Identificatori ──────────────────────────────────────────────────────
    private String counterpartToken;
    private String customerId;

    // ── Classificazione ─────────────────────────────────────────────────────
    // INBOUND = accrediti (stipendio, b2b ricevuti)
    // OUTBOUND = addebiti (bollette, abbonamenti, fornitori)
    private String  direction;       // "INBOUND" | "OUTBOUND"
    private boolean isRecurring;     // true se ≥ 3 pagamenti con CV intervallo < 0.30
    private boolean isSubscription;  // true se recurring + CV importo < 0.15

    // ── Storico importi (ultimi 12 mesi) ────────────────────────────────────
    private int    paymentCount12m;   // numero pagamenti negli ultimi 12 mesi
    private double sumAmount12m;      // somma importi
    private double avgAmount12m;      // sumAmount12m / paymentCount12m
    private double sumSquared12m;     // per std_dev Welford: sqrt(sumSq/n - avg^2)
    private double lastAmount;        // importo dell'ultimo pagamento
    private double minAmount12m;
    private double maxAmount12m;

    // ── Frequenza e timing ──────────────────────────────────────────────────
    private double avgIntervalDays;   // media giorni tra pagamenti consecutivi
    private double stdIntervalDays;   // variabilità dell'intervallo
    private long   expectedNextDate;  // epoch millis: lastDate + avgIntervalDays
    private int    daysOverdue;       // max(0, oggi - expectedNextDate in giorni)
    private long   lastDate;          // epoch millis ultimo pagamento

    // ── Subscription detection ──────────────────────────────────────────────
    private long firstSeenDate;       // epoch millis primo pagamento
    private int  monthsActive;        // mesi con almeno 1 pagamento negli ultimi 12
    private int  consecutiveMonths;   // mesi consecutivi con pagamento (si azzera se salta)

    // ── Merchant info ───────────────────────────────────────────────────────
    private String merchantCategory;  // categoria merchant più frequente per questa controparte

    // ── Timestamp ───────────────────────────────────────────────────────────
    private long lastUpdateTs;        // epoch millis ultimo aggiornamento profilo
}