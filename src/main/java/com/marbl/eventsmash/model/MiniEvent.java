package com.marbl.eventsmash.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Snapshot compatto di un evento significativo recente.
 * Viene memorizzato negli ultimi 5-10 eventi del CustomerProfile.
 *
 * Un evento è "significativo" se:
 * - amount > p95 della distribuzione storica del cliente
 * - merchant_category è in watchlist (real_estate, legal_services,
 *   crypto_exchange, salary_advance, investment)
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MiniEvent implements Serializable {

    private long   timestamp;      // epoch millis
    private double amount;
    private String merchantCat;
    private String channel;
}