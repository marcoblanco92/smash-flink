package com.marbl.eventsmash.model.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionEvent implements Serializable {

    // --- identifiers ---
    private String transactionId;
    private String accountId;
    private String customerId;

    // --- dati transazione ---
    private BigDecimal amount;
    private String     currency;          // default EUR
    private String     merchantCategory;  // 25 categorie vocabolario
    private String     channel;           // wire, pos, atm, online, sepa_dd, instant
    private String     counterpartToken;  // mascherato da SMT Debezium

    // --- carta — null per wire/sepa_dd/instant ---
    private String cardId;                // UUID carta usata, null se non applicabile

    // --- timing ---
    private Long    transactionTimestamp; // epoch millis
    private Integer valueDate;            // epoch days

    // --- flags ---
    private Boolean isRecurring;

    // --- ground truth (droppato da Flink prima del Layer 4) ---
    private String patternPhase;
}