package com.marbl.eventsmash.model.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AccountEvent implements Serializable {

    private String accountId;        // UUID
    private String customerId;       // UUID
    private String accountType;      // checking, savings, business
    private String ibanToken;        // tokenizzato
    private String currency;
    private BigDecimal currentBalance;
    private Integer openedDate;      // epoch days
    private String status;           // active, dormant, closed
    private BigDecimal overdraftLimit;
    private Long updatedAt;          // epoch millis
}