package com.marbl.eventsmash.model.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CardEvent implements Serializable {

    private String     cardId;
    private String     customerId;
    private String     accountId;
    private String     cardType;          // credit, debit, prepaid
    private String     cardToken;         // mascherato da SMT Debezium (era card_number)
    private BigDecimal plafondLimit;      // null per debit e prepaid
    private BigDecimal plafondUsed;
    private Short      billingCycleDay;   // null per debit e prepaid
    private String     status;            // active, blocked, expired, cancelled
    private Integer    issuedDate;        // epoch days
    private Integer    expiryDate;        // epoch days — null se non scadente
    private Long       updatedAt;         // epoch millis
}