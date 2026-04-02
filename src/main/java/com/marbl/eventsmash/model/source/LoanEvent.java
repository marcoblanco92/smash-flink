package com.marbl.eventsmash.model.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoanEvent implements Serializable {

    private String loanId;
    private String customerId;
    private String loanType;              // mortgage, personal, business, credit_line
    private BigDecimal principalAmount;
    private BigDecimal outstandingBalance;
    private BigDecimal interestRate;
    private Integer startDate;            // epoch days
    private Integer maturityDate;         // epoch days
    private Integer nextDueDate;          // epoch days
    private Integer daysPastDue;
    private BigDecimal creditLineUsagePct; // solo per credit_line
    private Integer avgPaymentDelayDays;
    private String status;                // active, closed, defaulted, restructured
    private String collateralType;        // property, vehicle, securities, none
    private Long updatedAt;               // epoch millis
}