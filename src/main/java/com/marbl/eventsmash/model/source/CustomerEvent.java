package com.marbl.eventsmash.model.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerEvent implements Serializable {

    private String customerId;           // UUID
    private String segment;              // retail, affluent, pmi
    private String patternType;          // real_estate, pmi_deterioration, ...
    private Integer patternTriggerDate;  // epoch days
    private String activePattern;
    private String riskClass;            // low, medium, high
    private BigDecimal clvScore;
    private String relationshipMgr;      // UUID
    private Integer onboardingDate;      // epoch days
    private Boolean isActive;
    private Long createdAt;              // epoch millis
}