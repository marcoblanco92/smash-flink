package com.marbl.eventsmash.model.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CrmProfileEvent implements Serializable {

    private String profileId;
    private String customerId;
    private String segment;
    private String productsHeld;          // JSONB → stringa raw, parsing applicativo
    private Boolean hasMortgage;
    private Boolean hasInvestments;
    private BigDecimal clvScore;
    private BigDecimal churnRiskScore;
    private String relationshipMgr;       // UUID
    private Integer lastContactDate;      // epoch days
    private String preferredChannel;      // app, branch, phone, email
    private Boolean pushOptIn;
    private Integer avgSessionDuration30d;
    private Short pushIgnoreStreak;
    private Integer daysSinceLastContact;
    private BigDecimal productUsageScore;
    private Long updatedAt;               // epoch millis
}