package com.marbl.eventsmash.model.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketDataEvent implements Serializable {

    private String recordId;
    private String dataType;         // ecb_rate, irs_curve, index, inflation
    private String metricName;
    private BigDecimal value;
    private BigDecimal previousValue;
    private Long recordedAt;         // epoch millis
    private String source;
}