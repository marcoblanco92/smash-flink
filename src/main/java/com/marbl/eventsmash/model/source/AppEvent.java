package com.marbl.eventsmash.model.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppEvent implements Serializable {

    private String eventId;
    private String customerId;
    private String eventType;
    private String screenName;
    private String sessionId;
    private Integer sessionDurationS;
    private Long eventTimestamp;          // epoch millis
    private String deviceType;            // ios, android, web
    private Boolean isPushOpened;
    private String featureCategory;       // essential, exploratory, commercial
    private Short screensVisitedN;
    private Boolean isReturnVisit;
}