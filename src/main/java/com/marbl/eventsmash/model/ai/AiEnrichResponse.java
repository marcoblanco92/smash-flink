package com.marbl.eventsmash.model.ai;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Maps the JSON response from smash-ai /enrich endpoint.
 * Jackson deserializes this from the HTTP response body.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AiEnrichResponse {

    private String transactionId;
    private String intentClass;
    private double riskScore;
    private double opportunityScore;
    private int    processingMs;
}