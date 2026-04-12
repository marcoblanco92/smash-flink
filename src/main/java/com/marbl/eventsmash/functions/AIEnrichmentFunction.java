package com.marbl.eventsmash.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marbl.eventsmash.model.CustomerProfile;
import com.marbl.eventsmash.model.ai.AiEnrichResponse;
import com.marbl.eventsmash.model.enrich.EnrichedEvent;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Layer 4 — AI Enrichment via smash-ai (FastAPI).
 *
 * Calls POST localhost:8085/enrich asynchronously.
 * Populates intentClass, riskScore, opportunityScore on PreEnrichedEvent.
 * Timeout: 250ms — on failure, forwards event with safe defaults (scores = 0.0).
 *
 * Uses Java 11+ HttpClient (non-blocking, no extra dependencies).
 */
public class AIEnrichmentFunction
        extends RichAsyncFunction<PreEnrichedEvent, PreEnrichedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AIEnrichmentFunction.class);

    private static final String ENRICH_URL = "http://localhost:8085/enrich";
    private static final int    TIMEOUT_MS = 250;

    // ObjectMapper is thread-safe — one instance, reused across calls
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // HttpClient is thread-safe — created once in open()
    private transient HttpClient httpClient;

    // ── Lifecycle ────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration parameters) {
        // Called once per TaskManager — initialize shared resources here
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(TIMEOUT_MS))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        LOG.info("AIEnrichmentFunction initialized — target: {}", ENRICH_URL);
    }

    // ── Core async logic ─────────────────────────────────────────────────────

    @Override
    public void asyncInvoke(PreEnrichedEvent input,
                            ResultFuture<PreEnrichedEvent> resultFuture) {

        // Build the JSON payload for smash-ai
        String requestBody = buildRequestBody(input);

        LOG.info("Calling smash-ai for customer [{}] url={}",
                input.getCustomerId(), ENRICH_URL);
        LOG.info("Request body: {}", requestBody);

        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(ENRICH_URL))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofMillis(TIMEOUT_MS))
                .build();

        LOG.info("Request headers: {}", httpRequest.headers().map());

        // Send async — does NOT block the Flink thread
        httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() != 200) {
                        throw new RuntimeException("smash-ai returned HTTP " + response.statusCode());
                    }
                    return response.body();
                })
                .thenApply(body -> {
                    try {
                        AiEnrichResponse aiResp = MAPPER.readValue(body, AiEnrichResponse.class);

                        // Populate AI scores
                        input.setIntentClass(aiResp.getIntentClass());
                        input.setRiskScore(aiResp.getRiskScore());
                        input.setOpportunityScore(aiResp.getOpportunityScore());

                        // ← aggiungi questa riga
                        // Rule Engine — deterministic, ~1ms, no network
                        RuleEngine.evaluate(input);

                        LOG.debug("AI enriched [{}] intent={} risk={} opp={} urgency={} action={}",
                                input.getCustomerId(),
                                aiResp.getIntentClass(),
                                aiResp.getRiskScore(),
                                aiResp.getOpportunityScore(),
                                input.getUrgency(),
                                input.getNextAction());

                        return input;

                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse smash-ai response", e);
                    }
                })
                .whenComplete((enriched, ex) -> {
                    if (ex != null) {
                        // ⚠️ Timeout or error — forward event with safe defaults
                        // Never drop events: partial enrichment is better than data loss
                        LOG.warn("AI enrichment failed for [{}]: {} — using defaults",
                                input.getCustomerId(), ex.getMessage());
                        input.setIntentClass("UNKNOWN");
                        input.setRiskScore(0.0);
                        input.setOpportunityScore(0.0);
                    }
                    // Always complete — Flink must receive exactly one result per input
                    resultFuture.complete(Collections.singleton(enriched != null ? enriched : input));
                });
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Builds the JSON payload matching smash-ai's EnrichRequest Pydantic model.
     * Uses Jackson ObjectNode to avoid manual string concatenation.
     */
    private String buildRequestBody(PreEnrichedEvent event) {
        try {
            ObjectNode node = MAPPER.createObjectNode();

            EnrichedEvent enriched = event.getEvent();

            // ── Core event fields ──────────────────────────────────────────
            String txnId = enriched != null && enriched.getTransactionId() != null
                    ? enriched.getTransactionId()
                    : event.getCustomerId() + "-" + event.getEventTimestamp();

            node.put("transactionId", txnId);
            node.put("customerId",    event.getCustomerId());
            node.put("eventType",     enriched != null ? enriched.getEventType() : "UNKNOWN");

            // ── Amount ────────────────────────────────────────────────────
            if (enriched != null && enriched.getAmount() != null) {
                node.put("amount", enriched.getAmount().doubleValue());
            } else {
                node.putNull("amount");
            }

            // ── Testo per Classifier ──────────────────────────────────────
            // description: testo CBS già anonimizzato es. "CARBURANTE/PEDAGGI EUR 308.91"
            if (enriched != null && enriched.getDescription() != null) {
                node.put("description", enriched.getDescription());
            } else {
                node.putNull("description");
            }

            // merchantCategory: vocabolario 25 categorie es. "FUEL_TRANSPORT"
            if (enriched != null && enriched.getMerchantCategory() != null) {
                node.put("categoryCode", enriched.getMerchantCategory());
            } else {
                node.putNull("categoryCode");
            }

            // ── CEP patterns from Layer 3 ─────────────────────────────────
            var patternsArray = node.putArray("detectedPatterns");
            if (event.getDetectedPatterns() != null) {
                event.getDetectedPatterns().forEach(patternsArray::add);
            }

            // ── CustomerProfile snapshot ──────────────────────────────────
            if (event.getProfileSnapshot() != null) {
                CustomerProfile profile = event.getProfileSnapshot();
                node.put("currentBalance",   profile.getCurrentBalance());
                node.put("estimatedMonthlyIncome", profile.getEstimatedMonthlyIncome());
                node.put("hasInvestments",   profile.isHasInvestments());
                node.put("hasMortgage",      profile.isHasMortgage());
                node.put("segment",          profile.getSegment() != null
                        ? profile.getSegment()
                        : "retail");
            } else {
                node.put("hasInvestments", false);
                node.put("hasMortgage",    false);
                node.putNull("segment");
                node.putNull("currentBalance");
                node.putNull("avgMonthlyIncome");
            }

            return MAPPER.writeValueAsString(node);

        } catch (Exception e) {
            LOG.error("Failed to build request body for customer {}", event.getCustomerId(), e);
            return "{\"transactionId\":\"unknown\",\"customerId\":\"unknown\"," +
                    "\"eventType\":\"UNKNOWN\",\"detectedPatterns\":[]," +
                    "\"hasInvestments\":false,\"hasMortgage\":false}";
        }
    }

    @Override
    public void timeout(PreEnrichedEvent input, ResultFuture<PreEnrichedEvent> resultFuture) {
        // Called by Flink when the async operation exceeds the configured timeout
        LOG.warn("AIEnrichmentFunction TIMEOUT for customer [{}]", input.getCustomerId());
        input.setIntentClass("TIMEOUT");
        input.setRiskScore(0.0);
        input.setOpportunityScore(0.0);
        resultFuture.complete(Collections.singleton(input));
    }
}