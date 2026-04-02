package com.marbl.eventsmash.model.enrich;

import com.marbl.eventsmash.model.CustomerProfile;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Evento arricchito completo — prodotto da CustomerProfileFunction.
 * Pubblicato su topic events.enriched verso il Layer 4 AI Agents.
 *
 * Contiene:
 * - evento originale (transazione, app event, card event)
 * - snapshot del CustomerProfile al momento dell'evento
 * - pattern rilevati dal CEP (popolati da CustomerProfileFunction)
 * - campi AI (popolati progressivamente dal Layer 4)
 *
 * Flusso Layer 4:
 *   Classifier  → popola intentClass
 *   Scorer      → popola riskScore + opportunityScore
 *   Reasoning   → popola urgency + nextAction
 *   Explainer   → popola explanation
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PreEnrichedEvent implements Serializable {

    // ── Evento originale ──────────────────────────────────────
    private EnrichedEvent   event;

    // ── Profilo snapshot al momento dell'evento ───────────────
    // Fotografato dopo l'aggiornamento — include w7 real-time
    // e baseline OLAP più recente da Hazelcast
    private CustomerProfile profileSnapshot;

    // ── CEP output (popolato da CustomerProfileFunction) ──────
    // Codici pattern/insight rilevati — es. ["P-04", "I-16"]
    // Lista vuota = transazione ordinaria → Reasoning emette SILENT
    private List<String>    detectedPatterns = new ArrayList<>();

    // ── Layer 4 AI output (popolato dagli agenti) ─────────────
    private String          intentClass      = null;   // Classifier
    private double          riskScore        = 0.0;    // Scorer
    private double          opportunityScore = 0.0;    // Scorer
    private String          urgency          = "SILENT"; // Reasoning: SILENT|LOW|MEDIUM|HIGH
    private String          nextAction       = "SILENT"; // Reasoning: SILENT|NOTIFY_BANKER|SEND_PUSH_CLIENT|ESCALATE_RISK
    private String          explanation      = null;   // Explainer

    // ── Metadata ──────────────────────────────────────────────
    private long            processedAt      = System.currentTimeMillis();

    // ── Helpers ───────────────────────────────────────────────

    public String getCustomerId() {
        return event != null ? event.getCustomerId() : null;
    }

    public long getEventTimestamp() {
        return event != null ? event.getEventTimestamp() : 0L;
    }

    public boolean hasPatterns() {
        return detectedPatterns != null && !detectedPatterns.isEmpty();
    }

    public void addPattern(String patternCode) {
        if (detectedPatterns == null) detectedPatterns = new ArrayList<>();
        if (!detectedPatterns.contains(patternCode)) {
            detectedPatterns.add(patternCode);
        }
    }
}