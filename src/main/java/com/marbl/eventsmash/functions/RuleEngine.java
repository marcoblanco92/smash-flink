package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * RuleEngine — maps (detectedPatterns + scores) → urgency + nextAction.
 *
 * Pure deterministic logic — no ML, no network, ~1ms.
 * Called synchronously inside AIEnrichmentFunction after smash-ai response.
 *
 * Priority: HIGH > MEDIUM > LOW > SILENT
 * Action:   ESCALATE_RISK > NOTIFY_BANKER > SEND_PUSH_CLIENT > SILENT
 */
public class RuleEngine {

    private static final Logger LOG = LoggerFactory.getLogger(RuleEngine.class);

    // Urgency levels — ordered by severity
    private static final String SILENT         = "SILENT";
    private static final String LOW            = "LOW";
    private static final String MEDIUM         = "MEDIUM";
    private static final String HIGH           = "HIGH";

    // Next actions — ordered by priority
    private static final String ACTION_SILENT       = "SILENT";
    private static final String ACTION_PUSH_CLIENT  = "SEND_PUSH_CLIENT";
    private static final String ACTION_NOTIFY       = "NOTIFY_BANKER";
    private static final String ACTION_ESCALATE     = "ESCALATE_RISK";

    private RuleEngine() {}

    /**
     * Evaluates all detected patterns and sets urgency + nextAction on the event.
     * Applies highest severity win across all patterns.
     */
    public static void evaluate(PreEnrichedEvent event) {
        List<String> patterns = event.getDetectedPatterns();

        if (patterns == null || patterns.isEmpty()) {
            event.setUrgency(SILENT);
            event.setNextAction(ACTION_SILENT);
            return;
        }

        String currentUrgency = SILENT;
        String currentAction  = ACTION_SILENT;

        for (String rawPattern : patterns) {
            // Strip category suffix — "I-07:subscriptions" → "I-07"
            String code = rawPattern.split(":")[0];

            String[] result = mapPattern(code, event.getRiskScore(), event.getOpportunityScore());
            String urgency = result[0];
            String action  = result[1];

            // Keep highest severity
            if (isHigher(urgency, currentUrgency)) {
                currentUrgency = urgency;
            }
            if (isHigherAction(action, currentAction)) {
                currentAction = action;
            }
        }

        event.setUrgency(currentUrgency);
        event.setNextAction(currentAction);

        if (!SILENT.equals(currentUrgency)) {
            LOG.info("RuleEngine [{}] patterns={} urgency={} action={}",
                    event.getCustomerId(), patterns, currentUrgency, currentAction);
        }
    }

    // ── Pattern mapping ───────────────────────────────────────────────────────

    /**
     * Maps a single pattern code to (urgency, nextAction).
     * Scores from smash-ai used as secondary signal for borderline cases.
     */
    private static String[] mapPattern(String code, double riskScore, double oppScore) {
        return switch (code) {

            // ── Pattern Predittivi ────────────────────────────────────────────

            case "P-01" ->
                // Accumulo pre-mutuo — alta opportunità commerciale
                new String[]{ HIGH, ACTION_NOTIFY };

            case "P-02-PHASE1" ->
                // Credit stress early PMI — intervento preventivo
                new String[]{ MEDIUM, ACTION_NOTIFY };

            case "P-02-PHASE2" ->
                // Credit stress confermato PMI — escalation risk
                new String[]{ HIGH, ACTION_ESCALATE };

            case "P-03-PHASE1" ->
                // Churn risk early — engagement proattivo
                new String[]{ MEDIUM, ACTION_PUSH_CLIENT };

            case "P-04" ->
                // Investment intent — opportunità commerciale
                new String[]{ HIGH, ACTION_NOTIFY };

            case "P-05" ->
                // Wire anomalo pre-immobile — segnale acquisto casa
                new String[]{ MEDIUM, ACTION_NOTIFY };

            // ── Insight Controparti ───────────────────────────────────────────

            case "I-01" ->
                // Pagamento controparte > media +20% — anomalia outbound
                new String[]{ LOW, ACTION_NOTIFY };

            case "I-02" ->
                // Pagamento controparte < media -20% — riduzione spesa (ok)
                new String[]{ LOW, ACTION_SILENT };

            case "I-03" ->
                // Incasso controparte > media +20% — entrata anomala PMI
                new String[]{ LOW, ACTION_NOTIFY };

            case "I-04" ->
                // Incasso controparte < media -20% — entrata ridotta (alert)
                new String[]{ MEDIUM, ACTION_NOTIFY };

            // ── Insight Categoria Spesa ───────────────────────────────────────

            case "I-07" ->
                // Categoria spesa +20% vs media 90d — informativo
                new String[]{ LOW, ACTION_SILENT };

            case "I-09" ->
                // Categoria spesa +15% vs media 90d — informativo
                new String[]{ LOW, ACTION_SILENT };

            case "I-10" ->
                // Categoria spesa -15% vs media 90d — informativo
                new String[]{ LOW, ACTION_SILENT };

            case "I-12" ->
                // Categoria > 30% reddito mensile — alert budget cliente
                new String[]{ MEDIUM, ACTION_PUSH_CLIENT };

            // ── Insight Ricorrenze e Abbonamenti ─────────────────────────────

            case "I-13" ->
                // Nuova ricorrenza rilevata — monitoraggio
                new String[]{ LOW, ACTION_SILENT };

            case "I-14" ->
                // Abbonamento aumentato di prezzo — notifica cliente
                new String[]{ LOW, ACTION_PUSH_CLIENT };

            // ── Insight Carta di Credito ──────────────────────────────────────

            case "I-16" ->
                // Plafond > 80% — rischio blocco carta
                new String[]{ MEDIUM, ACTION_NOTIFY };

            case "I-17" ->
                // Carta si blocca entro 4gg — urgente
                new String[]{ HIGH, ACTION_PUSH_CLIENT };

            case "I-18" ->
                // Transazione outlier vs storico 90d — monitoraggio
                new String[]{ LOW, ACTION_SILENT };

            // ── Default ───────────────────────────────────────────────────────
            default -> {
                LOG.debug("RuleEngine: unknown pattern code [{}] — SILENT", code);
                yield new String[]{ SILENT, ACTION_SILENT };
            }
        };
    }

    // ── Priority helpers ──────────────────────────────────────────────────────

    private static boolean isHigher(String candidate, String current) {
        return urgencyRank(candidate) > urgencyRank(current);
    }

    private static boolean isHigherAction(String candidate, String current) {
        return actionRank(candidate) > actionRank(current);
    }

    private static int urgencyRank(String urgency) {
        return switch (urgency) {
            case HIGH   -> 3;
            case MEDIUM -> 2;
            case LOW    -> 1;
            default     -> 0; // SILENT
        };
    }

    private static int actionRank(String action) {
        return switch (action) {
            case ACTION_ESCALATE    -> 3;
            case ACTION_NOTIFY      -> 2;
            case ACTION_PUSH_CLIENT -> 1;
            default                 -> 0; // SILENT
        };
    }
}