package com.marbl.eventsmash.model.enrich;

import com.marbl.eventsmash.model.source.AppEvent;
import com.marbl.eventsmash.model.source.CardEvent;
import com.marbl.eventsmash.model.source.TransactionEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrichedEvent implements Serializable {

    private String           eventType;    // "TRANSACTION" | "APP" | "CARD"
    private TransactionEvent transaction;
    private AppEvent         appEvent;
    private CardEvent        cardEvent;

    public static EnrichedEvent ofTransaction(TransactionEvent event) {
        EnrichedEvent e = new EnrichedEvent();
        e.setEventType("TRANSACTION");
        e.setTransaction(event);
        return e;
    }

    public static EnrichedEvent ofApp(AppEvent event) {
        EnrichedEvent e = new EnrichedEvent();
        e.setEventType("APP");
        e.setAppEvent(event);
        return e;
    }

    public static EnrichedEvent ofCard(CardEvent event) {
        EnrichedEvent e = new EnrichedEvent();
        e.setEventType("CARD");
        e.setCardEvent(event);
        return e;
    }

    public String getCustomerId() {
        return switch (eventType) {
            case "TRANSACTION" -> transaction != null ? transaction.getCustomerId() : null;
            case "APP"         -> appEvent    != null ? appEvent.getCustomerId()    : null;
            case "CARD"        -> cardEvent   != null ? cardEvent.getCustomerId()   : null;
            default            -> null;
        };
    }

    public long getEventTimestamp() {
        return switch (eventType) {
            case "TRANSACTION" -> transaction != null && transaction.getTransactionTimestamp() != null
                    ? transaction.getTransactionTimestamp() : 0L;
            case "APP"         -> appEvent    != null && appEvent.getEventTimestamp()          != null
                    ? appEvent.getEventTimestamp()          : 0L;
            case "CARD"        -> cardEvent   != null && cardEvent.getUpdatedAt()              != null
                    ? cardEvent.getUpdatedAt()              : 0L;
            default            -> 0L;
        };
    }

    public String getDescription() {
        if ("TRANSACTION".equals(eventType) && transaction != null) {
            return transaction.getDescription();
        }
        // APP_EVENT e CARD_EVENT non hanno description testuale
        return null;
    }

    public String getMerchantCategory() {
        if ("TRANSACTION".equals(eventType) && transaction != null) {
            return transaction.getMerchantCategory();
        }
        return null;
    }

    public String getTransactionId() {
        return switch (eventType != null ? eventType : "") {
            case "TRANSACTION" -> transaction != null ? transaction.getTransactionId() : null;
            case "CARD"        -> cardEvent   != null ? cardEvent.getCardId()          : null;
            case "APP"         -> appEvent    != null ? appEvent.getEventId()          : null;
            default            -> null;
        };
    }

    public BigDecimal getAmount() {
        if ("TRANSACTION".equals(eventType) && transaction != null) {
            return transaction.getAmount();
        }
        return null;
    }
}