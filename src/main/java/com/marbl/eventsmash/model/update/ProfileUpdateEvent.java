package com.marbl.eventsmash.model.update;

import com.marbl.eventsmash.model.source.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Wrapper union per gli stream di aggiornamento profilo laterali.
 *
 * Tipi gestiti: ACCOUNT, CRM, LOAN, CARD, CUSTOMER.
 * BASELINE è escluso: gestito da CustomerPipeline → Hazelcast → AsyncIO.
 */
@Data
@NoArgsConstructor
public class ProfileUpdateEvent implements Serializable {

    public enum Type {
        ACCOUNT, CRM, LOAN, CARD, CUSTOMER, BASELINE
        // BASELINE incluso nell'enum per retrocompatibilità ma ignorato
        // in processElement2 — vedi CustomerProfileFunction
    }

    private Type   type;
    private String customerId;

    private AccountEvent    accountEvent;
    private CrmProfileEvent crmProfileEvent;
    private LoanEvent       loanEvent;
    private CardEvent       cardEvent;
    private CustomerEvent   customerEvent;

    public static ProfileUpdateEvent fromAccount(AccountEvent e) {
        ProfileUpdateEvent u = new ProfileUpdateEvent();
        u.type = Type.ACCOUNT; u.customerId = e.getCustomerId(); u.accountEvent = e;
        return u;
    }

    public static ProfileUpdateEvent fromCrm(CrmProfileEvent e) {
        ProfileUpdateEvent u = new ProfileUpdateEvent();
        u.type = Type.CRM; u.customerId = e.getCustomerId(); u.crmProfileEvent = e;
        return u;
    }

    public static ProfileUpdateEvent fromLoan(LoanEvent e) {
        ProfileUpdateEvent u = new ProfileUpdateEvent();
        u.type = Type.LOAN; u.customerId = e.getCustomerId(); u.loanEvent = e;
        return u;
    }

    public static ProfileUpdateEvent fromCard(CardEvent e) {
        ProfileUpdateEvent u = new ProfileUpdateEvent();
        u.type = Type.CARD; u.customerId = e.getCustomerId(); u.cardEvent = e;
        return u;
    }

    public static ProfileUpdateEvent fromCustomer(CustomerEvent e) {
        ProfileUpdateEvent u = new ProfileUpdateEvent();
        u.type = Type.CUSTOMER; u.customerId = e.getCustomerId(); u.customerEvent = e;
        return u;
    }
}