package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.CustomerProfile;
import com.marbl.eventsmash.model.source.LoanEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LoanUpdateFunction extends KeyedProcessFunction<String, LoanEvent, Void> {

    private ValueState<CustomerProfile> profileState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<CustomerProfile> descriptor = new ValueStateDescriptor<>(
                "customer-profile",
                CustomerProfile.class
        );
        profileState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(LoanEvent loanEvent,
            KeyedProcessFunction<String, LoanEvent, Void>.Context context,
            Collector<Void> collector) throws Exception {

        CustomerProfile profile = profileState.value();

        if (profile == null) {
            profile = new CustomerProfile();
            profile.setCustomerId(loanEvent.getCustomerId());
        }

        // Aggiorna i segnali loan nel profilo
        // credit_line_usage_pct: presente solo per loan_type = credit_line
        // Per altri tipi di prestito (mortgage, personal, business) il valore è null
        double creditLineUsagePct = loanEvent.getCreditLineUsagePct() != null
                ? loanEvent.getCreditLineUsagePct().doubleValue()
                : profile.getCreditLineUsagePct();  // mantieni valore esistente se null

        int daysPastDue = loanEvent.getDaysPastDue() != null
                ? loanEvent.getDaysPastDue()
                : profile.getDaysPastDue();

        int avgPaymentDelayDays = loanEvent.getAvgPaymentDelayDays() != null
                ? loanEvent.getAvgPaymentDelayDays()
                : profile.getAvgPaymentDelayDays();

        // Aggiorna has_mortgage se il tipo è mortgage
        if ("mortgage".equals(loanEvent.getLoanType())) {
            profile.setHasMortgage("active".equals(loanEvent.getStatus()));
        }

        profile.updateFromLoan(creditLineUsagePct, daysPastDue, avgPaymentDelayDays);

        profileState.update(profile);
    }
}