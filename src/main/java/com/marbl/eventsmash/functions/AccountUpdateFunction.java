package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.CustomerProfile;
import com.marbl.eventsmash.model.source.AccountEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AccountUpdateFunction extends KeyedProcessFunction<String, AccountEvent, Void> {

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
    public void processElement(AccountEvent accountEvent,
            KeyedProcessFunction<String, AccountEvent, Void>.Context context,
            Collector<Void> collector) throws Exception {

        CustomerProfile profile = profileState.value();

        if (profile == null) {
            profile = new CustomerProfile();
            profile.setCustomerId(accountEvent.getCustomerId());
        }

        // Aggiorna solo il saldo del conto principale (checking o business)
        // Se il cliente ha più conti, sommiamo i saldi attivi
        // Per ora aggiorniamo direttamente — logica multi-conto in fase CEP
        if ("active".equals(accountEvent.getStatus())) {
            profile.updateFromAccount(
                    accountEvent.getCurrentBalance() != null
                            ? accountEvent.getCurrentBalance().doubleValue()
                            : 0.0
            );
        }

        profileState.update(profile);
    }
}