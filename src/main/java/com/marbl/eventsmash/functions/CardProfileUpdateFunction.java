package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.CardProfileState;
import com.marbl.eventsmash.model.CustomerProfile;
import com.marbl.eventsmash.model.source.CardEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CardProfileUpdateFunction extends KeyedProcessFunction<String, CardEvent, Void> {

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
    public void processElement(CardEvent cardEvent,
                               KeyedProcessFunction<String, CardEvent, Void>.Context context,
                               Collector<Void> collector) throws Exception {

        CustomerProfile profile = profileState.value();

        if (profile == null) {
            profile = new CustomerProfile();
            profile.setCustomerId(cardEvent.getCustomerId());
        }

        // Aggiorna o crea il CardProfileState per questa carta
        CardProfileState cardProfile = profile.getCardProfiles()
                .computeIfAbsent(cardEvent.getCardToken(), k -> new CardProfileState());

        cardProfile.setCardToken(cardEvent.getCardToken());
        cardProfile.setCustomerId(cardEvent.getCustomerId());
        cardProfile.setCardType(cardEvent.getCardType());
        cardProfile.setBillingCycleDay(
                cardEvent.getBillingCycleDay() != null ? cardEvent.getBillingCycleDay() : 0
        );
        cardProfile.setPlafondLimit(
                cardEvent.getPlafondLimit() != null ? cardEvent.getPlafondLimit().doubleValue() : 0.0
        );
        cardProfile.setPlafondUsed(
                cardEvent.getPlafondUsed() != null ? cardEvent.getPlafondUsed().doubleValue() : 0.0
        );
        cardProfile.setPlafondAvailable(cardProfile.getPlafondLimit() - cardProfile.getPlafondUsed());
        cardProfile.setUsagePct(
                cardProfile.getPlafondLimit() > 0
                        ? (cardProfile.getPlafondUsed() / cardProfile.getPlafondLimit()) * 100.0
                        : 0.0
        );
        cardProfile.setStatus(cardEvent.getStatus());
        cardProfile.setLastUpdateTs(
                cardEvent.getUpdatedAt() != null ? cardEvent.getUpdatedAt() : System.currentTimeMillis()
        );

        profileState.update(profile);
    }
}