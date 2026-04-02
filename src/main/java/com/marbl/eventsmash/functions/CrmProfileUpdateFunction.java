package com.marbl.eventsmash.functions;

import com.marbl.eventsmash.model.CustomerProfile;
import com.marbl.eventsmash.model.source.CrmProfileEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CrmProfileUpdateFunction extends KeyedProcessFunction<String, CrmProfileEvent, Void> {

    private ValueState<CustomerProfile> profileState;


    @Override
    public void processElement(CrmProfileEvent crmProfileEvent,
                               KeyedProcessFunction<String, CrmProfileEvent, Void>.Context context,
                               Collector<Void> collector) throws Exception {

        CustomerProfile profile = profileState.value();

        if (profile == null) {
            profile = new CustomerProfile();
            profile.setCustomerId(crmProfileEvent.getCustomerId());
        }

        profile.updateFromCrm(
                crmProfileEvent.getSegment(),
                crmProfileEvent.getHasMortgage()    != null && crmProfileEvent.getHasMortgage(),
                crmProfileEvent.getHasInvestments() != null && crmProfileEvent.getHasInvestments(),
                crmProfileEvent.getClvScore()       != null ? crmProfileEvent.getClvScore().doubleValue() : 0.0,
                crmProfileEvent.getPushOptIn()      != null && crmProfileEvent.getPushOptIn(),
                crmProfileEvent.getSegment(),
                crmProfileEvent.getRelationshipMgr() != null,
                crmProfileEvent.getAvgSessionDuration30d() != null ? crmProfileEvent.getAvgSessionDuration30d() : 0,
                crmProfileEvent.getPushIgnoreStreak()      != null ? crmProfileEvent.getPushIgnoreStreak()      : 0
        );

        profileState.update(profile);
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<CustomerProfile> descriptor = new ValueStateDescriptor<>(
                "customer-profile",
                CustomerProfile.class
        );
        profileState = getRuntimeContext().getState(descriptor);
    }
}
