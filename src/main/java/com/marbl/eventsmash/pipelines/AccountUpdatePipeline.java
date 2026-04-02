package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.AccountUpdateFunction;
import com.marbl.eventsmash.model.source.AccountEvent;
import org.apache.flink.streaming.api.datastream.DataStream;

public class AccountUpdatePipeline {

    public static void build(DataStream<AccountEvent> accountStream) {
        accountStream
                .keyBy(AccountEvent::getCustomerId)
                .process(new AccountUpdateFunction())
                .setParallelism(12);
    }
}