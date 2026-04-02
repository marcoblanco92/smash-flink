package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.LoanUpdateFunction;
import com.marbl.eventsmash.model.source.LoanEvent;
import org.apache.flink.streaming.api.datastream.DataStream;

public class LoanUpdatePipeline {

    public static void build(DataStream<LoanEvent> loanStream) {
        loanStream
                .keyBy(LoanEvent::getCustomerId)
                .process(new LoanUpdateFunction())
                .setParallelism(12);
    }
}