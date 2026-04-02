package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.CrmProfileUpdateFunction;
import com.marbl.eventsmash.model.source.CrmProfileEvent;
import com.marbl.eventsmash.model.source.TransactionEvent;
import org.apache.flink.streaming.api.datastream.DataStream;

public class CrmProfilePipeline {

    public static void build(DataStream<CrmProfileEvent> crmProfileStream) {
        crmProfileStream
                .keyBy(CrmProfileEvent::getCustomerId)
                .process(new CrmProfileUpdateFunction())
                .setParallelism(12);
    }

}
