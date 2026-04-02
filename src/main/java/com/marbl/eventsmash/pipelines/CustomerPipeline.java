package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.CustomerBaselineUpdateFunction;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import org.apache.flink.streaming.api.datastream.DataStream;

public class CustomerPipeline {


    public static void build(DataStream<CustomerBaseline> crmProfileStream) {
        crmProfileStream
                .keyBy(CustomerBaseline::getCustomerId)
                .process(new CustomerBaselineUpdateFunction())
                .setParallelism(12);
    }
}
