package com.marbl.eventsmash.pipelines;

import com.marbl.eventsmash.functions.CardProfileUpdateFunction;
import com.marbl.eventsmash.model.source.CardEvent;
import org.apache.flink.streaming.api.datastream.DataStream;

public class CardProfilePipeline {

    public static void build(DataStream<CardEvent> cardStream) {
        cardStream
                .keyBy(CardEvent::getCustomerId)
                .process(new CardProfileUpdateFunction())
                .setParallelism(6);
    }
}