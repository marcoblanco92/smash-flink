package com.marbl.eventsmash;

import com.marbl.eventsmash.connectors.builder.source.KafkaSourceBuilder;
import com.marbl.eventsmash.functions.AIEnrichmentFunction;
import com.marbl.eventsmash.kafka.deserializer.*;
import com.marbl.eventsmash.kafka.serializer.PreEnrichedEventSerializer;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import com.marbl.eventsmash.model.source.*;
import com.marbl.eventsmash.pipelines.CustomerPipeline;
import com.marbl.eventsmash.pipelines.HotPathPipeline;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class SmashJobFlink {

    private static final Logger logger = LoggerFactory.getLogger(SmashJobFlink.class);
    private static final String TOPIC_ENRICHED = "events.enriched";

    public static void main(String[] args) throws Exception {
        logger.info("Starting Flink Job");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");


        // ── Checkpoint — exactly-once, ogni 30s, snapshot su volume montato ──
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        ensureTopicExists(bootstrapServers, TOPIC_ENRICHED, 12, 1);

        // ── Sources — Hot Path ────────────────────────────────
        DataStream<TransactionEvent> transactionStream = KafkaSourceBuilder.<TransactionEvent>of(env)
                .bootstrapServers(bootstrapServers).topic("smash.smash_own.transactions")
                .groupId("transaction-group").parallelism(12).fromLatest()
                .deserializer(new TransactionEventDeserializer()).build("TransactionEvent Source");

        DataStream<AppEvent> appEventStream = KafkaSourceBuilder.<AppEvent>of(env)
                .bootstrapServers(bootstrapServers).topic("smash.smash_own.app_events")
                .groupId("app-event-group").parallelism(12).fromLatest()
                .deserializer(new AppEventDeserializer()).build("AppEvent Source");

        DataStream<MarketDataEvent> marketDataStream = KafkaSourceBuilder.<MarketDataEvent>of(env)
                .bootstrapServers(bootstrapServers).topic("smash.smash_own.market_data")
                .groupId("market-data-group").parallelism(6)
                .deserializer(new MarketDataEventDeserializer()).build("MarketDataEvent Source");

        // ── Sources — Laterali ────────────────────────────────
        DataStream<AccountEvent> accountStream = KafkaSourceBuilder.<AccountEvent>of(env)
                .bootstrapServers(bootstrapServers).topic("smash.smash_own.accounts")
                .groupId("account-group").parallelism(12)
                .deserializer(new AccountEventDeserializer()).build("AccountEvent Source");

        DataStream<CrmProfileEvent> crmProfileStream = KafkaSourceBuilder.<CrmProfileEvent>of(env)
                .bootstrapServers(bootstrapServers).topic("smash.smash_own.crm_profiles")
                .groupId("crm-profile-group").parallelism(12)
                .deserializer(new CrmProfileEventDeserializer()).build("CrmProfileEvent Source");

        DataStream<LoanEvent> loanStream = KafkaSourceBuilder.<LoanEvent>of(env)
                .bootstrapServers(bootstrapServers).topic("smash.smash_own.loans")
                .groupId("loan-group").parallelism(12)
                .deserializer(new LoanEventDeserializer()).build("LoanEvent Source");

        DataStream<CardEvent> cardEventStream = KafkaSourceBuilder.<CardEvent>of(env)
                .bootstrapServers(bootstrapServers).topic("smash.smash_own.cards")
                .groupId("card-event-group").parallelism(6)
                .deserializer(new CardEventDeserializer()).build("CardEvent Source");

        DataStream<CustomerEvent> customerStream = KafkaSourceBuilder.<CustomerEvent>of(env)
                .bootstrapServers(bootstrapServers).topic("smash.smash_own.customers")
                .groupId("customer-group").parallelism(12)
                .deserializer(new CustomerEventDeserializer()).build("CustomerEvent Source");

        // ── Source — Baseline ─────────────────────────────────
        DataStream<CustomerBaseline> customerBaselineStream = KafkaSourceBuilder.<CustomerBaseline>of(env)
                .bootstrapServers(bootstrapServers).topic("customer.baselines")
                .groupId("baselines-group").parallelism(12)
                .deserializer(new BaselineEventDeserializer()).build("CustomerBaseline Source");

        // ── CustomerPipeline — scrive SOLO su Hazelcast ───────
        // Responsabilità unica: aggiornare la cache Hazelcast con ogni
        // nuovo baseline pubblicato da smash-batch.
        // BaselineEnrichmentFunction legge da Hazelcast → hot path
        // riceve sempre la baseline più recente via AsyncIO.
        CustomerPipeline.build(customerBaselineStream);

        // ── HotPathPipeline — non riceve più lo stream baseline ─
        // La baseline entra nel profilo RocksDB via AsyncIO (Hazelcast),
        // non più come stream laterale in processElement2.
        DataStream<PreEnrichedEvent> hotPath = HotPathPipeline.build(
                transactionStream,
                appEventStream,
                marketDataStream,
                accountStream,
                crmProfileStream,
                loanStream,
                cardEventStream,
                customerStream
        );

        // ── Layer 4 — AI Enrichment (async, timeout 250ms) ───────
        // hotPath produce PreEnrichedEvent dal Layer 3
        // AIEnrichmentFunction chiama smash-ai via HTTP non bloccante
        // Output: PreEnrichedEvent con intentClass + riskScore + opportunityScore popolati
        DataStream<PreEnrichedEvent> aiEnrichedStream = AsyncDataStream.unorderedWait(
                hotPath,                          // input: output del Layer 3
                new AIEnrichmentFunction(),
                250,
                java.util.concurrent.TimeUnit.MILLISECONDS,
                100
        );

        // ── Sink — events.enriched ────────────────────────────────
        // Solo gli eventi AI-enriched escono su Kafka
        // Il sink originale su hotPath è rimosso — non bypassa più il Layer 4
        KafkaSink<PreEnrichedEvent> enrichedSink = KafkaSink.<PreEnrichedEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new PreEnrichedEventSerializer())
                .build();

        aiEnrichedStream
                .sinkTo(enrichedSink)
                .name(STR."Kafka Sink → \{TOPIC_ENRICHED}")
                .setParallelism(12);

        env.execute("Smash Event Flink Job");
    }

    private static void ensureTopicExists(String bootstrapServers, String topicName,
                                          int partitions, int replicationFactor) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> existing = admin.listTopics().names().get();
            if (!existing.contains(topicName)) {
                NewTopic topic = new NewTopic(topicName, partitions, (short) replicationFactor);
                admin.createTopics(Collections.singletonList(topic)).all().get();
                logger.info("Topic creato: {} ({} partizioni)", topicName, partitions);
            } else {
                logger.info("Topic già esistente: {}", topicName);
            }
        } catch (Exception e) {
            logger.warn("Impossibile creare topic {}: {}", topicName, e.getMessage());
        }
    }
}