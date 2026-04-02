package com.marbl.eventsmash;

import com.marbl.eventsmash.connectors.builder.source.KafkaSourceBuilder;
import com.marbl.eventsmash.kafka.deserializer.*;
import com.marbl.eventsmash.kafka.serializer.PreEnrichedEventSerializer;
import com.marbl.eventsmash.model.baseline.CustomerBaseline;
import com.marbl.eventsmash.model.enrich.PreEnrichedEvent;
import com.marbl.eventsmash.model.source.*;
import com.marbl.eventsmash.pipelines.*;
import org.apache.flink.connector.kafka.sink.KafkaSink;
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

        // ── Creazione topic events.enriched (se non esiste) ──────────────────
        ensureTopicExists(bootstrapServers, TOPIC_ENRICHED, 12, 1);

        // ── Sources — Hot Path ────────────────────────────────────────────────
        DataStream<TransactionEvent> transactionStream = KafkaSourceBuilder.<TransactionEvent>of(env)
                .bootstrapServers(bootstrapServers)
                .topic("smash.smash_own.transactions")
                .groupId("transaction-group")
                .parallelism(12)
                .deserializer(new TransactionEventDeserializer())
                .build("TransactionEvent Source");

        DataStream<AppEvent> appEventStream = KafkaSourceBuilder.<AppEvent>of(env)
                .bootstrapServers(bootstrapServers)
                .topic("smash.smash_own.app_events")
                .groupId("app-event-group")
                .parallelism(12)
                .deserializer(new AppEventDeserializer())
                .build("AppEvent Source");

        DataStream<MarketDataEvent> marketDataStream = KafkaSourceBuilder.<MarketDataEvent>of(env)
                .bootstrapServers(bootstrapServers)
                .topic("smash.smash_own.market_data")
                .groupId("market-data-group")
                .parallelism(6)
                .deserializer(new MarketDataEventDeserializer())
                .build("MarketDataEvent Source");

        // ── Sources — Lateral State ───────────────────────────────────────────
        DataStream<CrmProfileEvent> crmProfileStream = KafkaSourceBuilder.<CrmProfileEvent>of(env)
                .bootstrapServers(bootstrapServers)
                .topic("smash.smash_own.crm_profiles")
                .groupId("crm-profile-group")
                .parallelism(12)
                .deserializer(new CrmProfileEventDeserializer())
                .build("CrmProfileEvent Source");

        DataStream<AccountEvent> accountStream = KafkaSourceBuilder.<AccountEvent>of(env)
                .bootstrapServers(bootstrapServers)
                .topic("smash.smash_own.accounts")
                .groupId("account-group")
                .parallelism(12)
                .deserializer(new AccountEventDeserializer())
                .build("AccountEvent Source");

        DataStream<LoanEvent> loanStream = KafkaSourceBuilder.<LoanEvent>of(env)
                .bootstrapServers(bootstrapServers)
                .topic("smash.smash_own.loans")
                .groupId("loan-group")
                .parallelism(12)
                .deserializer(new LoanEventDeserializer())
                .build("LoanEvent Source");

        DataStream<CardEvent> cardEventStream = KafkaSourceBuilder.<CardEvent>of(env)
                .bootstrapServers(bootstrapServers)
                .topic("smash.smash_own.cards")
                .groupId("card-event-group")
                .parallelism(6)
                .deserializer(new CardEventDeserializer())
                .build("CardEvent Source");

        // ── Sources — Baseline OLAP ───────────────────────────────────────────
        DataStream<CustomerBaseline> customerBaselineStream = KafkaSourceBuilder.<CustomerBaseline>of(env)
                .bootstrapServers(bootstrapServers)
                .topic("customer.baselines")
                .groupId("baselines-group")
                .parallelism(12)
                .deserializer(new BaselineEventDeserializer())
                .build("CustomerBaseline Source");

        // ── Pipelines — Lateral State ─────────────────────────────────────────
        CrmProfilePipeline.build(crmProfileStream);
        AccountUpdatePipeline.build(accountStream);
        LoanUpdatePipeline.build(loanStream);
        CardProfilePipeline.build(cardEventStream);
        CustomerPipeline.build(customerBaselineStream);

        // ── Pipeline — Hot Path ───────────────────────────────────────────────
        DataStream<PreEnrichedEvent> hotPath = HotPathPipeline.build(
                transactionStream,
                appEventStream,
                marketDataStream
        );

        // ── Sink — events.enriched ────────────────────────────────────────────
        // Key: customerId → ordinamento per cliente sulla stessa partizione
        // Value: JSON dell'EnrichedEvent completo
        // Downstream: Layer 4 AI Agents (Classifier, Scorer, Reasoning, Explainer)
        KafkaSink<PreEnrichedEvent> enrichedSink = KafkaSink.<PreEnrichedEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new PreEnrichedEventSerializer())
                .build();

        hotPath.sinkTo(enrichedSink)
                .name("Kafka Sink → " + TOPIC_ENRICHED)
                .setParallelism(12);

        env.execute("Smash Event Flink Job");
    }

    /**
     * Crea il topic Kafka se non esiste già.
     * Evita di affidarsi all'auto-create di Kafka che usa 1 partizione di default.
     */
    private static void ensureTopicExists(String bootstrapServers,
                                          String topicName,
                                          int partitions,
                                          int replicationFactor) {
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