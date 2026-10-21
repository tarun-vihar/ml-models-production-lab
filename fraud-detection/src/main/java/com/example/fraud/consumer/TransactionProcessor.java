package com.example.fraud.consumer;

import com.example.fraud.features.OnlineFeatureStore;
import com.example.fraud.persistence.TransactionRepository;
import com.example.fraud.producer.EnrichedTransaction;
import com.example.fraud.rules.FraudRuleEngine;
import com.example.fraud.validation.RawTransaction;
import com.example.fraud.validation.TransactionValidator;
import tools.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

@Service
public class TransactionProcessor {
    private final ObjectMapper mapper;
    private final TransactionValidator validator;
    private final OnlineFeatureStore features;
    private final FraudRuleEngine rules;
    private final TransactionRepository repository;
    private final MeterRegistry metrics;
    public TransactionProcessor(ObjectMapper mapper, TransactionValidator validator, OnlineFeatureStore features,
            FraudRuleEngine rules, TransactionRepository repository, MeterRegistry metrics) {
        this.mapper = mapper; this.validator = validator; this.features = features; this.rules = rules; this.repository = repository; this.metrics = metrics;
    }

    public void process(String json) throws Exception {
        long started = System.nanoTime();
        RawTransaction event;
        try {
            event = mapper.readValue(json, RawTransaction.class);
            validator.validate(event);
        } catch (tools.jackson.core.JacksonException e) {
            metrics.counter("fraud.validation.failures", "category", "invalid").increment();
            throw new com.example.fraud.validation.InvalidTransactionException("malformed JSON: " + e.getMessage());
        } catch (Exception e) {
            metrics.counter("fraud.validation.failures", "category", "invalid").increment();
            throw e;
        }
        var history = features.snapshotThenRecord(event);
        var outcome = rules.evaluate(event, history);
        if (outcome.suspicious()) outcome.matchedRules().forEach(rule -> metrics.counter("fraud.rule.matches", "rule", rule).increment());
        var enriched = EnrichedTransaction.from(event, history.priorTransactionCount(), history.priorAmountTotal(), outcome);
        try {
            repository.persist(event, enriched);
        } catch (RuntimeException e) {
            metrics.counter("fraud.persistence.failures", "operation", "db_write").increment();
            throw e;
        }
        metrics.counter("fraud.messages.processed", "outcome", "accepted").increment();
        metrics.timer("fraud.processing.latency", "stage", "total").record(System.nanoTime() - started, java.util.concurrent.TimeUnit.NANOSECONDS);
    }
}
