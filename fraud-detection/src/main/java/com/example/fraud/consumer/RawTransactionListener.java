package com.example.fraud.consumer;

import com.example.fraud.config.FraudProperties;
import com.example.fraud.deadletter.DeadLetterPublisher;
import com.example.fraud.validation.InvalidTransactionException;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RawTransactionListener {
    private final TransactionProcessor processor;
    private final DeadLetterPublisher deadLetters;
    private final MeterRegistry metrics;
    public RawTransactionListener(TransactionProcessor processor, DeadLetterPublisher deadLetters, MeterRegistry metrics) { this.processor = processor; this.deadLetters = deadLetters; this.metrics = metrics; }

    @KafkaListener(topics = "${fraud.topics.raw}", groupId = "fraud-ingestion-v1")
    public void consume(String payload, Acknowledgment ack) throws Exception {
        metrics.counter("fraud.messages.consumed", "topic", "raw").increment();
        Exception last = null;
        boolean invalid = false;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                processor.process(payload);
                ack.acknowledge();
                return;
            } catch (InvalidTransactionException e) {
                last = e; invalid = true; break;
            } catch (Exception e) {
                last = e;
                if (attempt < 3) {
                    metrics.counter("fraud.processing.retries", "reason", "transient").increment();
                    Thread.sleep(attempt * 250L);
                }
            }
        }
        deadLetters.publish(payload, invalid ? "VALIDATION" : "PROCESSING_EXHAUSTED", last);
        metrics.counter("fraud.deadletter.count", "category", invalid ? "validation" : "processing").increment();
        ack.acknowledge();
    }
}
