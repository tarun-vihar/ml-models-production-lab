package com.example.fraud.producer;

import com.example.fraud.config.FraudProperties;
import com.example.fraud.persistence.TransactionRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class EnrichedEventPublisher {
    private final KafkaTemplate<String, String> kafka;
    private final FraudProperties properties;
    private final TransactionRepository repository;
    private final MeterRegistry metrics;
    public EnrichedEventPublisher(KafkaTemplate<String, String> kafka, FraudProperties properties, TransactionRepository repository, MeterRegistry metrics) {
        this.kafka = kafka; this.properties = properties; this.repository = repository; this.metrics = metrics;
    }

    /** Outbox is committed with PostgreSQL before publication; Kafka delivery can repeat after a crash. */
    @Scheduled(fixedDelayString = "${fraud.outbox-poll-ms:1000}")
    public void publishPending() {
        for (var record : repository.pending(100)) {
            try {
                kafka.send(properties.topics().enriched(), record.transactionId(), record.payload()).get();
                repository.markPublished(record.eventId());
                metrics.counter("fraud.messages.published", "topic", "enriched").increment();
            } catch (Exception e) {
                metrics.counter("fraud.persistence.failures", "operation", "outbox_publish").increment();
                throw new IllegalStateException("Unable to publish outbox event " + record.eventId(), e);
            }
        }
    }
}
