package com.example.fraud.observability;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

@Configuration
public class MetricsConfig {
    @Bean HealthIndicator kafkaListenerHealth(KafkaListenerEndpointRegistry registry) {
        return () -> registry.getListenerContainers().stream().allMatch(c -> c.isRunning())
                ? Health.up().build()
                : Health.down().withDetail("component", "kafka-listener").build();
    }
}
