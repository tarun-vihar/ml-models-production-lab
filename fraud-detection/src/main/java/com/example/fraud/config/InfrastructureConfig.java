package com.example.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;

@Configuration
@EnableConfigurationProperties(FraudProperties.class)
public class InfrastructureConfig {
    @Bean NewTopic rawTopic(FraudProperties p) { return TopicBuilder.name(p.topics().raw()).partitions(3).replicas(1).build(); }
    @Bean NewTopic enrichedTopic(FraudProperties p) { return TopicBuilder.name(p.topics().enriched()).partitions(3).replicas(1).build(); }
    @Bean NewTopic deadLetterTopic(FraudProperties p) { return TopicBuilder.name(p.topics().deadLetter()).partitions(3).replicas(1).build(); }
    @Bean CommonErrorHandler kafkaErrorHandler() { return new CommonContainerStoppingErrorHandler(); }
}
