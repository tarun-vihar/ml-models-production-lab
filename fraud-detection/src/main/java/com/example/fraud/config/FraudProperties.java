package com.example.fraud.config;

import java.math.BigDecimal;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fraud")
public record FraudProperties(Topics topics, Rules rules) {
    public record Topics(String raw, String enriched, String deadLetter, int replicationFactor) {}
    public record Rules(BigDecimal highAmountThreshold, long velocityThreshold) {}
}
