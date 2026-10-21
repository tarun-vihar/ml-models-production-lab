package com.example.fraud.rules;

import static org.junit.jupiter.api.Assertions.*;
import com.example.fraud.config.FraudProperties;
import com.example.fraud.features.FeatureSnapshot;
import com.example.fraud.validation.RawTransaction;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class FraudRuleEngineTest {
    private final FraudRuleEngine engine = new FraudRuleEngine(new FraudProperties(null, new FraudProperties.Rules(new BigDecimal("500"), 3)));
    private RawTransaction event(String amount) { return new RawTransaction("e", "t", 1, "TRANSACTION_AUTHORIZED", Instant.now(), "a", new BigDecimal(amount), "USD", "m", "d", "p"); }
    @Test void highAmountRuleIsDeterministicAndThresholdIsInclusive() {
        assertEquals(java.util.List.of("HIGH_AMOUNT"), engine.evaluate(event("500"), new FeatureSnapshot(0, BigDecimal.ZERO)).matchedRules());
        assertFalse(engine.evaluate(event("499.99"), new FeatureSnapshot(0, BigDecimal.ZERO)).suspicious());
    }
    @Test void velocityUsesOnlyTheSuppliedPriorCount() {
        assertFalse(engine.evaluate(event("10"), new FeatureSnapshot(2, BigDecimal.ZERO)).suspicious());
        assertEquals(java.util.List.of("HIGH_VELOCITY"), engine.evaluate(event("10"), new FeatureSnapshot(3, BigDecimal.ZERO)).matchedRules());
    }
}
