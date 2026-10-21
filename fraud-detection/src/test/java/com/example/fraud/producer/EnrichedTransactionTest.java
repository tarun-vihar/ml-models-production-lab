package com.example.fraud.producer;

import static org.junit.jupiter.api.Assertions.*;
import com.example.fraud.rules.RuleResult;
import com.example.fraud.validation.RawTransaction;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EnrichedTransactionTest {
    @Test void historicalValuesAreCopiedFromPreEventSnapshot() {
        var event = new RawTransaction("e", "t", 1, "TRANSACTION_AUTHORIZED", Instant.now(), "a", BigDecimal.TEN, "USD", "m", "d", "p");
        var enriched = EnrichedTransaction.from(event, 4, new BigDecimal("42.50"), new RuleResult(false, java.util.List.of()));
        assertEquals(4, enriched.priorTransactionCount());
        assertEquals(new BigDecimal("42.50"), enriched.priorAmountTotal());
    }
}
