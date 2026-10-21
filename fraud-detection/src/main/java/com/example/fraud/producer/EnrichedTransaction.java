package com.example.fraud.producer;

import com.example.fraud.rules.RuleResult;
import com.example.fraud.validation.RawTransaction;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record EnrichedTransaction(int schemaVersion, String eventId, String transactionId, Instant eventTimestamp,
        String accountId, BigDecimal amount, String currency, String merchantId, String deviceId,
        String paymentInstrumentId, long priorTransactionCount, BigDecimal priorAmountTotal,
        boolean suspicious, List<String> matchedRules, Instant enrichedAt) {
    public static EnrichedTransaction from(RawTransaction e, long count, BigDecimal total, RuleResult rules) {
        return new EnrichedTransaction(1, e.eventId(), e.transactionId(), e.eventTimestamp(), e.accountId(), e.amount(),
                e.currency(), e.merchantId(), e.deviceId(), e.paymentInstrumentId(), count, total,
                rules.suspicious(), rules.matchedRules(), Instant.now());
    }
}
