package com.example.fraud.validation;

import jakarta.validation.Validator;
import java.time.Instant;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class TransactionValidator {
    private final Validator validator;
    public TransactionValidator(Validator validator) { this.validator = validator; }

    public void validate(RawTransaction event) {
        var violations = validator.validate(event);
        if (!violations.isEmpty()) throw new InvalidTransactionException(violations.stream()
                .map(v -> v.getPropertyPath() + " " + v.getMessage()).sorted().collect(Collectors.joining("; ")));
        if (event.schemaVersion() != 1) throw new InvalidTransactionException("unsupported schemaVersion");
        if (!"TRANSACTION_AUTHORIZED".equals(event.eventType())) throw new InvalidTransactionException("unsupported eventType");
        if (event.eventTimestamp().isAfter(Instant.now().plusSeconds(300))) throw new InvalidTransactionException("eventTimestamp is in the future");
    }
}
