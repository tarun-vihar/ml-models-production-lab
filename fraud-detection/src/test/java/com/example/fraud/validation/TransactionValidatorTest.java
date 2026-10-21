package com.example.fraud.validation;

import static org.junit.jupiter.api.Assertions.*;
import jakarta.validation.Validation;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TransactionValidatorTest {
    private final TransactionValidator validator = new TransactionValidator(Validation.buildDefaultValidatorFactory().getValidator());
    private RawTransaction event(int version, String type, BigDecimal amount) {
        return new RawTransaction("e1", "t1", version, type, Instant.now(), "a1", amount, "USD", "m1", "d1", "p1");
    }
    @Test void acceptsAValidVersionOneTransaction() { assertDoesNotThrow(() -> validator.validate(event(1, "TRANSACTION_AUTHORIZED", new BigDecimal("12.30")))); }
    @Test void rejectsUnsupportedSchemaAndType() {
        assertThrows(InvalidTransactionException.class, () -> validator.validate(event(2, "TRANSACTION_AUTHORIZED", BigDecimal.ONE)));
        assertThrows(InvalidTransactionException.class, () -> validator.validate(event(1, "REFUND", BigDecimal.ONE)));
    }
    @Test void rejectsNonPositiveAmountAndInvalidCurrency() {
        var invalid = new RawTransaction("e", "t", 1, "TRANSACTION_AUTHORIZED", Instant.now(), "a", BigDecimal.ZERO, "US", "m", "d", "p");
        assertThrows(InvalidTransactionException.class, () -> validator.validate(invalid));
    }
}
