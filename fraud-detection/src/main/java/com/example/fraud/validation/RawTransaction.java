package com.example.fraud.validation;

import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.time.Instant;

public record RawTransaction(
        @NotBlank @Size(max = 100) String eventId,
        @NotBlank @Size(max = 100) String transactionId,
        @NotNull Integer schemaVersion,
        @NotBlank String eventType,
        @NotNull Instant eventTimestamp,
        @NotBlank @Size(max = 100) String accountId,
        @NotNull @DecimalMin(value = "0.00", inclusive = false) @Digits(integer = 15, fraction = 4) BigDecimal amount,
        @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency,
        @NotBlank @Size(max = 100) String merchantId,
        @NotBlank @Size(max = 150) String deviceId,
        @NotBlank @Size(max = 150) String paymentInstrumentId) {}
