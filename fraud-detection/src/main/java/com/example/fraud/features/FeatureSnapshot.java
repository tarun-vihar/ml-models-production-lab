package com.example.fraud.features;

import java.math.BigDecimal;
public record FeatureSnapshot(long priorTransactionCount, BigDecimal priorAmountTotal) {}
