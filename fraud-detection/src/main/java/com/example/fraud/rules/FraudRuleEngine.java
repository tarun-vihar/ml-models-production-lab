package com.example.fraud.rules;

import com.example.fraud.config.FraudProperties;
import com.example.fraud.features.FeatureSnapshot;
import com.example.fraud.validation.RawTransaction;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class FraudRuleEngine {
    private final FraudProperties properties;
    public FraudRuleEngine(FraudProperties properties) { this.properties = properties; }

    public RuleResult evaluate(RawTransaction event, FeatureSnapshot history) {
        var matches = new ArrayList<String>();
        if (event.amount().compareTo(properties.rules().highAmountThreshold()) >= 0) matches.add("HIGH_AMOUNT");
        if (history.priorTransactionCount() >= properties.rules().velocityThreshold()) matches.add("HIGH_VELOCITY");
        return new RuleResult(!matches.isEmpty(), List.copyOf(matches));
    }
}
