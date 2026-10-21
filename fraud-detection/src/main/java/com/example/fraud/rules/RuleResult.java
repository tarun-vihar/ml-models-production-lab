package com.example.fraud.rules;

import java.util.List;
public record RuleResult(boolean suspicious, List<String> matchedRules) {}
