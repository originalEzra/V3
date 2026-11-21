package com.pricepulse.rule.service;

public record RuleChangedEvent(Long ruleId, Long oldStockId) {
}
