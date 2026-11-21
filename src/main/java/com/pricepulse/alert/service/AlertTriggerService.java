package com.pricepulse.alert.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricepulse.alert.mapper.AlertTriggerMapper;
import com.pricepulse.alert.mapper.OutboxEventMapper;
import com.pricepulse.alert.model.AlertTrigger;
import com.pricepulse.alert.model.AlertTriggeredPayload;
import com.pricepulse.alert.model.OutboxEvent;
import com.pricepulse.alert.model.OutboxStatus;
import com.pricepulse.market.model.QuoteSnapshot;
import com.pricepulse.rule.mapper.AlertRuleMapper;
import com.pricepulse.rule.model.AlertRule;
import com.pricepulse.rule.model.Direction;
import com.pricepulse.rule.model.RuleStatus;
import com.pricepulse.rule.service.MatchedRuleListener;
import com.pricepulse.rule.service.RuleChangedEvent;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class AlertTriggerService implements MatchedRuleListener {

    private static final Logger log = LoggerFactory.getLogger(AlertTriggerService.class);
    private static final String EVENT_TYPE_ALERT_TRIGGERED = "ALERT_TRIGGERED";

    private final AlertRuleMapper alertRuleMapper;
    private final AlertTriggerMapper alertTriggerMapper;
    private final OutboxEventMapper outboxEventMapper;
    private final TransactionTemplate transactionTemplate;
    private final ApplicationEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    private final AtomicLong triggeredCount = new AtomicLong(0);
    private final AtomicLong optimisticConflictCount = new AtomicLong(0);

    public AlertTriggerService(AlertRuleMapper alertRuleMapper,
                               AlertTriggerMapper alertTriggerMapper,
                               OutboxEventMapper outboxEventMapper,
                               TransactionTemplate transactionTemplate,
                               ApplicationEventPublisher eventPublisher,
                               ObjectMapper objectMapper) {
        this.alertRuleMapper = alertRuleMapper;
        this.alertTriggerMapper = alertTriggerMapper;
        this.outboxEventMapper = outboxEventMapper;
        this.transactionTemplate = transactionTemplate;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onMatchedRules(QuoteSnapshot snapshot, List<Long> matchedRuleIds) {
        if (snapshot == null || matchedRuleIds == null || matchedRuleIds.isEmpty()) {
            return;
        }
        List<Long> uniqueIds = matchedRuleIds.stream()
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (uniqueIds.isEmpty()) {
            return;
        }
        List<AlertRule> candidates = alertRuleMapper.findByIds(uniqueIds);
        for (AlertRule rule : candidates) {
            if (rule.getStatus() != RuleStatus.ACTIVE
                    || !snapshot.stockId().equals(rule.getStockId())
                    || !isPriceConditionMet(rule, snapshot.price())) {
                continue;
            }
            try {
                tryTriggerRule(rule, snapshot);
            } catch (RuntimeException ex) {
                log.error("Failed to trigger alert rule {} for stock {}: {}",
                        rule.getId(), snapshot.symbol(), ex.getMessage(), ex);
            }
        }
    }

    public boolean tryTriggerRule(AlertRule rule, QuoteSnapshot snapshot) {
        Boolean committed = transactionTemplate.execute(status -> {
            int updated = alertRuleMapper.completeRuleOptimistic(
                    rule.getId(),
                    snapshot.stockId(),
                    rule.getVersion(),
                    snapshot.price()
            );
            if (updated != 1) {
                optimisticConflictCount.incrementAndGet();
                return false;
            }

            LocalDateTime nowUtc = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);

            AlertTrigger trigger = new AlertTrigger();
            trigger.setRuleId(rule.getId());
            trigger.setUserId(rule.getUserId());
            trigger.setStockId(snapshot.stockId());
            trigger.setDirection(rule.getDirection());
            trigger.setThresholdPrice(rule.getThresholdPrice());
            trigger.setTriggerPrice(snapshot.price());
            trigger.setTriggeredAt(nowUtc);
            alertTriggerMapper.insert(trigger);

            String content = String.format("%s triggered %s %s at %s",
                    snapshot.symbol(),
                    rule.getDirection(),
                    rule.getThresholdPrice().toPlainString(),
                    snapshot.price().toPlainString());
            AlertTriggeredPayload payload = new AlertTriggeredPayload(
                    trigger.getId(),
                    rule.getId(),
                    rule.getUserId(),
                    snapshot.stockId(),
                    snapshot.symbol(),
                    rule.getDirection(),
                    rule.getThresholdPrice(),
                    snapshot.price(),
                    nowUtc.toString(),
                    content
            );

            OutboxEvent outboxEvent = new OutboxEvent();
            outboxEvent.setAggregateId(trigger.getId());
            outboxEvent.setEventType(EVENT_TYPE_ALERT_TRIGGERED);
            outboxEvent.setPayload(serializePayload(payload));
            outboxEvent.setStatus(OutboxStatus.PENDING);
            outboxEvent.setAttempts(0);
            outboxEventMapper.insert(outboxEvent);

            eventPublisher.publishEvent(new RuleChangedEvent(rule.getId(), rule.getStockId()));
            return true;
        });

        if (Boolean.TRUE.equals(committed)) {
            triggeredCount.incrementAndGet();
            return true;
        }
        return false;
    }

    public long getTriggeredCount() {
        return triggeredCount.get();
    }

    public long getOptimisticConflictCount() {
        return optimisticConflictCount.get();
    }

    public void resetMetricsForTest() {
        triggeredCount.set(0);
        optimisticConflictCount.set(0);
    }

    private boolean isPriceConditionMet(AlertRule rule, BigDecimal price) {
        if (rule.getDirection() == Direction.ABOVE) {
            return price.compareTo(rule.getThresholdPrice()) >= 0;
        }
        return price.compareTo(rule.getThresholdPrice()) <= 0;
    }

    private String serializePayload(AlertTriggeredPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize AlertTriggeredPayload for trigger " + payload.triggerId(), e);
        }
    }
}
