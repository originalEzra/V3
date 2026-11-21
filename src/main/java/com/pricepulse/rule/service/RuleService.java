package com.pricepulse.rule.service;

import com.pricepulse.market.model.QuoteSnapshot;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import com.pricepulse.market.service.StockCatalog;
import com.pricepulse.rule.mapper.AlertRuleMapper;
import com.pricepulse.rule.model.AlertRule;
import com.pricepulse.rule.model.Direction;
import com.pricepulse.rule.model.RuleStatus;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class RuleService {

    private final AlertRuleMapper alertRuleMapper;
    private final StockCatalog stockCatalog;
    private final QuoteLaneDispatcher quoteLaneDispatcher;
    private final ApplicationEventPublisher eventPublisher;

    public RuleService(AlertRuleMapper alertRuleMapper,
                       StockCatalog stockCatalog,
                       QuoteLaneDispatcher quoteLaneDispatcher,
                       ApplicationEventPublisher eventPublisher) {
        this.alertRuleMapper = alertRuleMapper;
        this.stockCatalog = stockCatalog;
        this.quoteLaneDispatcher = quoteLaneDispatcher;
        this.eventPublisher = eventPublisher;
    }

    public List<AlertRule> listRules(Long userId, Long stockId) {
        if (userId != null && stockId != null) {
            return alertRuleMapper.findByUserIdAndStockId(userId, stockId);
        }
        if (stockId != null) {
            return alertRuleMapper.findByStockId(stockId);
        }
        if (userId != null) {
            return alertRuleMapper.findByUserId(userId);
        }
        return alertRuleMapper.findActiveAll();
    }

    public List<AlertRule> listRules(Long userId, Long stockId, String symbol) {
        Long resolvedStockId = stockId;
        if (symbol != null && !symbol.isBlank()) {
            Stock stock = stockCatalog.getBySymbol(symbol);
            if (stock == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
            }
            resolvedStockId = stock.getId();
        }
        return listRules(userId, resolvedStockId);
    }

    public AlertRule getRule(Long ruleId) {
        AlertRule rule = alertRuleMapper.findById(ruleId);
        if (rule == null || rule.getStatus() == RuleStatus.DELETED) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Rule not found: " + ruleId);
        }
        return rule;
    }

    @Transactional
    public AlertRule createRule(Long userId, String symbol, Direction direction, BigDecimal thresholdPrice) {
        if (userId == null || symbol == null || symbol.isBlank() || direction == null
                || thresholdPrice == null || thresholdPrice.compareTo(BigDecimal.ZERO) <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "userId, symbol, direction, and positive thresholdPrice are required");
        }
        Stock stock = stockCatalog.getBySymbol(symbol);
        if (stock == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
        }
        BigDecimal normalizedThreshold = thresholdPrice.setScale(2, RoundingMode.HALF_UP);
        validateAgainstTrustedLatestPrice(stock, direction, normalizedThreshold);

        AlertRule rule = new AlertRule();
        rule.setUserId(userId);
        rule.setStockId(stock.getId());
        rule.setDirection(direction);
        rule.setThresholdPrice(normalizedThreshold);
        rule.setStatus(RuleStatus.ACTIVE);
        rule.setVersion(0L);

        alertRuleMapper.insert(rule);
        eventPublisher.publishEvent(new RuleChangedEvent(rule.getId(), null));
        return alertRuleMapper.findById(rule.getId());
    }

    @Transactional
    public AlertRule updateRule(Long ruleId, Direction direction, BigDecimal thresholdPrice, Long expectedVersion) {
        if (direction == null || thresholdPrice == null || thresholdPrice.compareTo(BigDecimal.ZERO) <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "direction and positive thresholdPrice are required");
        }
        AlertRule existing = alertRuleMapper.findById(ruleId);
        if (existing == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Rule not found: " + ruleId);
        }
        if (existing.getStatus() != RuleStatus.ACTIVE) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Rule is not ACTIVE: " + ruleId);
        }
        Stock stock = stockCatalog.getById(existing.getStockId());
        if (stock == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stock not found for rule: " + ruleId);
        }

        BigDecimal normalizedThreshold = thresholdPrice.setScale(2, RoundingMode.HALF_UP);
        validateAgainstTrustedLatestPrice(stock, direction, normalizedThreshold);

        long targetVersion = expectedVersion != null ? expectedVersion : existing.getVersion();
        int updated = alertRuleMapper.updateRuleOptimistic(ruleId, direction, normalizedThreshold, targetVersion);
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Rule modified concurrently or no longer ACTIVE");
        }
        eventPublisher.publishEvent(new RuleChangedEvent(ruleId, existing.getStockId()));
        return alertRuleMapper.findById(ruleId);
    }

    @Transactional
    public void deleteRule(Long ruleId, Long expectedVersion) {
        AlertRule existing = alertRuleMapper.findById(ruleId);
        if (existing == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Rule not found: " + ruleId);
        }
        if (existing.getStatus() != RuleStatus.ACTIVE) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Rule is not ACTIVE: " + ruleId);
        }
        long targetVersion = expectedVersion != null ? expectedVersion : existing.getVersion();
        int updated = alertRuleMapper.softDeleteOptimistic(ruleId, targetVersion);
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Rule modified concurrently or no longer ACTIVE");
        }
        eventPublisher.publishEvent(new RuleChangedEvent(ruleId, existing.getStockId()));
    }

    private void validateAgainstTrustedLatestPrice(Stock stock, Direction direction, BigDecimal threshold) {
        QuoteSnapshot latest = quoteLaneDispatcher.getLatestQuote(stock.getSymbol());
        if (latest == null) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Trusted latest price is not available yet for " + stock.getSymbol()
            );
        }
        boolean alreadySatisfied = (direction == Direction.ABOVE && latest.price().compareTo(threshold) >= 0)
                || (direction == Direction.BELOW && latest.price().compareTo(threshold) <= 0);
        if (alreadySatisfied) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Current price " + latest.price() + " already satisfies " + direction + " threshold " + threshold
            );
        }
    }
}
