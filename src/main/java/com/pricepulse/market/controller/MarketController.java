package com.pricepulse.market.controller;

import com.pricepulse.alert.service.AlertNotificationConsumer;
import com.pricepulse.alert.service.AlertTriggerService;
import com.pricepulse.alert.service.OutboxPublisher;
import com.pricepulse.alert.ws.NotificationWebSocketHandler;
import com.pricepulse.market.feed.MarketSimulatorRunner;
import com.pricepulse.market.model.KlineBar;
import com.pricepulse.market.model.QuoteSnapshot;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.pipeline.KlineAggregator;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import com.pricepulse.market.service.MarketQueryService;
import com.pricepulse.rule.service.RuleIndexMaintainer;
import com.pricepulse.rule.service.RuleMatchEngine;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class MarketController {

    private final MarketQueryService marketQueryService;
    private final QuoteLaneDispatcher quoteLaneDispatcher;
    private final KlineAggregator klineAggregator;
    private final RuleMatchEngine ruleMatchEngine;
    private final RuleIndexMaintainer ruleIndexMaintainer;
    private final MarketSimulatorRunner marketSimulatorRunner;
    private final AlertTriggerService alertTriggerService;
    private final OutboxPublisher outboxPublisher;
    private final AlertNotificationConsumer alertNotificationConsumer;
    private final NotificationWebSocketHandler notificationWebSocketHandler;

    public MarketController(MarketQueryService marketQueryService,
                            QuoteLaneDispatcher quoteLaneDispatcher,
                            KlineAggregator klineAggregator,
                            RuleMatchEngine ruleMatchEngine,
                            RuleIndexMaintainer ruleIndexMaintainer,
                            MarketSimulatorRunner marketSimulatorRunner,
                            AlertTriggerService alertTriggerService,
                            OutboxPublisher outboxPublisher,
                            AlertNotificationConsumer alertNotificationConsumer,
                            NotificationWebSocketHandler notificationWebSocketHandler) {
        this.marketQueryService = marketQueryService;
        this.quoteLaneDispatcher = quoteLaneDispatcher;
        this.klineAggregator = klineAggregator;
        this.ruleMatchEngine = ruleMatchEngine;
        this.ruleIndexMaintainer = ruleIndexMaintainer;
        this.marketSimulatorRunner = marketSimulatorRunner;
        this.alertTriggerService = alertTriggerService;
        this.outboxPublisher = outboxPublisher;
        this.alertNotificationConsumer = alertNotificationConsumer;
        this.notificationWebSocketHandler = notificationWebSocketHandler;
    }

    @GetMapping("/stocks")
    public List<Stock> listStocks(@RequestParam(required = false) String q) {
        return marketQueryService.listStocks(q);
    }

    @GetMapping("/quotes/{symbol}")
    public QuoteSnapshot getLatestQuote(@PathVariable String symbol) {
        return marketQueryService.getLatestQuote(symbol);
    }

    @GetMapping("/klines/{symbol}")
    public List<KlineBar> getKlines(@PathVariable String symbol,
                                    @RequestParam(defaultValue = "60") int limit) {
        return marketQueryService.getKlines(symbol, limit);
    }

    @GetMapping("/system/status")
    public Map<String, Object> getSystemStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("matchingMode", ruleMatchEngine.getCurrentMode());
        status.put("redisIndexReady", ruleMatchEngine.isRedisIndexReady());
        status.put("circuitBreakerState", ruleMatchEngine.getCircuitBreakerState().name());
        status.put("lanes", quoteLaneDispatcher.getLanes());
        status.put("simulatorRunning", marketSimulatorRunner.isRunning());
        status.put("simulatorIntervalMs", marketSimulatorRunner.getIntervalMs());
        status.put("processedQuotes", quoteLaneDispatcher.getProcessedQuoteCount());
        status.put("outOfOrderDiscarded", quoteLaneDispatcher.getOutOfOrderDiscardedCount());
        status.put("droppedQuotes", quoteLaneDispatcher.getDroppedQuoteCount());
        status.put("redisMatchCount", ruleMatchEngine.getRedisMatchCount());
        status.put("fallbackMatchCount", ruleMatchEngine.getFallbackMatchCount());
        status.put("flushedKlineBars", klineAggregator.getFlushedBarCount());
        status.put("dbQueryCount", marketQueryService.getDbQueryCount());
        status.put("maintainerQueueSize", ruleIndexMaintainer.getQueueSize());
        status.put("outboxPendingCount", outboxPublisher.getPendingCount());
        status.put("outboxPublishedCount", outboxPublisher.getPublishedCount());
        status.put("outboxRetryCount", outboxPublisher.getRetryScheduledCount());
        status.put("outboxStaleLeaseCount", outboxPublisher.getStaleLeaseIgnoredCount());
        status.put("triggeredRuleCount", alertTriggerService.getTriggeredCount());
        status.put("optimisticConflictCount", alertTriggerService.getOptimisticConflictCount());
        status.put("consumedNotificationCount", alertNotificationConsumer.getConsumedCount());
        status.put("duplicateNotificationCount", alertNotificationConsumer.getDuplicateCount());
        status.put("wsActiveSessions", notificationWebSocketHandler.getActiveSessionCount());
        return status;
    }

    @PostMapping("/simulator/start")
    public Map<String, Object> startSimulator(@RequestParam(required = false) Long intervalMs) {
        marketSimulatorRunner.start(intervalMs);
        return Map.of(
                "running", marketSimulatorRunner.isRunning(),
                "intervalMs", marketSimulatorRunner.getIntervalMs()
        );
    }

    @PostMapping("/simulator/stop")
    public Map<String, Object> stopSimulator() {
        marketSimulatorRunner.stop();
        return Map.of(
                "running", marketSimulatorRunner.isRunning(),
                "intervalMs", marketSimulatorRunner.getIntervalMs()
        );
    }

    @PutMapping("/simulator/rate")
    public Map<String, Object> updateSimulatorRate(@RequestParam long intervalMs) {
        marketSimulatorRunner.updateInterval(intervalMs);
        return Map.of(
                "running", marketSimulatorRunner.isRunning(),
                "intervalMs", marketSimulatorRunner.getIntervalMs()
        );
    }

    @PostMapping("/simulator/emit")
    public Map<String, Object> emitOnce() {
        int emitted = marketSimulatorRunner.emitBatchNow();
        return Map.of("emitted", emitted);
    }

    @PostMapping("/simulator/crash-storm")
    public Map<String, Object> crashStorm(@RequestParam(defaultValue = "0.05") double dropRatio) {
        int emitted = marketSimulatorRunner.triggerCrashStorm(dropRatio);
        return Map.of(
                "emitted", emitted,
                "dropRatio", dropRatio
        );
    }
}
