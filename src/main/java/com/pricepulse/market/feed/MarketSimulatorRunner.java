package com.pricepulse.market.feed;

import com.pricepulse.PricePulseProperties;
import com.pricepulse.market.model.QuoteTick;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
public class MarketSimulatorRunner {

    private static final Logger log = LoggerFactory.getLogger(MarketSimulatorRunner.class);
    private static final long MIN_INTERVAL_MS = 10L;

    private final PricePulseProperties properties;
    private final MarketFeedSource marketFeedSource;
    private final QuoteLaneDispatcher quoteLaneDispatcher;
    private final TaskScheduler taskScheduler;

    private ScheduledFuture<?> scheduledFuture;
    private volatile long intervalMs;

    public MarketSimulatorRunner(PricePulseProperties properties,
                                 MarketFeedSource marketFeedSource,
                                 QuoteLaneDispatcher quoteLaneDispatcher,
                                 TaskScheduler taskScheduler) {
        this.properties = properties;
        this.marketFeedSource = marketFeedSource;
        this.quoteLaneDispatcher = quoteLaneDispatcher;
        this.taskScheduler = taskScheduler;
        this.intervalMs = Math.max(MIN_INTERVAL_MS, properties.getSimulator().getIntervalMs());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (properties.getSimulator().isEnabled()) {
            start(intervalMs);
        }
    }

    public synchronized void start(Long customIntervalMs) {
        if (customIntervalMs != null) {
            validateInterval(customIntervalMs);
            intervalMs = customIntervalMs;
        }
        stop();
        scheduledFuture = taskScheduler.scheduleWithFixedDelay(
                this::emitBatchSafely, Duration.ofMillis(intervalMs));
        log.info("Market simulator started with interval {} ms", intervalMs);
    }

    @PreDestroy
    public synchronized void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduledFuture = null;
            log.info("Market simulator stopped");
        }
    }

    public synchronized void updateInterval(long newIntervalMs) {
        validateInterval(newIntervalMs);
        intervalMs = newIntervalMs;
        if (isRunning()) {
            start(intervalMs);
        }
    }

    public synchronized boolean isRunning() {
        return scheduledFuture != null && !scheduledFuture.isCancelled() && !scheduledFuture.isDone();
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public int emitBatchNow() {
        List<QuoteTick> ticks = marketFeedSource.emitBatch();
        quoteLaneDispatcher.dispatchAll(ticks);
        return ticks.size();
    }

    public int triggerCrashStorm(double dropRatio) {
        if (dropRatio <= 0.0 || dropRatio >= 1.0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "dropRatio must be between 0 and 1");
        }
        List<QuoteTick> ticks = marketFeedSource.triggerCrashStorm(dropRatio);
        quoteLaneDispatcher.dispatchAll(ticks);
        return ticks.size();
    }

    private void emitBatchSafely() {
        try {
            emitBatchNow();
        } catch (RuntimeException ex) {
            log.warn("Simulator batch emit failed: {}", ex.getMessage());
        }
    }

    private static void validateInterval(long value) {
        if (value < MIN_INTERVAL_MS) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "intervalMs must be at least " + MIN_INTERVAL_MS);
        }
    }
}
