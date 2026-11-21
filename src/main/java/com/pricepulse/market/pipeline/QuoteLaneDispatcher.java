package com.pricepulse.market.pipeline;

import com.pricepulse.PricePulseProperties;
import com.pricepulse.common.PriceCents;
import com.pricepulse.market.model.QuoteSnapshot;
import com.pricepulse.market.model.QuoteTick;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.service.StockCatalog;
import com.pricepulse.rule.service.MatchedRuleListener;
import com.pricepulse.rule.service.RuleMatchEngine;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class QuoteLaneDispatcher {

    private static final Logger log = LoggerFactory.getLogger(QuoteLaneDispatcher.class);

    private final int lanes;
    private final ThreadPoolExecutor[] laneExecutors;
    private final List<Map<String, OrderCursor>> laneOrderCursors;
    private final ConcurrentHashMap<String, QuoteSnapshot> latestQuotes = new ConcurrentHashMap<>();

    private final StockCatalog stockCatalog;
    private final StringRedisTemplate stringRedisTemplate;
    private final KlineAggregator klineAggregator;
    private final RuleMatchEngine ruleMatchEngine;
    private final List<MatchedRuleListener> matchedRuleListeners;

    private final AtomicLong processedQuoteCount = new AtomicLong(0);
    private final AtomicLong outOfOrderDiscardedCount = new AtomicLong(0);
    private final AtomicLong droppedQuoteCount = new AtomicLong(0);
    private final AtomicLong redisQuoteWriteErrorCount = new AtomicLong(0);

    public QuoteLaneDispatcher(PricePulseProperties properties,
                               StockCatalog stockCatalog,
                               StringRedisTemplate stringRedisTemplate,
                               KlineAggregator klineAggregator,
                               RuleMatchEngine ruleMatchEngine,
                               ObjectProvider<MatchedRuleListener> matchedRuleListenersProvider) {
        this.lanes = Math.max(1, properties.getLanes());
        int queueCapacity = Math.max(1, properties.getLaneQueueCapacity());
        boolean blocking = properties.isBackpressureBlocking();

        this.stockCatalog = stockCatalog;
        this.stringRedisTemplate = stringRedisTemplate;
        this.klineAggregator = klineAggregator;
        this.ruleMatchEngine = ruleMatchEngine;
        this.matchedRuleListeners = matchedRuleListenersProvider.orderedStream().toList();

        this.laneExecutors = new ThreadPoolExecutor[lanes];
        this.laneOrderCursors = new ArrayList<>(lanes);

        for (int i = 0; i < lanes; i++) {
            final int laneIndex = i;
            ThreadFactory threadFactory = r -> {
                Thread t = new Thread(r, "quote-lane-" + laneIndex);
                t.setDaemon(false);
                return t;
            };
            RejectedExecutionHandler rejectedHandler = blocking
                    ? (r, executor) -> {
                        while (!executor.isShutdown()) {
                            try {
                                if (executor.getQueue().offer(r, 100, TimeUnit.MILLISECONDS)) {
                                    return;
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                if (r instanceof LaneTask task) {
                                    task.completeDropped();
                                }
                                return;
                            }
                        }
                        if (r instanceof LaneTask task) {
                            task.completeDropped();
                        }
                    }
                    : (r, executor) -> {
                        if (!executor.isShutdown() && executor.getQueue().offer(r)) {
                            return;
                        }
                        if (!executor.isShutdown()) {
                            droppedQuoteCount.incrementAndGet();
                        }
                        if (r instanceof LaneTask task) {
                            task.completeDropped();
                        }
                    };

            laneExecutors[i] = new ThreadPoolExecutor(
                    1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(queueCapacity),
                    threadFactory,
                    rejectedHandler
            );
            laneOrderCursors.add(new HashMap<>());
        }
    }

    public int laneFor(String symbol) {
        String normalized = symbol.trim().toUpperCase(Locale.ROOT);
        int h = normalized.hashCode();
        return Math.floorMod(h ^ (h >>> 16), lanes);
    }

    public CompletableFuture<Void> dispatch(QuoteTick tick) {
        String normalizedSymbol = tick.symbol().trim().toUpperCase(Locale.ROOT);
        QuoteTick normalizedTick = new QuoteTick(normalizedSymbol, tick.price(), tick.eventTs(), tick.seq());
        int lane = laneFor(normalizedSymbol);
        CompletableFuture<Void> future = new CompletableFuture<>();
        LaneTask task = new LaneTask(() -> {
            try {
                processInLane(lane, normalizedTick);
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }, future);
        laneExecutors[lane].execute(task);
        return future;
    }

    public void dispatchAll(List<QuoteTick> ticks) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(ticks.size());
        for (QuoteTick tick : ticks) {
            futures.add(dispatch(tick));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    }

    public QuoteSnapshot getLatestQuote(String symbol) {
        if (symbol == null) {
            return null;
        }
        return latestQuotes.get(symbol.trim().toUpperCase(Locale.ROOT));
    }

    public int getLanes() {
        return lanes;
    }

    public ThreadPoolExecutor getLaneExecutor(int laneIndex) {
        return laneExecutors[laneIndex];
    }

    public long getProcessedQuoteCount() {
        return processedQuoteCount.get();
    }

    public long getOutOfOrderDiscardedCount() {
        return outOfOrderDiscardedCount.get();
    }

    public long getDroppedQuoteCount() {
        return droppedQuoteCount.get();
    }

    public long getRedisQuoteWriteErrorCount() {
        return redisQuoteWriteErrorCount.get();
    }

    public void resetForTest() {
        List<CompletableFuture<Void>> barriers = new ArrayList<>(lanes);
        for (int i = 0; i < lanes; i++) {
            final int laneIdx = i;
            barriers.add(CompletableFuture.runAsync(() -> laneOrderCursors.get(laneIdx).clear(), laneExecutors[i]));
        }
        CompletableFuture.allOf(barriers.toArray(CompletableFuture[]::new)).join();
        latestQuotes.clear();
        processedQuoteCount.set(0);
        outOfOrderDiscardedCount.set(0);
        droppedQuoteCount.set(0);
        redisQuoteWriteErrorCount.set(0);
    }

    @PreDestroy
    public void shutdown() {
        for (ThreadPoolExecutor executor : laneExecutors) {
            executor.shutdown();
        }
        for (ThreadPoolExecutor executor : laneExecutors) {
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }

    private void processInLane(int lane, QuoteTick tick) {
        Stock stock = stockCatalog.getBySymbol(tick.symbol());
        if (stock == null) {
            return;
        }

        Map<String, OrderCursor> cursors = laneOrderCursors.get(lane);
        OrderCursor cursor = cursors.computeIfAbsent(tick.symbol(), s -> new OrderCursor());
        if (tick.seq() <= cursor.lastSeq || tick.eventTs() < cursor.lastEventTs) {
            outOfOrderDiscardedCount.incrementAndGet();
            return;
        }

        cursor.lastSeq = tick.seq();
        cursor.lastEventTs = tick.eventTs();

        BigDecimal normalizedPrice = tick.price().setScale(2, RoundingMode.HALF_UP);
        long priceCents = PriceCents.toCents(normalizedPrice);
        QuoteSnapshot snapshot = new QuoteSnapshot(
                stock.getId(),
                stock.getSymbol(),
                normalizedPrice,
                priceCents,
                tick.eventTs(),
                tick.seq()
        );

        latestQuotes.put(stock.getSymbol(), snapshot);

        try {
            stringRedisTemplate.opsForHash().putAll("quote:latest:" + stock.getSymbol(), Map.of(
                    "stockId", String.valueOf(stock.getId()),
                    "symbol", stock.getSymbol(),
                    "price", normalizedPrice.toPlainString(),
                    "priceCents", String.valueOf(priceCents),
                    "eventTs", String.valueOf(tick.eventTs()),
                    "seq", String.valueOf(tick.seq())
            ));
        } catch (RuntimeException ex) {
            redisQuoteWriteErrorCount.incrementAndGet();
            ruleMatchEngine.recordExternalRedisError(ex);
            log.warn("Failed writing quote hash to Redis for {}, continuing to rule matching: {}",
                    stock.getSymbol(), ex.getMessage());
        }

        klineAggregator.recordTick(snapshot);

        List<Long> matchedRuleIds = ruleMatchEngine.match(stock.getId(), normalizedPrice, priceCents);
        processedQuoteCount.incrementAndGet();

        if (!matchedRuleIds.isEmpty() && !matchedRuleListeners.isEmpty()) {
            for (MatchedRuleListener listener : matchedRuleListeners) {
                listener.onMatchedRules(snapshot, matchedRuleIds);
            }
        }
    }

    private static final class LaneTask implements Runnable {
        private final Runnable action;
        private final CompletableFuture<Void> future;

        private LaneTask(Runnable action, CompletableFuture<Void> future) {
            this.action = action;
            this.future = future;
        }

        @Override
        public void run() {
            action.run();
        }

        private void completeDropped() {
            future.complete(null);
        }
    }

    private static final class OrderCursor {
        private long lastEventTs = Long.MIN_VALUE;
        private long lastSeq = Long.MIN_VALUE;
    }
}
