package com.pricepulse.rule.service;

import com.pricepulse.common.PriceCents;
import com.pricepulse.market.service.StockCatalog;
import com.pricepulse.rule.mapper.AlertRuleMapper;
import com.pricepulse.rule.model.AlertRule;
import com.pricepulse.rule.model.Direction;
import com.pricepulse.rule.model.RuleStatus;
import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class RuleIndexMaintainer {

    public static final String INDEX_READY_KEY = "pricepulse:v3:index:ready";
    private static final Logger log = LoggerFactory.getLogger(RuleIndexMaintainer.class);

    private final AlertRuleMapper alertRuleMapper;
    private final StockCatalog stockCatalog;
    private final StringRedisTemplate stringRedisTemplate;
    private final ThreadPoolExecutor maintainerExecutor;
    private final AtomicLong incrementalRefreshCount = new AtomicLong(0);
    private final AtomicLong reconcileCount = new AtomicLong(0);

    public RuleIndexMaintainer(AlertRuleMapper alertRuleMapper,
                               StockCatalog stockCatalog,
                               StringRedisTemplate stringRedisTemplate) {
        this.alertRuleMapper = alertRuleMapper;
        this.stockCatalog = stockCatalog;
        this.stringRedisTemplate = stringRedisTemplate;

        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "rule-index-maintainer");
            t.setDaemon(false);
            return t;
        };
        RejectedExecutionHandler blockingHandler = (r, executor) -> {
            while (!executor.isShutdown()) {
                try {
                    if (executor.getQueue().offer(r, 100, TimeUnit.MILLISECONDS)) {
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        };
        this.maintainerExecutor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(4096),
                threadFactory,
                blockingHandler
        );
    }

    public static String aboveKey(Long stockId) {
        return "rules:above:" + stockId;
    }

    public static String belowKey(Long stockId) {
        return "rules:below:" + stockId;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
    public void onRuleChanged(RuleChangedEvent event) {
        enqueueRefresh(event.ruleId(), event.oldStockId());
    }

    public Future<?> enqueueRefresh(Long ruleId, Long oldStockId) {
        return maintainerExecutor.submit(() -> applyRuleRefresh(ruleId, oldStockId));
    }

    @Scheduled(fixedDelayString = "${pricepulse.matching.reconcile-interval-ms:30000}",
            initialDelayString = "${pricepulse.matching.reconcile-interval-ms:30000}")
    public void scheduledReconcile() {
        maintainerExecutor.submit(() -> {
            try {
                reconcileAllOnMaintainerThread(true);
            } catch (RuntimeException ex) {
                log.warn("Scheduled reconciliation failed: {}", ex.getMessage());
            }
        });
    }

    public Future<?> triggerReconcileAsync() {
        return maintainerExecutor.submit(() -> {
            try {
                reconcileAllOnMaintainerThread(true);
            } catch (RuntimeException ex) {
                log.warn("Manual reconciliation failed: {}", ex.getMessage());
            }
        });
    }

    public void rebuildAndAwaitDrain() {
        try {
            try {
                stringRedisTemplate.delete(INDEX_READY_KEY);
            } catch (RuntimeException ignored) {
                // Continue rebuild if key deletion fails.
            }
            Future<?> rebuildFuture = maintainerExecutor.submit(() -> {
                stringRedisTemplate.delete(INDEX_READY_KEY);
                reconcileAllOnMaintainerThread(false);
            });
            rebuildFuture.get();

            Future<?> drainFuture = maintainerExecutor.submit(
                    () -> stringRedisTemplate.opsForValue().set(INDEX_READY_KEY, "1"));
            drainFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while rebuilding rule index", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException("Failed to rebuild rule index", cause);
        }
    }

    public int getQueueSize() {
        return maintainerExecutor.getQueue().size();
    }

    public long getIncrementalRefreshCount() {
        return incrementalRefreshCount.get();
    }

    public long getReconcileCount() {
        return reconcileCount.get();
    }

    public ThreadPoolExecutor getMaintainerExecutor() {
        return maintainerExecutor;
    }

    @PreDestroy
    public void shutdown() {
        maintainerExecutor.shutdown();
        try {
            if (!maintainerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                maintainerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            maintainerExecutor.shutdownNow();
        }
    }

    private void applyRuleRefresh(Long ruleId, Long oldStockId) {
        try {
            AlertRule rule = alertRuleMapper.findById(ruleId);
            String member = String.valueOf(ruleId);
            if (oldStockId != null && (rule == null || !oldStockId.equals(rule.getStockId()))) {
                stringRedisTemplate.opsForZSet().remove(aboveKey(oldStockId), member);
                stringRedisTemplate.opsForZSet().remove(belowKey(oldStockId), member);
            }
            if (rule == null || rule.getStatus() != RuleStatus.ACTIVE) {
                if (rule != null) {
                    stringRedisTemplate.opsForZSet().remove(aboveKey(rule.getStockId()), member);
                    stringRedisTemplate.opsForZSet().remove(belowKey(rule.getStockId()), member);
                }
                incrementalRefreshCount.incrementAndGet();
                return;
            }
            long cents = PriceCents.toCents(rule.getThresholdPrice());
            if (rule.getDirection() == Direction.ABOVE) {
                stringRedisTemplate.opsForZSet().remove(belowKey(rule.getStockId()), member);
                stringRedisTemplate.opsForZSet().add(aboveKey(rule.getStockId()), member, (double) cents);
            } else {
                stringRedisTemplate.opsForZSet().remove(aboveKey(rule.getStockId()), member);
                stringRedisTemplate.opsForZSet().add(belowKey(rule.getStockId()), member, (double) cents);
            }
            incrementalRefreshCount.incrementAndGet();
        } catch (RuntimeException ex) {
            log.warn("Failed incremental rule refresh for rule {}: {}", ruleId, ex.getMessage());
        }
    }

    private void reconcileAllOnMaintainerThread(boolean writeSentinel) {
        List<AlertRule> activeRules = alertRuleMapper.findActiveAll();
        Map<Long, Set<ZSetOperations.TypedTuple<String>>> aboveByStock = new HashMap<>();
        Map<Long, Set<ZSetOperations.TypedTuple<String>>> belowByStock = new HashMap<>();

        for (AlertRule rule : activeRules) {
            String member = String.valueOf(rule.getId());
            double score = PriceCents.toCents(rule.getThresholdPrice());
            DefaultTypedTuple<String> tuple = new DefaultTypedTuple<>(member, score);
            if (rule.getDirection() == Direction.ABOVE) {
                aboveByStock.computeIfAbsent(rule.getStockId(), k -> new HashSet<>()).add(tuple);
            } else {
                belowByStock.computeIfAbsent(rule.getStockId(), k -> new HashSet<>()).add(tuple);
            }
        }

        for (Long stockId : stockCatalog.getAllStockIds()) {
            replaceZSetAtomically(aboveKey(stockId), "rules:tmp:above:" + stockId, aboveByStock.get(stockId));
            replaceZSetAtomically(belowKey(stockId), "rules:tmp:below:" + stockId, belowByStock.get(stockId));
        }

        if (writeSentinel) {
            stringRedisTemplate.opsForValue().set(INDEX_READY_KEY, "1");
        }
        reconcileCount.incrementAndGet();
    }

    private void replaceZSetAtomically(String liveKey,
                                       String tmpKey,
                                       Set<ZSetOperations.TypedTuple<String>> tuples) {
        if (tuples == null || tuples.isEmpty()) {
            stringRedisTemplate.delete(liveKey);
            return;
        }
        stringRedisTemplate.delete(tmpKey);
        stringRedisTemplate.opsForZSet().add(tmpKey, tuples);
        stringRedisTemplate.rename(tmpKey, liveKey);
    }
}
