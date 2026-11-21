package com.pricepulse.alert.service;

import com.pricepulse.PricePulseProperties;
import com.pricepulse.alert.mapper.OutboxEventMapper;
import com.pricepulse.alert.model.OutboxEvent;
import com.pricepulse.alert.model.OutboxStatus;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int MAX_BATCHES_PER_TICK = 10;

    private final OutboxEventMapper outboxEventMapper;
    private final TransactionTemplate transactionTemplate;
    private final AlertMessageSender alertMessageSender;
    private final PricePulseProperties properties;

    private final AtomicBoolean pollingPaused = new AtomicBoolean(false);
    private final ReentrantLock pollLock = new ReentrantLock();
    private final AtomicLong publishedCount = new AtomicLong(0);
    private final AtomicLong retryScheduledCount = new AtomicLong(0);
    private final AtomicLong staleLeaseIgnoredCount = new AtomicLong(0);

    public OutboxPublisher(OutboxEventMapper outboxEventMapper,
                           TransactionTemplate transactionTemplate,
                           AlertMessageSender alertMessageSender,
                           PricePulseProperties properties) {
        this.outboxEventMapper = outboxEventMapper;
        this.transactionTemplate = transactionTemplate;
        this.alertMessageSender = alertMessageSender;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${pricepulse.outbox.poll-interval-ms:200}",
            initialDelayString = "${pricepulse.outbox.poll-interval-ms:200}")
    public void scheduledPoll() {
        if (!properties.getOutbox().isEnabled() || pollingPaused.get()) {
            return;
        }
        if (!pollLock.tryLock()) {
            return;
        }
        try {
            int batchSize = Math.max(1, properties.getOutbox().getBatchSize());
            for (int i = 0; i < MAX_BATCHES_PER_TICK; i++) {
                if (!properties.getOutbox().isEnabled() || pollingPaused.get()) {
                    return;
                }
                BatchOutcome outcome = publishBatchInternal(batchSize);
                if (outcome.hitSendFailure() || outcome.claimedCount() < batchSize) {
                    return;
                }
            }
        } finally {
            pollLock.unlock();
        }
    }

    public int publishBatchOnce() {
        int batchSize = Math.max(1, properties.getOutbox().getBatchSize());
        return publishBatchInternal(batchSize).publishedCount();
    }

    public List<OutboxEvent> claimBatch(String leaseId, int batchSize) {
        int leaseSeconds = Math.max(1, properties.getOutbox().getLeaseSeconds());
        List<OutboxEvent> claimed = transactionTemplate.execute(status -> {
            List<OutboxEvent> pending = outboxEventMapper.selectPendingForUpdateSkipLocked(batchSize);
            if (pending == null || pending.isEmpty()) {
                return List.of();
            }
            List<Long> ids = pending.stream().map(OutboxEvent::getId).toList();
            outboxEventMapper.lockEvents(ids, leaseId, leaseSeconds);
            for (OutboxEvent event : pending) {
                event.setLockedBy(leaseId);
            }
            return pending;
        });
        return claimed != null ? claimed : List.of();
    }

    public long calculateBackoffSeconds(int currentAttempts) {
        long base = Math.max(1L, properties.getOutbox().getInitialBackoffSeconds());
        long cap = Math.max(base, properties.getOutbox().getMaxBackoffSeconds());
        int shift = Math.min(Math.max(currentAttempts, 0), 20);
        long delay = base * (1L << shift);
        return Math.min(delay, cap);
    }

    public void pausePolling() {
        pollingPaused.set(true);
        pollLock.lock();
        pollLock.unlock();
    }

    public void resumePolling() {
        pollingPaused.set(false);
    }

    public boolean isPollingPaused() {
        return pollingPaused.get();
    }

    public long getPendingCount() {
        return outboxEventMapper.countByStatus(OutboxStatus.PENDING);
    }

    public long getPublishedCount() {
        return publishedCount.get();
    }

    public long getRetryScheduledCount() {
        return retryScheduledCount.get();
    }

    public long getStaleLeaseIgnoredCount() {
        return staleLeaseIgnoredCount.get();
    }

    public void resetMetricsForTest() {
        publishedCount.set(0);
        retryScheduledCount.set(0);
        staleLeaseIgnoredCount.set(0);
    }

    private BatchOutcome publishBatchInternal(int batchSize) {
        String leaseId = UUID.randomUUID().toString();
        List<OutboxEvent> batch = claimBatch(leaseId, batchSize);
        if (batch.isEmpty()) {
            return new BatchOutcome(0, 0, false);
        }

        int sentInBatch = 0;
        for (int i = 0; i < batch.size(); i++) {
            OutboxEvent event = batch.get(i);
            try {
                alertMessageSender.send(event);
                int updated = outboxEventMapper.markSent(event.getId(), leaseId);
                if (updated == 1) {
                    publishedCount.incrementAndGet();
                    sentInBatch++;
                } else {
                    staleLeaseIgnoredCount.incrementAndGet();
                    log.warn("Ignored stale lease when marking outbox event {} SENT (leaseId={})",
                            event.getId(), leaseId);
                }
            } catch (RuntimeException ex) {
                long backoff = calculateBackoffSeconds(event.getAttempts());
                int updated = outboxEventMapper.markRetry(event.getId(), leaseId, backoff);
                if (updated == 1) {
                    retryScheduledCount.incrementAndGet();
                } else {
                    staleLeaseIgnoredCount.incrementAndGet();
                    log.warn("Ignored stale lease when scheduling retry for outbox event {} (leaseId={})",
                            event.getId(), leaseId);
                }
                for (int j = i + 1; j < batch.size(); j++) {
                    OutboxEvent remaining = batch.get(j);
                    outboxEventMapper.releaseLeaseWithDelay(remaining.getId(), leaseId, backoff);
                }
                log.warn("Failed to publish outbox event {} (attempt {}), scheduled retry in {}s and released {} remaining events in batch: {}",
                        event.getId(), event.getAttempts() + 1, backoff, batch.size() - i - 1, ex.getMessage());
                return new BatchOutcome(batch.size(), sentInBatch, true);
            }
        }
        return new BatchOutcome(batch.size(), sentInBatch, false);
    }

    private record BatchOutcome(int claimedCount, int publishedCount, boolean hitSendFailure) {
    }
}
