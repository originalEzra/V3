package com.pricepulse.market.pipeline;

import com.pricepulse.market.mapper.KlineMapper;
import com.pricepulse.market.model.KlineBar;
import com.pricepulse.market.model.QuoteSnapshot;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class KlineAggregator {

    private static final Logger log = LoggerFactory.getLogger(KlineAggregator.class);
    private static final int BATCH_SIZE = 200;

    private final KlineMapper klineMapper;
    private final Map<Long, MutableSlot> activeSlots = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<KlineBar> closedBars = new ConcurrentLinkedQueue<>();
    private final Deque<KlineBar> retryBuffer = new ArrayDeque<>();
    private final ReentrantLock flushLock = new ReentrantLock();
    private final AtomicLong flushFailureCount = new AtomicLong(0);
    private final AtomicLong flushedBarCount = new AtomicLong(0);

    public KlineAggregator(KlineMapper klineMapper) {
        this.klineMapper = klineMapper;
    }

    public void recordTick(QuoteSnapshot snapshot) {
        LocalDateTime minuteTs = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(snapshot.eventTs()), ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.MINUTES);
        MutableSlot slot = activeSlots.computeIfAbsent(snapshot.stockId(), id -> new MutableSlot());
        synchronized (slot) {
            if (slot.bar == null) {
                slot.bar = new MutableBar(snapshot.stockId(), minuteTs, snapshot.price());
                return;
            }
            if (minuteTs.isAfter(slot.bar.minuteTs)) {
                closedBars.offer(slot.bar.toImmutable());
                slot.bar = new MutableBar(snapshot.stockId(), minuteTs, snapshot.price());
            } else if (minuteTs.equals(slot.bar.minuteTs)) {
                slot.bar.update(snapshot.price());
            }
        }
    }

    @Scheduled(fixedDelay = 10_000, initialDelay = 10_000)
    public void scheduledFlush() {
        flushClosedMinutes(true);
    }

    public int flushClosedMinutes(boolean sealExpiredCurrentMinutes) {
        flushLock.lock();
        try {
            List<KlineBar> toPersist = new ArrayList<>(retryBuffer.size() + 64);
            while (!retryBuffer.isEmpty()) {
                toPersist.add(retryBuffer.pollFirst());
            }
            KlineBar closed;
            while ((closed = closedBars.poll()) != null) {
                toPersist.add(closed);
            }
            if (sealExpiredCurrentMinutes) {
                LocalDateTime currentMinute = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
                for (MutableSlot slot : activeSlots.values()) {
                    synchronized (slot) {
                        if (slot.bar != null && slot.bar.minuteTs.isBefore(currentMinute)) {
                            toPersist.add(slot.bar.toImmutable());
                            slot.bar = null;
                        }
                    }
                }
            }
            if (toPersist.isEmpty()) {
                return 0;
            }
            int persisted = 0;
            for (int i = 0; i < toPersist.size(); i += BATCH_SIZE) {
                List<KlineBar> chunk = toPersist.subList(i, Math.min(i + BATCH_SIZE, toPersist.size()));
                try {
                    klineMapper.batchUpsert(chunk);
                    persisted += chunk.size();
                    flushedBarCount.addAndGet(chunk.size());
                } catch (RuntimeException ex) {
                    flushFailureCount.incrementAndGet();
                    for (int j = toPersist.size() - 1; j >= i; j--) {
                        retryBuffer.addFirst(toPersist.get(j));
                    }
                    log.error("Failed to batch upsert kline bars; retained {} bars for next retry",
                            toPersist.size() - i, ex);
                    return persisted;
                }
            }
            return persisted;
        } finally {
            flushLock.unlock();
        }
    }

    public int getPendingFlushCount() {
        flushLock.lock();
        try {
            return retryBuffer.size() + closedBars.size();
        } finally {
            flushLock.unlock();
        }
    }

    public long getFlushFailureCount() {
        return flushFailureCount.get();
    }

    public long getFlushedBarCount() {
        return flushedBarCount.get();
    }

    public void clearStateForTest() {
        flushLock.lock();
        try {
            activeSlots.clear();
            closedBars.clear();
            retryBuffer.clear();
        } finally {
            flushLock.unlock();
        }
    }

    private static final class MutableSlot {
        private MutableBar bar;
    }

    private static final class MutableBar {
        private final Long stockId;
        private final LocalDateTime minuteTs;
        private final BigDecimal openPrice;
        private BigDecimal highPrice;
        private BigDecimal lowPrice;
        private BigDecimal closePrice;

        private MutableBar(Long stockId, LocalDateTime minuteTs, BigDecimal initialPrice) {
            this.stockId = stockId;
            this.minuteTs = minuteTs;
            this.openPrice = initialPrice;
            this.highPrice = initialPrice;
            this.lowPrice = initialPrice;
            this.closePrice = initialPrice;
        }

        private void update(BigDecimal price) {
            if (price.compareTo(highPrice) > 0) {
                highPrice = price;
            }
            if (price.compareTo(lowPrice) < 0) {
                lowPrice = price;
            }
            closePrice = price;
        }

        private KlineBar toImmutable() {
            return new KlineBar(stockId, minuteTs, openPrice, highPrice, lowPrice, closePrice);
        }
    }
}
