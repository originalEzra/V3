package com.pricepulse.market;

import com.pricepulse.PricePulseProperties;
import com.pricepulse.common.PriceCents;
import com.pricepulse.market.mapper.KlineMapper;
import com.pricepulse.market.model.KlineBar;
import com.pricepulse.market.model.QuoteSnapshot;
import com.pricepulse.market.model.QuoteTick;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.pipeline.KlineAggregator;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import com.pricepulse.market.service.StockCatalog;
import com.pricepulse.rule.service.MatchedRuleListener;
import com.pricepulse.rule.service.RuleMatchEngine;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QuotePipelineUnitTest {

    @Test
    void priceCentsRoundTripsExactScale() {
        assertEquals(15025L, PriceCents.toCents(new BigDecimal("150.25")));
        assertEquals(new BigDecimal("150.25"), PriceCents.fromCents(15025L));
        assertThrows(IllegalArgumentException.class, () -> PriceCents.toCents(null));
    }

    @Test
    void dispatcherUsesBoundedQueueThreadNamesAndDiscardsOutOfOrderTicks() {
        PricePulseProperties props = new PricePulseProperties();
        props.setLanes(4);
        props.setLaneQueueCapacity(16);

        StockCatalog catalog = mock(StockCatalog.class);
        when(catalog.getBySymbol("AAPL")).thenReturn(new Stock(1L, "AAPL", "Apple Inc.", "ACTIVE"));

        StringRedisTemplate redis = mockRedis();
        KlineAggregator aggregator = mock(KlineAggregator.class);
        RuleMatchEngine matchEngine = mock(RuleMatchEngine.class);
        AtomicReference<String> workerThreadName = new AtomicReference<>();
        when(matchEngine.match(eq(1L), any(), anyLong())).thenAnswer(inv -> {
            workerThreadName.set(Thread.currentThread().getName());
            return List.of();
        });

        QuoteLaneDispatcher dispatcher = new QuoteLaneDispatcher(
                props, catalog, redis, aggregator, matchEngine, emptyListeners());
        try {
            assertInstanceOf(ArrayBlockingQueue.class, dispatcher.getLaneExecutor(0).getQueue());
            assertEquals(16, dispatcher.getLaneExecutor(0).getQueue().remainingCapacity());

            int expectedLane = dispatcher.laneFor("AAPL");
            assertEquals(expectedLane, dispatcher.laneFor(" aapl "));

            dispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("150.00"), 1_000L, 2L)).join();
            assertEquals("quote-lane-" + expectedLane, workerThreadName.get());

            // Lower seq and lower eventTs must both be discarded without overwriting latest price
            dispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("140.00"), 2_000L, 2L)).join();
            dispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("141.00"), 999L, 3L)).join();
            dispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("155.00"), 2_000L, 3L)).join();

            assertEquals(2L, dispatcher.getProcessedQuoteCount());
            assertEquals(2L, dispatcher.getOutOfOrderDiscardedCount());
            assertEquals(new BigDecimal("155.00"), dispatcher.getLatestQuote("AAPL").price());
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    void nonBlockingModeDropsOverflowWithoutHangingCaller() throws Exception {
        PricePulseProperties props = new PricePulseProperties();
        props.setLanes(1);
        props.setLaneQueueCapacity(1);
        props.setBackpressureBlocking(false);

        StockCatalog catalog = mock(StockCatalog.class);
        when(catalog.getBySymbol("AAPL")).thenReturn(new Stock(1L, "AAPL", "Apple Inc.", "ACTIVE"));

        StringRedisTemplate redis = mockRedis();
        KlineAggregator aggregator = mock(KlineAggregator.class);
        RuleMatchEngine matchEngine = mock(RuleMatchEngine.class);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger(0);
        when(matchEngine.match(eq(1L), any(), anyLong())).thenAnswer(inv -> {
            if (calls.incrementAndGet() == 1) {
                firstStarted.countDown();
                releaseFirst.await(5, TimeUnit.SECONDS);
            }
            return List.of();
        });

        QuoteLaneDispatcher dispatcher = new QuoteLaneDispatcher(
                props, catalog, redis, aggregator, matchEngine, emptyListeners());
        try {
            CompletableFuture<Void> f1 = dispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("100.00"), 100L, 1L));
            assertTrue(firstStarted.await(2, TimeUnit.SECONDS));

            CompletableFuture<Void> f2 = dispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("101.00"), 200L, 2L));
            CompletableFuture<Void> f3 = dispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("102.00"), 300L, 3L));

            // Overflow tick must complete immediately rather than hanging
            f3.get(1, TimeUnit.SECONDS);
            releaseFirst.countDown();
            CompletableFuture.allOf(f1, f2, f3).get(2, TimeUnit.SECONDS);

            assertEquals(1L, dispatcher.getDroppedQuoteCount());
            assertEquals(2L, dispatcher.getProcessedQuoteCount());
        } finally {
            releaseFirst.countDown();
            dispatcher.shutdown();
        }
    }

    @Test
    void redisHashWriteErrorDoesNotBlockMatchingOrKlinePipeline() {
        PricePulseProperties props = new PricePulseProperties();
        props.setLanes(2);

        StockCatalog catalog = mock(StockCatalog.class);
        when(catalog.getBySymbol("NVDA")).thenReturn(new Stock(3L, "NVDA", "NVIDIA", "ACTIVE"));

        @SuppressWarnings("unchecked")
        HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
        doThrow(new RuntimeException("redis unavailable")).when(hashOps).putAll(anyString(), anyMap());
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForHash()).thenReturn(hashOps);

        KlineAggregator aggregator = mock(KlineAggregator.class);
        RuleMatchEngine matchEngine = mock(RuleMatchEngine.class);
        when(matchEngine.match(3L, new BigDecimal("130.50"), 13050L)).thenReturn(List.of(99L));

        MatchedRuleListener listener = mock(MatchedRuleListener.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<MatchedRuleListener> provider = mock(ObjectProvider.class);
        when(provider.orderedStream()).thenAnswer(inv -> Stream.of(listener));

        QuoteLaneDispatcher dispatcher = new QuoteLaneDispatcher(
                props, catalog, redis, aggregator, matchEngine, provider);
        try {
            dispatcher.dispatch(new QuoteTick("NVDA", new BigDecimal("130.50"), 5_000L, 1L)).join();

            assertEquals(1L, dispatcher.getRedisQuoteWriteErrorCount());
            assertEquals(1L, dispatcher.getProcessedQuoteCount());
            assertNotNull(dispatcher.getLatestQuote("NVDA"));
            verify(matchEngine).recordExternalRedisError(any(RuntimeException.class));
            verify(aggregator).recordTick(any(QuoteSnapshot.class));
            verify(listener).onMatchedRules(any(QuoteSnapshot.class), eq(List.of(99L)));
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    void klineAggregatorAggregatesOhlcAndRetainsFailedBatchForRetry() {
        KlineMapper klineMapper = mock(KlineMapper.class);
        KlineAggregator aggregator = new KlineAggregator(klineMapper);

        long m1Start = Instant.parse("2025-02-01T10:00:05Z").toEpochMilli();
        long m1Mid = Instant.parse("2025-02-01T10:00:25Z").toEpochMilli();
        long m1Low = Instant.parse("2025-02-01T10:00:40Z").toEpochMilli();
        long m1End = Instant.parse("2025-02-01T10:00:55Z").toEpochMilli();
        long m2Start = Instant.parse("2025-02-01T10:01:05Z").toEpochMilli();

        aggregator.recordTick(new QuoteSnapshot(1L, "AAPL", new BigDecimal("100.00"), 10000L, m1Start, 1L));
        aggregator.recordTick(new QuoteSnapshot(1L, "AAPL", new BigDecimal("105.00"), 10500L, m1Mid, 2L));
        aggregator.recordTick(new QuoteSnapshot(1L, "AAPL", new BigDecimal("98.00"), 9800L, m1Low, 3L));
        aggregator.recordTick(new QuoteSnapshot(1L, "AAPL", new BigDecimal("102.00"), 10200L, m1End, 4L));
        aggregator.recordTick(new QuoteSnapshot(1L, "AAPL", new BigDecimal("103.00"), 10300L, m2Start, 5L));

        when(klineMapper.batchUpsert(anyList()))
                .thenThrow(new RuntimeException("mysql transient failure"))
                .thenReturn(1);
        assertEquals(0, aggregator.flushClosedMinutes(false));
        assertEquals(1L, aggregator.getFlushFailureCount());
        assertEquals(1, aggregator.getPendingFlushCount());

        assertEquals(1, aggregator.flushClosedMinutes(false));
        assertEquals(0, aggregator.getPendingFlushCount());
        assertEquals(1L, aggregator.getFlushedBarCount());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<KlineBar>> captor = ArgumentCaptor.forClass(List.class);
        verify(klineMapper, org.mockito.Mockito.times(2)).batchUpsert(captor.capture());
        KlineBar flushed = captor.getValue().get(0);
        assertEquals(new BigDecimal("100.00"), flushed.getOpenPrice());
        assertEquals(new BigDecimal("105.00"), flushed.getHighPrice());
        assertEquals(new BigDecimal("98.00"), flushed.getLowPrice());
        assertEquals(new BigDecimal("102.00"), flushed.getClosePrice());
    }

    @SuppressWarnings("unchecked")
    private static StringRedisTemplate mockRedis() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashOps);
        return redis;
    }

    @SuppressWarnings("unchecked")
    private static ObjectProvider<MatchedRuleListener> emptyListeners() {
        ObjectProvider<MatchedRuleListener> provider = mock(ObjectProvider.class);
        when(provider.orderedStream()).thenAnswer(inv -> Stream.empty());
        return provider;
    }
}
