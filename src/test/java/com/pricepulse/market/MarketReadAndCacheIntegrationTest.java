package com.pricepulse.market;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricepulse.PricePulseProperties;
import com.pricepulse.market.feed.MarketSimulatorRunner;
import com.pricepulse.market.model.KlineBar;
import com.pricepulse.market.model.QuoteTick;
import com.pricepulse.market.pipeline.KlineAggregator;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import com.pricepulse.market.service.MarketQueryService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class MarketReadAndCacheIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private PricePulseProperties properties;

    @Autowired
    private QuoteLaneDispatcher quoteLaneDispatcher;

    @Autowired
    private KlineAggregator klineAggregator;

    @Autowired
    private MarketQueryService marketQueryService;

    @Autowired
    private MarketSimulatorRunner marketSimulatorRunner;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    @AfterEach
    void resetState() {
        marketSimulatorRunner.stop();
        properties.getReadCache().setEnabled(true);
        jdbcTemplate.execute("DELETE FROM kline_1m");
        deleteRedisByPattern("cache:*");
        deleteRedisByPattern("quote:*");
        deleteRedisByPattern("lock:*");
        quoteLaneDispatcher.resetForTest();
        klineAggregator.clearStateForTest();
        marketQueryService.resetDbQueryCount();
    }

    @Test
    void stocksSeedAndSearchEndpointReturnSeededStocks() throws Exception {
        MvcResult allRes = mockMvc.perform(get("/api/stocks"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode all = objectMapper.readTree(allRes.getResponse().getContentAsString());
        assertEquals(100, all.size());

        MvcResult searchRes = mockMvc.perform(get("/api/stocks").param("q", "AAPL"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode filtered = objectMapper.readTree(searchRes.getResponse().getContentAsString());
        assertTrue(filtered.size() >= 1 && filtered.size() < 10);
        assertEquals("AAPL", filtered.get(0).get("symbol").asText());
    }

    @Test
    void klineAggregationPersistsOhlcAndReturnsAscendingBars() throws Exception {
        long t1 = Instant.parse("2025-03-01T09:30:10Z").toEpochMilli();
        long t2 = Instant.parse("2025-03-01T09:30:40Z").toEpochMilli();
        long t3 = Instant.parse("2025-03-01T09:31:10Z").toEpochMilli();

        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("180.00"), t1, 1L)).join();
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("185.50"), t2, 2L)).join();
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("182.25"), t3, 3L)).join();

        int flushed = klineAggregator.flushClosedMinutes(true);
        assertEquals(2, flushed);

        MvcResult res = mockMvc.perform(get("/api/klines/AAPL").param("limit", "10"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode bars = objectMapper.readTree(res.getResponse().getContentAsString());
        assertEquals(2, bars.size());
        assertEquals(0, new BigDecimal("180.00").compareTo(bars.get(0).get("openPrice").decimalValue()));
        assertEquals(0, new BigDecimal("185.50").compareTo(bars.get(0).get("highPrice").decimalValue()));
        assertEquals(0, new BigDecimal("185.50").compareTo(bars.get(0).get("closePrice").decimalValue()));
        assertTrue(bars.get(0).get("minuteTs").asText().compareTo(bars.get(1).get("minuteTs").asText()) < 0);
    }

    @Test
    void cacheAsideDefensesLiveQuoteAndReadSwitch() throws Exception {
        // 1. Penetration defense: unknown symbol caches __NULL__
        marketQueryService.resetDbQueryCount();
        mockMvc.perform(get("/api/quotes/UNKNOWN_XYZ")).andExpect(status().isNotFound());
        assertEquals(MarketQueryService.NULL_SENTINEL,
                stringRedisTemplate.opsForValue().get("cache:quote:UNKNOWN_XYZ"));
        long afterFirstMiss = marketQueryService.getDbQueryCount();

        mockMvc.perform(get("/api/quotes/UNKNOWN_XYZ")).andExpect(status().isNotFound());
        assertEquals(afterFirstMiss, marketQueryService.getDbQueryCount());

        mockMvc.perform(get("/api/klines/UNKNOWN_XYZ")).andExpect(status().isNotFound());
        assertEquals(MarketQueryService.NULL_SENTINEL,
                stringRedisTemplate.opsForValue().get(MarketQueryService.klineCacheKey("UNKNOWN_XYZ", 60)));
        long afterKlineMiss = marketQueryService.getDbQueryCount();
        mockMvc.perform(get("/api/klines/UNKNOWN_XYZ")).andExpect(status().isNotFound());
        assertEquals(afterKlineMiss, marketQueryService.getDbQueryCount());

        // 2. Breakdown (mutex single-flight) + Avalanche (TTL jitter)
        long t1 = Instant.parse("2025-03-01T10:00:10Z").toEpochMilli();
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("190.00"), t1, 1L)).join();
        klineAggregator.flushClosedMinutes(true);
        stringRedisTemplate.delete(MarketQueryService.klineCacheKey("AAPL", 60));
        marketQueryService.resetDbQueryCount();

        int threads = 20;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<List<KlineBar>>> futures = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                Callable<List<KlineBar>> task = () -> {
                    ready.countDown();
                    start.await(5, TimeUnit.SECONDS);
                    return marketQueryService.getKlines("AAPL", 60);
                };
                futures.add(pool.submit(task));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            for (Future<List<KlineBar>> future : futures) {
                List<KlineBar> bars = future.get(5, TimeUnit.SECONDS);
                assertEquals(1, bars.size());
            }
        } finally {
            pool.shutdownNow();
        }
        assertTrue(marketQueryService.getDbQueryCount() <= 2,
                "Expected single-flight MySQL load <= 2 queries, got " + marketQueryService.getDbQueryCount());

        Long ttl = stringRedisTemplate.getExpire(MarketQueryService.klineCacheKey("AAPL", 60), TimeUnit.SECONDS);
        assertTrue(ttl != null && ttl >= 295 && ttl <= 360, "Unexpected cache TTL jitter: " + ttl);

        // 3. Live quote hash takes precedence over stale fallback cache + read-cache.enabled=false toggle
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("201.25"), t1 + 1_000, 2L)).join();
        mockMvc.perform(get("/api/quotes/AAPL"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.price").value(201.25));

        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("205.50"), t1 + 2_000, 3L)).join();
        mockMvc.perform(get("/api/quotes/AAPL"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.price").value(205.50));

        try {
            properties.getReadCache().setEnabled(false);
            long before = marketQueryService.getDbQueryCount();
            mockMvc.perform(get("/api/klines/AAPL")).andExpect(status().isOk());
            assertTrue(marketQueryService.getDbQueryCount() > before);
        } finally {
            properties.getReadCache().setEnabled(true);
        }
    }

    @Test
    void simulatorEndpointsControlEmissionAndSystemStatus() throws Exception {
        mockMvc.perform(post("/api/simulator/emit"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.emitted").value(100));

        mockMvc.perform(post("/api/simulator/crash-storm").param("dropRatio", "0.05"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.emitted").value(100))
                .andExpect(jsonPath("$.dropRatio").value(0.05));

        mockMvc.perform(post("/api/simulator/start").param("intervalMs", "150"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.running").value(true))
                .andExpect(jsonPath("$.intervalMs").value(150));

        mockMvc.perform(put("/api/simulator/rate").param("intervalMs", "80"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.running").value(true))
                .andExpect(jsonPath("$.intervalMs").value(80));

        mockMvc.perform(get("/api/system/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lanes").value(16))
                .andExpect(jsonPath("$.simulatorRunning").value(true))
                .andExpect(jsonPath("$.simulatorIntervalMs").value(80));

        mockMvc.perform(post("/api/simulator/stop"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.running").value(false));
        assertFalse(marketSimulatorRunner.isRunning());
    }

    private void deleteRedisByPattern(String pattern) {
        Set<String> keys = stringRedisTemplate.keys(pattern);
        if (keys != null && !keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
        }
    }
}
