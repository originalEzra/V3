package com.pricepulse.rule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricepulse.PricePulseProperties;
import com.pricepulse.market.feed.MarketSimulatorRunner;
import com.pricepulse.market.model.QuoteTick;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import com.pricepulse.market.service.StockCatalog;
import com.pricepulse.rule.model.AlertRule;
import com.pricepulse.rule.model.Direction;
import com.pricepulse.rule.service.RuleIndexMaintainer;
import com.pricepulse.rule.service.RuleMatchEngine;
import com.pricepulse.rule.service.RuleService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class RuleMatchingAndCircuitBreakerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private PricePulseProperties properties;

    @Autowired
    private StockCatalog stockCatalog;

    @Autowired
    private QuoteLaneDispatcher quoteLaneDispatcher;

    @Autowired
    private RuleService ruleService;

    @Autowired
    private RuleIndexMaintainer ruleIndexMaintainer;

    @Autowired
    private RuleMatchEngine ruleMatchEngine;

    @Autowired
    private MarketSimulatorRunner marketSimulatorRunner;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    @AfterEach
    void resetState() {
        marketSimulatorRunner.stop();
        properties.getMatching().setForceDbFallback(false);
        jdbcTemplate.execute("DELETE FROM alert_rules");
        deleteRedisByPattern("rules:*");
        deleteRedisByPattern("quote:*");
        stringRedisTemplate.delete(RuleIndexMaintainer.INDEX_READY_KEY);
        quoteLaneDispatcher.resetForTest();
        ruleMatchEngine.resetForTest();
        ruleMatchEngine.attemptRecoveryIfNeeded();
    }

    @Test
    void ruleCrudPriceGuardsAndOptimisticLocking() throws Exception {
        // 503 when no trusted latest quote exists yet
        mockMvc.perform(post("/api/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":1,\"symbol\":\"AAPL\",\"direction\":\"ABOVE\",\"thresholdPrice\":160.00}"))
                .andExpect(status().isServiceUnavailable());

        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("150.00"), 1_000L, 1L)).join();

        // 400 when current price already satisfies threshold
        mockMvc.perform(post("/api/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":1,\"symbol\":\"AAPL\",\"direction\":\"ABOVE\",\"thresholdPrice\":150.00}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/api/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":1,\"symbol\":\"AAPL\",\"direction\":\"BELOW\",\"thresholdPrice\":150.00}"))
                .andExpect(status().isBadRequest());

        MvcResult createdRes = mockMvc.perform(post("/api/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":1,\"symbol\":\"AAPL\",\"direction\":\"ABOVE\",\"thresholdPrice\":160.00}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.version").value(0))
                .andReturn();
        long ruleId = objectMapper.readTree(createdRes.getResponse().getContentAsString()).get("id").asLong();

        mockMvc.perform(post("/api/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":1,\"symbol\":\"AAPL\",\"direction\":\"BELOW\",\"thresholdPrice\":140.00}"))
                .andExpect(status().isCreated());

        MvcResult listRes = mockMvc.perform(get("/api/rules")
                        .param("userId", "1")
                        .param("symbol", "AAPL"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode listed = objectMapper.readTree(listRes.getResponse().getContentAsString());
        assertEquals(2, listed.size());

        mockMvc.perform(put("/api/rules/{id}", ruleId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"direction\":\"ABOVE\",\"thresholdPrice\":165.00,\"version\":0}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.thresholdPrice").value(165.00));

        // Stale version rejected with 409
        mockMvc.perform(put("/api/rules/{id}", ruleId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"direction\":\"ABOVE\",\"thresholdPrice\":170.00,\"version\":0}"))
                .andExpect(status().isConflict());

        mockMvc.perform(delete("/api/rules/{id}", ruleId).param("version", "0"))
                .andExpect(status().isConflict());

        mockMvc.perform(delete("/api/rules/{id}", ruleId).param("version", "1"))
                .andExpect(status().isNoContent());

        mockMvc.perform(get("/api/rules/{id}", ruleId))
                .andExpect(status().isNotFound());
    }

    @Test
    void zsetIndexSyncLuaBoundaryMatchAndReconcileRepair() throws Exception {
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("150.00"), 1_000L, 1L)).join();
        Long stockId = stockCatalog.getBySymbol("AAPL").getId();

        AlertRule aboveRule = ruleService.createRule(1L, "AAPL", Direction.ABOVE, new BigDecimal("160.00"));
        AlertRule belowRule = ruleService.createRule(1L, "AAPL", Direction.BELOW, new BigDecimal("140.00"));
        drainMaintainer();

        assertEquals(16000.0, stringRedisTemplate.opsForZSet()
                .score(RuleIndexMaintainer.aboveKey(stockId), String.valueOf(aboveRule.getId())));
        assertEquals(14000.0, stringRedisTemplate.opsForZSet()
                .score(RuleIndexMaintainer.belowKey(stockId), String.valueOf(belowRule.getId())));

        assertEquals("REDIS", ruleMatchEngine.getCurrentMode());
        assertTrue(ruleMatchEngine.match(stockId, new BigDecimal("159.99"), 15999L).isEmpty());
        assertEquals(List.of(aboveRule.getId()),
                ruleMatchEngine.match(stockId, new BigDecimal("160.00"), 16000L));
        assertEquals(List.of(belowRule.getId()),
                ruleMatchEngine.match(stockId, new BigDecimal("140.00"), 14000L));
        assertTrue(ruleMatchEngine.getRedisMatchCount() >= 3);

        // Simulate missing ZSet repaired by scheduled reconcile
        stringRedisTemplate.delete(RuleIndexMaintainer.aboveKey(stockId));
        ruleIndexMaintainer.triggerReconcileAsync().get();
        assertEquals(List.of(aboveRule.getId()),
                ruleMatchEngine.match(stockId, new BigDecimal("160.00"), 16000L));
    }

    @Test
    void sentinelNotReadyCircuitBreakerFallbackRecoveryAndForceSwitch() throws Exception {
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("150.00"), 1_000L, 1L)).join();
        Long stockId = stockCatalog.getBySymbol("AAPL").getId();
        AlertRule rule = ruleService.createRule(1L, "AAPL", Direction.ABOVE, new BigDecimal("160.00"));
        drainMaintainer();

        // 1. Empty Redis restart (sentinel + ZSet missing while PING succeeds) -> NOT_READY triggers immediate SQL fallback
        stringRedisTemplate.delete(RuleIndexMaintainer.INDEX_READY_KEY);
        stringRedisTemplate.delete(RuleIndexMaintainer.aboveKey(stockId));

        long fallbackBefore = ruleMatchEngine.getFallbackMatchCount();
        List<Long> matchedOnNotReady = ruleMatchEngine.match(stockId, new BigDecimal("165.00"), 16500L);
        assertEquals(List.of(rule.getId()), matchedOnNotReady);
        assertFalse(ruleMatchEngine.isRedisIndexReady());
        assertEquals(fallbackBefore + 1, ruleMatchEngine.getFallbackMatchCount());

        assertTrue(ruleMatchEngine.attemptRecoveryIfNeeded());
        assertTrue(ruleMatchEngine.isRedisIndexReady());
        assertEquals("REDIS", ruleMatchEngine.getCurrentMode());

        long redisBefore = ruleMatchEngine.getRedisMatchCount();
        assertEquals(List.of(rule.getId()), ruleMatchEngine.match(stockId, new BigDecimal("165.00"), 16500L));
        assertEquals(redisBefore + 1, ruleMatchEngine.getRedisMatchCount());

        // 2. CircuitBreaker OPEN -> MySQL fallback -> Recovery Gate restores CLOSED + REDIS
        for (int i = 0; i < 5; i++) {
            ruleMatchEngine.recordExternalRedisError(new RuntimeException("simulated redis error"));
        }
        assertEquals(CircuitBreaker.State.OPEN, ruleMatchEngine.getCircuitBreakerState());
        assertEquals("DB_FALLBACK", ruleMatchEngine.getCurrentMode());
        assertEquals(List.of(rule.getId()), ruleMatchEngine.match(stockId, new BigDecimal("165.00"), 16500L));

        assertTrue(ruleMatchEngine.attemptRecoveryIfNeeded());
        assertEquals(CircuitBreaker.State.CLOSED, ruleMatchEngine.getCircuitBreakerState());
        assertEquals("REDIS", ruleMatchEngine.getCurrentMode());

        // 3. force-db-fallback comparison toggle
        try {
            properties.getMatching().setForceDbFallback(true);
            assertEquals("DB_FALLBACK", ruleMatchEngine.getCurrentMode());
            long fbCount = ruleMatchEngine.getFallbackMatchCount();
            assertEquals(List.of(rule.getId()), ruleMatchEngine.match(stockId, new BigDecimal("165.00"), 16500L));
            assertEquals(fbCount + 1, ruleMatchEngine.getFallbackMatchCount());
        } finally {
            properties.getMatching().setForceDbFallback(false);
        }
    }

    @Test
    void explainAnalyzeFallbackQueryHitsIdxMatchOnBothBranches() {
        List<Stock> stocks = stockCatalog.getAllActive();
        Long aaplId = stockCatalog.getBySymbol("AAPL").getId();

        List<Object[]> batch = new ArrayList<>(250);
        for (int i = 0; i < 50; i++) {
            String dir = (i % 2 == 0) ? "ABOVE" : "BELOW";
            BigDecimal price = new BigDecimal(120 + i).setScale(2);
            batch.add(new Object[]{1L, aaplId, dir, price, "ACTIVE", 0L});
        }
        for (int i = 0; i < 200; i++) {
            Long sid = stocks.get((i % 20) + 1).getId();
            String dir = (i % 2 == 0) ? "ABOVE" : "BELOW";
            BigDecimal price = new BigDecimal(100 + (i % 80)).setScale(2);
            batch.add(new Object[]{1L, sid, dir, price, "ACTIVE", 0L});
        }
        jdbcTemplate.batchUpdate(
                "INSERT INTO alert_rules (user_id, stock_id, direction, threshold_price, status, version) VALUES (?, ?, ?, ?, ?, ?)",
                batch
        );
        jdbcTemplate.execute("ANALYZE TABLE alert_rules");

        String sql = """
                EXPLAIN ANALYZE
                SELECT id FROM alert_rules
                WHERE stock_id = ? AND status = 'ACTIVE' AND direction = 'ABOVE' AND threshold_price <= ?
                UNION ALL
                SELECT id FROM alert_rules
                WHERE stock_id = ? AND status = 'ACTIVE' AND direction = 'BELOW' AND threshold_price >= ?
                """;
        List<String> lines = jdbcTemplate.queryForList(
                sql,
                String.class,
                aaplId, new BigDecimal("150.00"),
                aaplId, new BigDecimal("150.00")
        );
        String plan = String.join("\n", lines);
        System.out.println("=== EXPLAIN ANALYZE (idx_match) ===\n" + plan);

        int idxMatchOccurrences = countOccurrences(plan, "idx_match");
        assertTrue(idxMatchOccurrences >= 2,
                "Expected idx_match in both UNION ALL branches, actual plan:\n" + plan);
        assertFalse(plan.contains("Table scan on alert_rules"),
                "Unexpected full table scan in plan:\n" + plan);
    }

    private void drainMaintainer() throws Exception {
        ruleIndexMaintainer.getMaintainerExecutor().submit(() -> null).get();
    }

    private void deleteRedisByPattern(String pattern) {
        Set<String> keys = stringRedisTemplate.keys(pattern);
        if (keys != null && !keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
        }
    }

    private static int countOccurrences(String text, String token) {
        int count = 0;
        int idx = 0;
        while ((idx = text.indexOf(token, idx)) != -1) {
            count++;
            idx += token.length();
        }
        return count;
    }
}
