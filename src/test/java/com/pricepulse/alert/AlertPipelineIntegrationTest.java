package com.pricepulse.alert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricepulse.PricePulseProperties;
import com.pricepulse.alert.mapper.AlertTriggerMapper;
import com.pricepulse.alert.mapper.NotificationMapper;
import com.pricepulse.alert.mapper.OutboxEventMapper;
import com.pricepulse.alert.model.OutboxEvent;
import com.pricepulse.alert.model.OutboxStatus;
import com.pricepulse.alert.service.AlertMessageSender;
import com.pricepulse.alert.service.AlertNotificationConsumer;
import com.pricepulse.alert.service.AlertTriggerService;
import com.pricepulse.alert.service.OutboxPublisher;
import com.pricepulse.common.PriceCents;
import com.pricepulse.market.feed.MarketSimulatorRunner;
import com.pricepulse.market.model.QuoteSnapshot;
import com.pricepulse.market.model.QuoteTick;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import com.pricepulse.market.service.StockCatalog;
import com.pricepulse.rule.mapper.AlertRuleMapper;
import com.pricepulse.rule.model.AlertRule;
import com.pricepulse.rule.model.Direction;
import com.pricepulse.rule.model.RuleStatus;
import com.pricepulse.rule.service.RuleIndexMaintainer;
import com.pricepulse.rule.service.RuleMatchEngine;
import com.pricepulse.rule.service.RuleService;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AlertPipelineIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

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
    private AlertRuleMapper alertRuleMapper;

    @Autowired
    private AlertTriggerMapper alertTriggerMapper;

    @Autowired
    private OutboxEventMapper outboxEventMapper;

    @Autowired
    private NotificationMapper notificationMapper;

    @Autowired
    private AlertTriggerService alertTriggerService;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private AlertMessageSender alertMessageSender;

    @Autowired
    private AlertNotificationConsumer alertNotificationConsumer;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    @AfterEach
    void resetState() {
        marketSimulatorRunner.stop();
        outboxPublisher.pausePolling();
        properties.getMatching().setForceDbFallback(false);

        jdbcTemplate.execute("DELETE FROM notifications");
        jdbcTemplate.execute("DELETE FROM outbox_events");
        jdbcTemplate.execute("DELETE FROM alert_triggers");
        jdbcTemplate.execute("DELETE FROM alert_rules");

        deleteRedisByPattern("rules:*");
        deleteRedisByPattern("quote:*");
        stringRedisTemplate.delete(RuleIndexMaintainer.INDEX_READY_KEY);

        quoteLaneDispatcher.resetForTest();
        ruleMatchEngine.resetForTest();
        ruleMatchEngine.attemptRecoveryIfNeeded();
        alertTriggerService.resetMetricsForTest();
        outboxPublisher.resetMetricsForTest();
        alertNotificationConsumer.resetMetricsForTest();
        outboxPublisher.resumePolling();
    }

    @Test
    void endToEndTriggerOutboxRocketMqWebSocketAndRestApi() throws Exception {
        BlockingQueue<String> user1Messages = new LinkedBlockingQueue<>();
        BlockingQueue<String> user2Messages = new LinkedBlockingQueue<>();
        StandardWebSocketClient wsClient = new StandardWebSocketClient();

        WebSocketSession user1Session = wsClient.execute(
                new RecordingTextHandler(user1Messages),
                "ws://127.0.0.1:" + port + "/ws/notifications?userId=1"
        ).get(5, TimeUnit.SECONDS);
        WebSocketSession user2Session = wsClient.execute(
                new RecordingTextHandler(user2Messages),
                "ws://127.0.0.1:" + port + "/ws/notifications?userId=2"
        ).get(5, TimeUnit.SECONDS);

        try {
            quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("150.00"), 10_000L, 1L)).join();
            AlertRule rule = ruleService.createRule(1L, "AAPL", Direction.ABOVE, new BigDecimal("160.00"));
            drainMaintainer();

            Long aaplId = stockCatalog.getBySymbol("AAPL").getId();
            assertEquals(16000.0, stringRedisTemplate.opsForZSet()
                    .score(RuleIndexMaintainer.aboveKey(aaplId), String.valueOf(rule.getId())));

            // Price crosses threshold -> optimistic-lock trigger -> Outbox -> RocketMQ -> Notification + WebSocket
            quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("165.00"), 11_000L, 2L)).join();
            drainMaintainer();

            awaitUntil(15_000, () ->
                    outboxEventMapper.countByStatus(OutboxStatus.SENT) == 1
                            && notificationMapper.countAll() == 1);

            String wsPayload = user1Messages.poll(10, TimeUnit.SECONDS);
            assertNotNull(wsPayload, "Expected WebSocket notification for userId=1");
            assertNull(user2Messages.poll(300, TimeUnit.MILLISECONDS),
                    "Did not expect WebSocket notification for unrelated userId=2");

            JsonNode wsJson = objectMapper.readTree(wsPayload);
            assertEquals(1L, wsJson.get("userId").asLong());
            assertEquals("AAPL", wsJson.get("symbol").asText());
            assertFalse(wsJson.get("isRead").asBoolean());

            AlertRule completed = alertRuleMapper.findById(rule.getId());
            assertEquals(RuleStatus.COMPLETED, completed.getStatus());
            assertEquals(1L, completed.getVersion());
            assertNull(stringRedisTemplate.opsForZSet()
                    .score(RuleIndexMaintainer.aboveKey(aaplId), String.valueOf(rule.getId())));
            assertEquals(1L, alertTriggerMapper.countAll());
            assertEquals(0L, outboxEventMapper.countByStatus(OutboxStatus.PENDING));

            // Notification REST API + /api/system/status verification
            ResponseEntity<JsonNode> listResp = restTemplate.getForEntity(
                    "/api/notifications?userId=1&limit=10", JsonNode.class);
            assertEquals(HttpStatus.OK, listResp.getStatusCode());
            assertEquals(1, listResp.getBody().size());
            assertFalse(listResp.getBody().get(0).get("isRead").asBoolean());

            ResponseEntity<JsonNode> unreadBefore = restTemplate.getForEntity(
                    "/api/notifications/unread-count?userId=1", JsonNode.class);
            assertEquals(1L, unreadBefore.getBody().get("unreadCount").asLong());

            restTemplate.postForEntity("/api/notifications/read-all?userId=1", null, JsonNode.class);
            ResponseEntity<JsonNode> unreadAfter = restTemplate.getForEntity(
                    "/api/notifications/unread-count?userId=1", JsonNode.class);
            assertEquals(0L, unreadAfter.getBody().get("unreadCount").asLong());

            ResponseEntity<JsonNode> sysStatus = restTemplate.getForEntity("/api/system/status", JsonNode.class);
            assertEquals(HttpStatus.OK, sysStatus.getStatusCode());
            assertEquals(0L, sysStatus.getBody().get("outboxPendingCount").asLong());
            assertTrue(sysStatus.getBody().get("triggeredRuleCount").asLong() >= 1L);
            assertTrue(sysStatus.getBody().get("consumedNotificationCount").asLong() >= 1L);
        } finally {
            user1Session.close();
            user2Session.close();
        }
    }

    @Test
    void optimisticLockConflictPreservesUpdatedRuleInRedisAndRejectsPostCompleteEdit() throws Exception {
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("150.00"), 10_000L, 1L)).join();
        AlertRule staleSnapshot = ruleService.createRule(1L, "AAPL", Direction.ABOVE, new BigDecimal("160.00"));
        drainMaintainer();

        // User concurrently raises threshold to 200.00 (version 0 -> 1)
        AlertRule updatedRule = ruleService.updateRule(
                staleSnapshot.getId(), Direction.ABOVE, new BigDecimal("200.00"), 0L);
        drainMaintainer();
        assertEquals(1L, updatedRule.getVersion());

        Long aaplId = stockCatalog.getBySymbol("AAPL").getId();
        QuoteSnapshot tickAt165 = new QuoteSnapshot(
                aaplId, "AAPL", new BigDecimal("165.00"), PriceCents.toCents(new BigDecimal("165.00")), 11_000L, 2L);

        // Stale trigger attempt with version=0 must affect 0 rows and MUST NOT delete the updated rule from Redis
        boolean triggered = alertTriggerService.tryTriggerRule(staleSnapshot, tickAt165);
        assertFalse(triggered);
        assertEquals(0L, alertTriggerMapper.countAll());
        assertEquals(0L, outboxEventMapper.countByStatus(OutboxStatus.PENDING));
        assertEquals(1L, alertTriggerService.getOptimisticConflictCount());

        AlertRule currentInDb = alertRuleMapper.findById(staleSnapshot.getId());
        assertEquals(RuleStatus.ACTIVE, currentInDb.getStatus());
        assertEquals(1L, currentInDb.getVersion());
        assertEquals(0, new BigDecimal("200.00").compareTo(currentInDb.getThresholdPrice()));
        assertEquals(20000.0, stringRedisTemplate.opsForZSet()
                .score(RuleIndexMaintainer.aboveKey(aaplId), String.valueOf(staleSnapshot.getId())));

        // Later tick reaching 205.00 triggers the updated rule cleanly
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("205.00"), 12_000L, 3L)).join();
        drainMaintainer();

        awaitUntil(10_000, () -> notificationMapper.countAll() == 1);
        AlertRule completed = alertRuleMapper.findById(staleSnapshot.getId());
        assertEquals(RuleStatus.COMPLETED, completed.getStatus());
        assertEquals(2L, completed.getVersion());

        // Completed rules cannot be resurrected or edited/deleted
        ResponseStatusException updateEx = assertThrows(ResponseStatusException.class, () ->
                ruleService.updateRule(completed.getId(), Direction.ABOVE, new BigDecimal("250.00"), 2L));
        assertEquals(HttpStatus.CONFLICT, updateEx.getStatusCode());

        ResponseStatusException deleteEx = assertThrows(ResponseStatusException.class, () ->
                ruleService.deleteRule(completed.getId(), 2L));
        assertEquals(HttpStatus.CONFLICT, deleteEx.getStatusCode());
    }

    @Test
    void outboxSkipLockedLeaseGuardAndContinuousCappedBackoff() throws Exception {
        outboxPublisher.pausePolling();

        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("150.00"), 10_000L, 1L)).join();
        for (int i = 1; i <= 6; i++) {
            ruleService.createRule(1L, "AAPL", Direction.ABOVE, new BigDecimal(155 + i + ".00"));
        }
        drainMaintainer();

        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("170.00"), 11_000L, 2L)).join();
        drainMaintainer();
        assertEquals(6L, outboxEventMapper.countByStatus(OutboxStatus.PENDING));

        // 1. Concurrent FOR UPDATE SKIP LOCKED returns disjoint batches without blocking
        CountDownLatch worker1Locked = new CountDownLatch(1);
        CountDownLatch allowWorker1Commit = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Long> allIds;
        try {
            Future<List<Long>> f1 = pool.submit(() -> transactionTemplate.execute(status -> {
                List<OutboxEvent> rows = outboxEventMapper.selectPendingForUpdateSkipLocked(3);
                worker1Locked.countDown();
                try {
                    assertTrue(allowWorker1Commit.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return rows.stream().map(OutboxEvent::getId).toList();
            }));

            assertTrue(worker1Locked.await(5, TimeUnit.SECONDS));
            Future<List<Long>> f2 = pool.submit(() -> transactionTemplate.execute(status ->
                    outboxEventMapper.selectPendingForUpdateSkipLocked(3)
                            .stream().map(OutboxEvent::getId).toList()));

            List<Long> batch2Ids = f2.get(5, TimeUnit.SECONDS);
            allowWorker1Commit.countDown();
            List<Long> batch1Ids = f1.get(5, TimeUnit.SECONDS);

            assertEquals(3, batch1Ids.size());
            assertEquals(3, batch2Ids.size());
            Set<Long> union = new HashSet<>(batch1Ids);
            union.addAll(batch2Ids);
            assertEquals(6, union.size());
            allIds = union.stream().sorted().toList();
        } finally {
            pool.shutdownNow();
        }

        // 2. Stale leaseId cannot overwrite newer lease holder on markRetry or markSent
        long id1 = allIds.get(0);
        List<OutboxEvent> claimedA = outboxPublisher.claimBatch("lease-a", 1);
        assertEquals(1, claimedA.size());
        assertEquals(id1, claimedA.get(0).getId());

        jdbcTemplate.update("UPDATE outbox_events SET locked_until = DATE_SUB(NOW(), INTERVAL 10 SECOND) WHERE id = ?", id1);
        List<OutboxEvent> claimedB = outboxPublisher.claimBatch("lease-b", 1);
        assertEquals(1, claimedB.size());
        assertEquals(id1, claimedB.get(0).getId());

        assertEquals(0, outboxEventMapper.markRetry(id1, "lease-a", 1L));
        assertEquals(0, outboxEventMapper.markSent(id1, "lease-a"));
        assertEquals(1, outboxEventMapper.markSent(id1, "lease-b"));
        assertEquals(0, outboxEventMapper.markRetry(id1, "lease-a", 1L));
        assertEquals(OutboxStatus.SENT, outboxEventMapper.findById(id1).getStatus());

        // 3. Continuous capped backoff past 8 attempts still retries and eventually publishes
        long id2 = allIds.get(1);
        long[] expectedBackoffs = {1L, 2L, 4L, 8L, 16L, 30L, 30L, 30L, 30L, 30L};
        for (int attempt = 0; attempt < expectedBackoffs.length; attempt++) {
            String lease = "retry-lease-" + attempt;
            List<OutboxEvent> claimed = outboxPublisher.claimBatch(lease, 1);
            assertEquals(1, claimed.size());
            assertEquals(id2, claimed.get(0).getId());
            long backoff = outboxPublisher.calculateBackoffSeconds(claimed.get(0).getAttempts());
            assertEquals(expectedBackoffs[attempt], backoff);
            assertEquals(1, outboxEventMapper.markRetry(id2, lease, backoff));
            jdbcTemplate.update("UPDATE outbox_events SET next_retry_at = NOW() WHERE id = ?", id2);
        }

        OutboxEvent after10Failures = outboxEventMapper.findById(id2);
        assertEquals(10, after10Failures.getAttempts());
        assertEquals(OutboxStatus.PENDING, after10Failures.getStatus());

        int published = outboxPublisher.publishBatchOnce();
        assertEquals(5, published);
        assertEquals(OutboxStatus.SENT, outboxEventMapper.findById(id2).getStatus());
        awaitUntil(15_000, () -> notificationMapper.findByTriggerId(after10Failures.getAggregateId()) != null);
    }

    @Test
    void consumerConcurrencyConfigIdempotencyAndCrashStorm50Rules() throws Exception {
        assertNotNull(alertNotificationConsumer.getConsumer());
        assertEquals(4, alertNotificationConsumer.getConsumer().getConsumeThreadMin());
        assertEquals(8, alertNotificationConsumer.getConsumer().getConsumeThreadMax());

        // 1. Duplicate RocketMQ message delivery is idempotently deduplicated by trigger_id unique key
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("150.00"), 10_000L, 1L)).join();
        AlertRule dupTestRule = ruleService.createRule(1L, "AAPL", Direction.ABOVE, new BigDecimal("160.00"));
        drainMaintainer();
        quoteLaneDispatcher.dispatch(new QuoteTick("AAPL", new BigDecimal("165.00"), 11_000L, 2L)).join();
        drainMaintainer();

        awaitUntil(15_000, () ->
                outboxEventMapper.countByStatus(OutboxStatus.SENT) == 1
                        && notificationMapper.countAll() == 1);

        long triggerId = alertTriggerMapper.findByRuleId(dupTestRule.getId()).getId();
        OutboxEvent sentOutbox = outboxEventMapper.findByAggregateId(triggerId);
        alertMessageSender.send(sentOutbox);

        awaitUntil(10_000, () -> alertNotificationConsumer.getDuplicateCount() >= 1);
        assertEquals(1L, notificationMapper.countAll());
        assertNotNull(notificationMapper.findByTriggerId(triggerId));

        // Reset before 50-rule crash storm check
        resetState();

        // 2. Crash storm across 10 stocks x 5 BELOW rules = 50 rules
        List<Stock> targetStocks = stockCatalog.getAllActive().subList(0, 10);
        List<QuoteTick> initialTicks = new ArrayList<>(10);
        for (Stock stock : targetStocks) {
            initialTicks.add(new QuoteTick(stock.getSymbol(), new BigDecimal("100.00"), 100_000L, 1L));
        }
        quoteLaneDispatcher.dispatchAll(initialTicks);

        for (Stock stock : targetStocks) {
            for (int r = 1; r <= 5; r++) {
                ruleService.createRule(
                        (long) r,
                        stock.getSymbol(),
                        Direction.BELOW,
                        new BigDecimal(96 + (r * 0.25) + "").setScale(2)
                );
            }
        }
        drainMaintainer();

        List<QuoteTick> crashTicks = new ArrayList<>(10);
        for (Stock stock : targetStocks) {
            crashTicks.add(new QuoteTick(stock.getSymbol(), new BigDecimal("95.00"), 101_000L, 2L));
        }
        quoteLaneDispatcher.dispatchAll(crashTicks);

        awaitUntil(20_000, () ->
                alertTriggerMapper.countAll() == 50
                        && outboxEventMapper.countByStatus(OutboxStatus.SENT) == 50
                        && outboxEventMapper.countByStatus(OutboxStatus.PENDING) == 0
                        && notificationMapper.countAll() == 50);

        assertEquals(50L, alertTriggerMapper.countAll());
        assertEquals(50L, outboxEventMapper.countByStatus(OutboxStatus.SENT));
        assertEquals(0L, outboxEventMapper.countByStatus(OutboxStatus.PENDING));
        assertEquals(50L, notificationMapper.countAll());
    }

    private void drainMaintainer() throws Exception {
        ruleIndexMaintainer.getMaintainerExecutor().submit(() -> null).get(10, TimeUnit.SECONDS);
    }

    private void deleteRedisByPattern(String pattern) {
        Set<String> keys = stringRedisTemplate.keys(pattern);
        if (keys != null && !keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
        }
    }

    private void awaitUntil(long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        assertTrue(condition.getAsBoolean(), "Timed out after " + timeoutMs + "ms waiting for condition");
    }

    private static final class RecordingTextHandler extends TextWebSocketHandler {
        private final BlockingQueue<String> messages;

        private RecordingTextHandler(BlockingQueue<String> messages) {
            this.messages = messages;
        }

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) {
            messages.offer(message.getPayload());
        }
    }
}
