package com.pricepulse.benchmark;

import com.pricepulse.PricePulseProperties;
import com.pricepulse.alert.mapper.AlertTriggerMapper;
import com.pricepulse.alert.mapper.NotificationMapper;
import com.pricepulse.alert.mapper.OutboxEventMapper;
import com.pricepulse.alert.model.OutboxStatus;
import com.pricepulse.alert.service.AlertNotificationConsumer;
import com.pricepulse.alert.service.AlertTriggerService;
import com.pricepulse.alert.service.OutboxPublisher;
import com.pricepulse.market.feed.MarketSimulatorRunner;
import com.pricepulse.market.model.QuoteTick;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.pipeline.KlineAggregator;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import com.pricepulse.market.service.MarketQueryService;
import com.pricepulse.market.service.StockCatalog;
import com.pricepulse.rule.service.RuleIndexMaintainer;
import com.pricepulse.rule.service.RuleMatchEngine;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class Phase6Benchmark {

    private static final int STOCK_COUNT = 100;
    private static final int RULES_PER_STOCK = 50;
    private static final int REPLAY_WAVES = 200;
    private static final int TOTAL_REPLAY_TICKS = STOCK_COUNT * REPLAY_WAVES;
    private static final int EXPECTED_TRIGGERS = 500;
    private static final int DISPATCH_CONCURRENCY = 128;

    private static final int READ_THREADS = 100;
    private static final int READ_REQUESTS_PER_THREAD = 200;
    private static final int TOTAL_READ_REQUESTS = READ_THREADS * READ_REQUESTS_PER_THREAD;
    private static final List<String> HOT_SYMBOLS = List.of("AAPL", "TSLA", "NVDA", "MSFT", "AMZN");

    @LocalServerPort
    private int port;

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
    private KlineAggregator klineAggregator;

    @Autowired
    private MarketQueryService marketQueryService;

    @Autowired
    private RuleIndexMaintainer ruleIndexMaintainer;

    @Autowired
    private RuleMatchEngine ruleMatchEngine;

    @Autowired
    private MarketSimulatorRunner marketSimulatorRunner;

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
    private AlertNotificationConsumer alertNotificationConsumer;

    @BeforeEach
    @AfterEach
    void resetAllState() throws Exception {
        marketSimulatorRunner.stop();
        outboxPublisher.pausePolling();
        properties.getMatching().setForceDbFallback(false);
        properties.getReadCache().setEnabled(true);

        drainMaintainer();
        quoteLaneDispatcher.resetForTest();
        klineAggregator.clearStateForTest();

        jdbcTemplate.execute("DELETE FROM notifications");
        jdbcTemplate.execute("DELETE FROM outbox_events");
        jdbcTemplate.execute("DELETE FROM alert_triggers");
        jdbcTemplate.execute("DELETE FROM alert_rules");
        jdbcTemplate.execute("DELETE FROM kline_1m");

        deleteRedisByPattern("rules:*");
        deleteRedisByPattern("quote:*");
        deleteRedisByPattern("cache:*");
        deleteRedisByPattern("lock:*");
        stringRedisTemplate.delete(RuleIndexMaintainer.INDEX_READY_KEY);

        marketQueryService.resetDbQueryCount();
        ruleMatchEngine.resetForTest();
        alertTriggerService.resetMetricsForTest();
        outboxPublisher.resetMetricsForTest();
        alertNotificationConsumer.resetMetricsForTest();

        ruleMatchEngine.attemptRecoveryIfNeeded();
        outboxPublisher.resumePolling();
    }

    @Test
    void benchmarkHighConcurrencyMatchingABAndExplainAnalyze() throws Exception {
        List<Stock> stocks = sortedActiveStocks();
        assertEquals(STOCK_COUNT, stocks.size());

        // 1. Seed 5,000 rules and verify EXPLAIN ANALYZE on the exact UNION ALL fallback query
        seed5000Rules(stocks);
        jdbcTemplate.execute("ANALYZE TABLE alert_rules");

        Long aaplId = stockCatalog.getBySymbol("AAPL").getId();
        String explainSql = """
                EXPLAIN ANALYZE
                SELECT id FROM alert_rules
                WHERE stock_id = ? AND status = 'ACTIVE' AND direction = 'ABOVE' AND threshold_price <= ?
                UNION ALL
                SELECT id FROM alert_rules
                WHERE stock_id = ? AND status = 'ACTIVE' AND direction = 'BELOW' AND threshold_price >= ?
                """;
        List<String> explainLines = jdbcTemplate.queryForList(
                explainSql,
                String.class,
                aaplId, new BigDecimal("100.00"),
                aaplId, new BigDecimal("100.00")
        );
        String explainPlan = String.join("\n", explainLines);
        System.out.println("=== PHASE6 EXPLAIN ANALYZE (5000 RULES) ===");
        System.out.println(explainPlan);

        assertTrue(countOccurrences(explainPlan, "idx_match") >= 2,
                "Expected idx_match in both UNION ALL branches, actual plan:\n" + explainPlan);
        assertFalse(explainPlan.contains("Table scan on alert_rules"),
                "Unexpected full table scan on alert_rules:\n" + explainPlan);

        // 2. Build deterministic warmup (3,000 ticks) and benchmark replay (20,000 ticks)
        long baseTs = Instant.parse("2026-11-21T10:00:00Z").toEpochMilli();
        List<QuoteTick> warmupTicks = buildWarmupTicks(stocks, 30, baseTs);
        List<QuoteTick> replayTicks = buildDeterministicReplayTicks(stocks, baseTs);
        assertEquals(TOTAL_REPLAY_TICKS, replayTicks.size());

        // Warmup DB fallback and Redis paths
        runWarmupMatching(stocks, warmupTicks, true);
        runWarmupMatching(stocks, warmupTicks, false);

        // 3. Run A: force-db-fallback = true
        MatchingRunResult dbRun = runMatchingBenchmark(stocks, replayTicks, true);

        // 4. Run B: force-db-fallback = false (Redis ZSet + Lua pre-filter)
        MatchingRunResult redisRun = runMatchingBenchmark(stocks, replayTicks, false);

        assertEquals(EXPECTED_TRIGGERS, dbRun.triggeredCount());
        assertEquals(EXPECTED_TRIGGERS, redisRun.triggeredCount());
        assertTrue(redisRun.tps() > dbRun.tps(),
                "Expected Redis TPS (" + redisRun.tps() + ") > DB fallback TPS (" + dbRun.tps() + ")");

        double speedup = redisRun.tps() / dbRun.tps();
        System.out.println("=== PHASE6 MATCHING A/B SUMMARY (5,000 RULES, 20,000 TICKS) ===");
        System.out.printf("DB_FALLBACK (force-db-fallback=true) : elapsed=%.3fs, TPS=%.2f, P50=%.3fms, P95=%.3fms, P99=%.3fms, triggers=%d%n",
                dbRun.elapsedSeconds(), dbRun.tps(), dbRun.p50Ms(), dbRun.p95Ms(), dbRun.p99Ms(), dbRun.triggeredCount());
        System.out.printf("REDIS_ZSET  (force-db-fallback=false): elapsed=%.3fs, TPS=%.2f, P50=%.3fms, P95=%.3fms, P99=%.3fms, triggers=%d%n",
                redisRun.elapsedSeconds(), redisRun.tps(), redisRun.p50Ms(), redisRun.p95Ms(), redisRun.p99Ms(), redisRun.triggeredCount());
        System.out.printf("MATCHING_SPEEDUP=%.2fx%n", speedup);
    }

    @Test
    void benchmarkConcurrentRead100ThreadsAB() throws Exception {
        List<Stock> stocks = sortedActiveStocks();
        assertEquals(STOCK_COUNT, stocks.size());

        // Populate live quote snapshots (memory + Redis quote:latest:*) for all 100 stocks
        long baseTs = Instant.parse("2026-11-21T10:00:00Z").toEpochMilli();
        List<QuoteTick> seedTicks = new ArrayList<>(STOCK_COUNT);
        for (Stock stock : stocks) {
            seedTicks.add(new QuoteTick(stock.getSymbol(), new BigDecimal("150.00"), baseTs, 1L));
        }
        quoteLaneDispatcher.dispatchAll(seedTicks);

        // Seed 60 1-minute K-line bars for each of the 5 hot stocks (300 rows total)
        seedHotStockKlines();

        // Pre-generate deterministic 100-thread x 200-request workload (20,000 requests total)
        ReadRequest[][] workload = buildReadWorkload();

        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        // Warmup 100 threads x 10 requests = 1,000 requests
        ReadRequest[][] warmupWorkload = buildWarmupReadWorkload();
        properties.getReadCache().setEnabled(false);
        executeReadWorkload(httpClient, warmupWorkload);
        properties.getReadCache().setEnabled(true);
        executeReadWorkload(httpClient, warmupWorkload);

        // Run A: read-cache.enabled = false
        properties.getReadCache().setEnabled(false);
        marketQueryService.resetDbQueryCount();
        long comSelectBeforeOff = queryGlobalComSelect();
        ReadRunResult cacheOffRun = executeReadWorkload(httpClient, workload);
        long appDbQueriesOff = marketQueryService.getDbQueryCount();
        long comSelectDeltaOff = queryGlobalComSelect() - comSelectBeforeOff;

        // Run B: read-cache.enabled = true (cold read-cache start; preserve live quote:latest:* hashes)
        properties.getReadCache().setEnabled(true);
        deleteRedisByPattern("cache:*");
        deleteRedisByPattern("lock:*");
        marketQueryService.resetDbQueryCount();
        long comSelectBeforeOn = queryGlobalComSelect();
        ReadRunResult cacheOnRun = executeReadWorkload(httpClient, workload);
        long appDbQueriesOn = marketQueryService.getDbQueryCount();
        long comSelectDeltaOn = queryGlobalComSelect() - comSelectBeforeOn;

        double appReductionPct = (1.0 - ((double) appDbQueriesOn / appDbQueriesOff)) * 100.0;
        double comSelectReductionPct = (1.0 - ((double) comSelectDeltaOn / comSelectDeltaOff)) * 100.0;

        assertTrue(cacheOnRun.qps() > cacheOffRun.qps(),
                "Expected cache-on QPS (" + cacheOnRun.qps() + ") > cache-off QPS (" + cacheOffRun.qps() + ")");
        assertTrue(appDbQueriesOn < appDbQueriesOff * 0.05,
                "Expected >= 95% reduction in app DB queries, got off=" + appDbQueriesOff + ", on=" + appDbQueriesOn);

        System.out.println("=== PHASE6 CONCURRENT READ A/B SUMMARY (100 THREADS, 20,000 REQUESTS) ===");
        System.out.printf("READ_CACHE_OFF (enabled=false): elapsed=%.3fs, QPS=%.2f, P50=%.3fms, P95=%.3fms, P99=%.3fms, appDbQueries=%d, Com_select_delta=%d%n",
                cacheOffRun.elapsedSeconds(), cacheOffRun.qps(), cacheOffRun.p50Ms(), cacheOffRun.p95Ms(), cacheOffRun.p99Ms(),
                appDbQueriesOff, comSelectDeltaOff);
        System.out.printf("READ_CACHE_ON  (enabled=true) : elapsed=%.3fs, QPS=%.2f, P50=%.3fms, P95=%.3fms, P99=%.3fms, appDbQueries=%d, Com_select_delta=%d%n",
                cacheOnRun.elapsedSeconds(), cacheOnRun.qps(), cacheOnRun.p50Ms(), cacheOnRun.p95Ms(), cacheOnRun.p99Ms(),
                appDbQueriesOn, comSelectDeltaOn);
        System.out.printf("DB_QUERY_REDUCTION: app=%.2f%%, mysql_Com_select=%.2f%%, QPS_SPEEDUP=%.2fx%n",
                appReductionPct, comSelectReductionPct, cacheOnRun.qps() / cacheOffRun.qps());
    }

    private void runWarmupMatching(List<Stock> stocks, List<QuoteTick> warmupTicks, boolean forceDbFallback) throws Exception {
        resetAllState();
        seed5000Rules(stocks);
        ruleIndexMaintainer.rebuildAndAwaitDrain();
        quoteLaneDispatcher.resetForTest();
        ruleMatchEngine.resetForTest();
        ruleMatchEngine.attemptRecoveryIfNeeded();
        properties.getMatching().setForceDbFallback(forceDbFallback);

        dispatchReplayBounded(warmupTicks);
        drainMaintainer();
    }

    private MatchingRunResult runMatchingBenchmark(List<Stock> stocks,
                                                   List<QuoteTick> replayTicks,
                                                   boolean forceDbFallback) throws Exception {
        resetAllState();
        seed5000Rules(stocks);
        ruleIndexMaintainer.rebuildAndAwaitDrain();
        quoteLaneDispatcher.resetForTest();
        ruleMatchEngine.resetForTest();
        ruleMatchEngine.attemptRecoveryIfNeeded();
        alertTriggerService.resetMetricsForTest();
        outboxPublisher.resetMetricsForTest();
        alertNotificationConsumer.resetMetricsForTest();
        properties.getMatching().setForceDbFallback(forceDbFallback);

        DispatchTiming timing = dispatchReplayBounded(replayTicks);
        drainMaintainer();

        assertEquals(TOTAL_REPLAY_TICKS, quoteLaneDispatcher.getProcessedQuoteCount());
        assertEquals(0L, quoteLaneDispatcher.getOutOfOrderDiscardedCount());
        assertEquals(0L, quoteLaneDispatcher.getDroppedQuoteCount());
        if (forceDbFallback) {
            assertEquals(TOTAL_REPLAY_TICKS, ruleMatchEngine.getFallbackMatchCount());
            assertEquals(0L, ruleMatchEngine.getRedisMatchCount());
        } else {
            assertEquals(TOTAL_REPLAY_TICKS, ruleMatchEngine.getRedisMatchCount());
            assertEquals(0L, ruleMatchEngine.getFallbackMatchCount());
        }

        long triggeredCount = alertTriggerMapper.countAll();
        assertEquals(EXPECTED_TRIGGERS, triggeredCount);

        // Drain Outbox -> RocketMQ -> Consumer before resetting state for the next run
        awaitUntil(30_000, () ->
                outboxEventMapper.countByStatus(OutboxStatus.SENT) == EXPECTED_TRIGGERS
                        && outboxEventMapper.countByStatus(OutboxStatus.PENDING) == 0
                        && notificationMapper.countAll() == EXPECTED_TRIGGERS);
        Thread.sleep(200);

        double elapsedSeconds = timing.elapsedNanos() / 1_000_000_000.0;
        double tps = TOTAL_REPLAY_TICKS / elapsedSeconds;
        double[] pcts = percentilesMs(timing.latenciesNanos());
        return new MatchingRunResult(elapsedSeconds, tps, pcts[0], pcts[1], pcts[2], triggeredCount);
    }

    private DispatchTiming dispatchReplayBounded(List<QuoteTick> ticks) throws Exception {
        int count = ticks.size();
        long[] latenciesNanos = new long[count];
        Semaphore semaphore = new Semaphore(DISPATCH_CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(count);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        long wallStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final int idx = i;
            QuoteTick tick = ticks.get(i);
            semaphore.acquire();
            long t0 = System.nanoTime();
            quoteLaneDispatcher.dispatch(tick).whenComplete((ignored, ex) -> {
                latenciesNanos[idx] = System.nanoTime() - t0;
                if (ex != null) {
                    errors.add(ex);
                }
                semaphore.release();
                latch.countDown();
            });
        }
        assertTrue(latch.await(120, TimeUnit.SECONDS), "Timed out waiting for replay ticks");
        long wallEnd = System.nanoTime();
        assertTrue(errors.isEmpty(), "Unexpected dispatch errors: " + errors.peek());
        return new DispatchTiming(wallEnd - wallStart, latenciesNanos);
    }

    private ReadRunResult executeReadWorkload(HttpClient client, ReadRequest[][] workload) throws Exception {
        int threadCount = workload.length;
        int perThread = workload[0].length;
        long[] latenciesNanos = new long[threadCount * perThread];
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        try {
            List<Future<?>> futures = new ArrayList<>(threadCount);
            for (int t = 0; t < threadCount; t++) {
                final int threadIdx = t;
                futures.add(pool.submit(() -> {
                    ReadRequest[] reqs = workload[threadIdx];
                    int baseOffset = threadIdx * perThread;
                    ready.countDown();
                    try {
                        start.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failures.add("Interrupted waiting to start");
                        return;
                    }
                    for (int i = 0; i < reqs.length; i++) {
                        ReadRequest req = reqs[i];
                        HttpRequest httpReq = HttpRequest.newBuilder()
                                .uri(URI.create("http://127.0.0.1:" + port + req.path()))
                                .timeout(Duration.ofSeconds(10))
                                .GET()
                                .build();
                        long t0 = System.nanoTime();
                        try {
                            HttpResponse<String> resp = client.send(httpReq, HttpResponse.BodyHandlers.ofString());
                            long elapsed = System.nanoTime() - t0;
                            latenciesNanos[baseOffset + i] = elapsed;
                            if (resp.statusCode() != req.expectedStatus()) {
                                failures.add("Expected " + req.expectedStatus() + " for " + req.path()
                                        + " but got " + resp.statusCode());
                            }
                        } catch (Exception ex) {
                            latenciesNanos[baseOffset + i] = System.nanoTime() - t0;
                            failures.add("Request failed for " + req.path() + ": " + ex.getMessage());
                        }
                    }
                }));
            }

            assertTrue(ready.await(10, TimeUnit.SECONDS), "Read benchmark threads did not become ready");
            long wallStart = System.nanoTime();
            start.countDown();
            for (Future<?> future : futures) {
                future.get(120, TimeUnit.SECONDS);
            }
            long wallEnd = System.nanoTime();
            assertTrue(failures.isEmpty(), "Unexpected HTTP errors in read benchmark (first=" + failures.peek()
                    + ", total=" + failures.size() + ")");

            double elapsedSeconds = (wallEnd - wallStart) / 1_000_000_000.0;
            double qps = (threadCount * perThread) / elapsedSeconds;
            double[] pcts = percentilesMs(latenciesNanos);
            return new ReadRunResult(elapsedSeconds, qps, pcts[0], pcts[1], pcts[2]);
        } finally {
            pool.shutdownNow();
        }
    }

    private void seed5000Rules(List<Stock> stocks) {
        List<Object[]> rows = new ArrayList<>(STOCK_COUNT * RULES_PER_STOCK);
        for (Stock stock : stocks) {
            int ruleCounter = 0;
            for (int k = 0; k < 25; k++) {
                BigDecimal threshold = new BigDecimal(105 + k).setScale(2);
                long userId = (ruleCounter++ % 5) + 1L;
                rows.add(new Object[]{userId, stock.getId(), "ABOVE", threshold, "ACTIVE", 0L});
            }
            for (int k = 0; k < 25; k++) {
                BigDecimal threshold = new BigDecimal(95 - k).setScale(2);
                long userId = (ruleCounter++ % 5) + 1L;
                rows.add(new Object[]{userId, stock.getId(), "BELOW", threshold, "ACTIVE", 0L});
            }
        }
        jdbcTemplate.batchUpdate(
                "INSERT INTO alert_rules (user_id, stock_id, direction, threshold_price, status, version) VALUES (?, ?, ?, ?, ?, ?)",
                rows
        );
    }

    private void seedHotStockKlines() {
        List<Object[]> rows = new ArrayList<>(HOT_SYMBOLS.size() * 60);
        LocalDateTime startMinute = LocalDateTime.of(2026, 11, 21, 9, 0, 0).withNano(0);
        for (String symbol : HOT_SYMBOLS) {
            Stock stock = stockCatalog.getBySymbol(symbol);
            for (int m = 0; m < 60; m++) {
                LocalDateTime ts = startMinute.plusMinutes(m);
                BigDecimal open = BigDecimal.valueOf(14800L + m * 3L, 2);
                BigDecimal high = open.add(new BigDecimal("1.25"));
                BigDecimal low = open.subtract(new BigDecimal("0.90"));
                BigDecimal close = open.add(new BigDecimal("0.40"));
                rows.add(new Object[]{
                        stock.getId(),
                        Timestamp.valueOf(ts.atOffset(ZoneOffset.UTC).toLocalDateTime()),
                        open, high, low, close
                });
            }
        }
        jdbcTemplate.batchUpdate(
                "INSERT INTO kline_1m (stock_id, minute_ts, open_price, high_price, low_price, close_price) VALUES (?, ?, ?, ?, ?, ?)",
                rows
        );
    }

    private List<QuoteTick> buildWarmupTicks(List<Stock> stocks, int waves, long baseTs) {
        Random random = new Random(12345L);
        List<QuoteTick> ticks = new ArrayList<>(waves * stocks.size());
        for (int w = 1; w <= waves; w++) {
            long eventTs = baseTs + w * 100L;
            for (Stock stock : stocks) {
                BigDecimal price = BigDecimal.valueOf(9600L + random.nextInt(801), 2);
                ticks.add(new QuoteTick(stock.getSymbol(), price, eventTs, w));
            }
        }
        return ticks;
    }

    private List<QuoteTick> buildDeterministicReplayTicks(List<Stock> stocks, long baseTs) {
        Random random = new Random(20261121L);
        List<QuoteTick> ticks = new ArrayList<>(TOTAL_REPLAY_TICKS);
        for (int w = 1; w <= REPLAY_WAVES; w++) {
            long eventTs = baseTs + w * 100L;
            BigDecimal forcedPrice = switch (w) {
                case 40 -> new BigDecimal("105.00");
                case 80 -> new BigDecimal("106.00");
                case 120 -> new BigDecimal("107.00");
                case 160 -> new BigDecimal("95.00");
                case 190 -> new BigDecimal("94.00");
                default -> null;
            };
            for (Stock stock : stocks) {
                BigDecimal price = forcedPrice != null
                        ? forcedPrice
                        : BigDecimal.valueOf(9600L + random.nextInt(801), 2);
                ticks.add(new QuoteTick(stock.getSymbol(), price, eventTs, w));
            }
        }
        return ticks;
    }

    private ReadRequest[][] buildReadWorkload() {
        Random random = new Random(20261121L);
        ReadRequest[][] workload = new ReadRequest[READ_THREADS][READ_REQUESTS_PER_THREAD];
        for (int t = 0; t < READ_THREADS; t++) {
            for (int i = 0; i < READ_REQUESTS_PER_THREAD; i++) {
                int r = random.nextInt(100);
                if (r < 45) {
                    String sym = HOT_SYMBOLS.get(random.nextInt(HOT_SYMBOLS.size()));
                    workload[t][i] = new ReadRequest("/api/quotes/" + sym, 200);
                } else if (r < 90) {
                    String sym = HOT_SYMBOLS.get(random.nextInt(HOT_SYMBOLS.size()));
                    workload[t][i] = new ReadRequest("/api/klines/" + sym + "?limit=60", 200);
                } else if (r < 95) {
                    String invalid = String.format("INVALID_%02d", random.nextInt(50));
                    workload[t][i] = new ReadRequest("/api/quotes/" + invalid, 404);
                } else {
                    String invalid = String.format("INVALID_%02d", random.nextInt(50));
                    workload[t][i] = new ReadRequest("/api/klines/" + invalid + "?limit=60", 404);
                }
            }
        }
        return workload;
    }

    private ReadRequest[][] buildWarmupReadWorkload() {
        Random random = new Random(999L);
        ReadRequest[][] workload = new ReadRequest[READ_THREADS][10];
        for (int t = 0; t < READ_THREADS; t++) {
            for (int i = 0; i < 10; i++) {
                String sym = HOT_SYMBOLS.get(random.nextInt(HOT_SYMBOLS.size()));
                workload[t][i] = (i % 2 == 0)
                        ? new ReadRequest("/api/quotes/" + sym, 200)
                        : new ReadRequest("/api/klines/" + sym + "?limit=60", 200);
            }
        }
        return workload;
    }

    private long queryGlobalComSelect() {
        Map<String, Object> row = jdbcTemplate.queryForMap("SHOW GLOBAL STATUS LIKE 'Com_select'");
        return Long.parseLong(String.valueOf(row.get("Value")));
    }

    private List<Stock> sortedActiveStocks() {
        return stockCatalog.getAllActive().stream()
                .sorted(Comparator.comparing(Stock::getId))
                .toList();
    }

    private void drainMaintainer() throws Exception {
        ruleIndexMaintainer.getMaintainerExecutor().submit(() -> null).get(30, TimeUnit.SECONDS);
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

    private static double[] percentilesMs(long[] latenciesNanos) {
        long[] copy = Arrays.copyOf(latenciesNanos, latenciesNanos.length);
        Arrays.sort(copy);
        return new double[]{
                percentileAt(copy, 0.50) / 1_000_000.0,
                percentileAt(copy, 0.95) / 1_000_000.0,
                percentileAt(copy, 0.99) / 1_000_000.0
        };
    }

    private static long percentileAt(long[] sorted, double p) {
        int idx = Math.min(sorted.length - 1, Math.max(0, (int) Math.ceil(p * sorted.length) - 1));
        return sorted[idx];
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

    private record DispatchTiming(long elapsedNanos, long[] latenciesNanos) {
    }

    private record MatchingRunResult(
            double elapsedSeconds,
            double tps,
            double p50Ms,
            double p95Ms,
            double p99Ms,
            long triggeredCount
    ) {
    }

    private record ReadRequest(String path, int expectedStatus) {
    }

    private record ReadRunResult(
            double elapsedSeconds,
            double qps,
            double p50Ms,
            double p95Ms,
            double p99Ms
    ) {
    }
}
