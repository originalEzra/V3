package com.pricepulse.rule.service;

import com.pricepulse.PricePulseProperties;
import com.pricepulse.rule.mapper.AlertRuleMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionCommands;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RuleMatchEngine {

    private static final Logger log = LoggerFactory.getLogger(RuleMatchEngine.class);

    private final PricePulseProperties properties;
    private final AlertRuleMapper alertRuleMapper;
    private final RuleIndexMaintainer ruleIndexMaintainer;
    private final StringRedisTemplate stringRedisTemplate;
    private final DefaultRedisScript<List> matchRulesScript;
    private final CircuitBreaker circuitBreaker;
    private final String redisHost;
    private final int redisPort;

    private final AtomicBoolean redisIndexReady = new AtomicBoolean(false);
    private final AtomicBoolean recoveryInProgress = new AtomicBoolean(false);
    private final AtomicLong redisMatchCount = new AtomicLong(0);
    private final AtomicLong fallbackMatchCount = new AtomicLong(0);

    public RuleMatchEngine(PricePulseProperties properties,
                           AlertRuleMapper alertRuleMapper,
                           RuleIndexMaintainer ruleIndexMaintainer,
                           StringRedisTemplate stringRedisTemplate,
                           @Value("${spring.data.redis.host:127.0.0.1}") String redisHost,
                           @Value("${spring.data.redis.port:6379}") int redisPort) {
        this.properties = properties;
        this.alertRuleMapper = alertRuleMapper;
        this.ruleIndexMaintainer = ruleIndexMaintainer;
        this.stringRedisTemplate = stringRedisTemplate;
        this.redisHost = redisHost;
        this.redisPort = redisPort;

        this.matchRulesScript = new DefaultRedisScript<>();
        this.matchRulesScript.setLocation(new ClassPathResource("lua/match_rules.lua"));
        this.matchRulesScript.setResultType(List.class);

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .minimumNumberOfCalls(5)
                .slidingWindowSize(10)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(2)
                .automaticTransitionFromOpenToHalfOpenEnabled(false)
                .build();
        this.circuitBreaker = CircuitBreaker.of("redisRuleMatch", config);
        this.circuitBreaker.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition().getToState() == CircuitBreaker.State.OPEN) {
                redisIndexReady.set(false);
                log.warn("Redis rule match circuit breaker transitioned to OPEN; switching to MySQL fallback");
            }
        });
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initializeOnStartup() {
        try {
            ruleIndexMaintainer.rebuildAndAwaitDrain();
            redisIndexReady.set(true);
            log.info("Initialized Redis rule ZSet index and sentinel key");
        } catch (RuntimeException ex) {
            redisIndexReady.set(false);
            log.warn("Startup Redis index build failed; starting in MySQL fallback mode: {}", ex.getMessage());
        }
    }

    public List<Long> match(Long stockId, BigDecimal price, long priceCents) {
        if (properties.getMatching().isForceDbFallback()
                || !redisIndexReady.get()
                || !circuitBreaker.tryAcquirePermission()) {
            return executeDbFallback(stockId, price);
        }

        long startNanos = System.nanoTime();
        try {
            List<?> raw = stringRedisTemplate.execute(
                    matchRulesScript,
                    List.of(
                            RuleIndexMaintainer.INDEX_READY_KEY,
                            RuleIndexMaintainer.aboveKey(stockId),
                            RuleIndexMaintainer.belowKey(stockId)
                    ),
                    String.valueOf(priceCents)
            );
            long elapsedNanos = System.nanoTime() - startNanos;
            if (raw != null && raw.size() == 1 && "NOT_READY".equals(String.valueOf(raw.get(0)))) {
                redisIndexReady.set(false);
                circuitBreaker.onError(elapsedNanos, TimeUnit.NANOSECONDS,
                        new IllegalStateException("Redis rule index sentinel missing: NOT_READY"));
                return executeDbFallback(stockId, price);
            }

            circuitBreaker.onSuccess(elapsedNanos, TimeUnit.NANOSECONDS);
            redisMatchCount.incrementAndGet();
            if (raw == null || raw.isEmpty()) {
                return Collections.emptyList();
            }
            List<Long> ids = new ArrayList<>(raw.size());
            for (Object item : raw) {
                ids.add(Long.parseLong(String.valueOf(item)));
            }
            return ids;
        } catch (RuntimeException ex) {
            long elapsedNanos = System.nanoTime() - startNanos;
            circuitBreaker.onError(elapsedNanos, TimeUnit.NANOSECONDS, ex);
            return executeDbFallback(stockId, price);
        }
    }

    public void recordExternalRedisError(Throwable ex) {
        circuitBreaker.onError(0, TimeUnit.NANOSECONDS, ex);
    }

    @Scheduled(fixedDelayString = "${pricepulse.matching.recovery-probe-interval-ms:2000}",
            initialDelayString = "${pricepulse.matching.recovery-probe-interval-ms:2000}")
    public void scheduledRecoveryProbe() {
        attemptRecoveryIfNeeded();
    }

    public boolean attemptRecoveryIfNeeded() {
        if (properties.getMatching().isForceDbFallback()) {
            return false;
        }
        if (redisIndexReady.get() && circuitBreaker.getState() == CircuitBreaker.State.CLOSED) {
            return true;
        }
        if (!recoveryInProgress.compareAndSet(false, true)) {
            return false;
        }
        try {
            redisIndexReady.set(false);
            if (!isTcpPortReachable(redisHost, redisPort, 300)) {
                return false;
            }
            String pong = pingWithReconnectFallback();
            if (!"PONG".equalsIgnoreCase(pong)) {
                return false;
            }
            ruleIndexMaintainer.rebuildAndAwaitDrain();
            if (circuitBreaker.getState() != CircuitBreaker.State.CLOSED) {
                circuitBreaker.transitionToClosedState();
            }
            redisIndexReady.set(true);
            log.info("Recovery Gate completed Redis rebuild + queue drain; switched matching back to Redis");
            return true;
        } catch (RuntimeException ex) {
            redisIndexReady.set(false);
            log.debug("Redis recovery probe unsuccessful, staying on MySQL fallback: {}", ex.getMessage());
            return false;
        } finally {
            recoveryInProgress.set(false);
        }
    }

    public boolean isRedisIndexReady() {
        return redisIndexReady.get();
    }

    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }

    public String getCurrentMode() {
        if (properties.getMatching().isForceDbFallback()
                || !redisIndexReady.get()
                || circuitBreaker.getState() != CircuitBreaker.State.CLOSED) {
            return "DB_FALLBACK";
        }
        return "REDIS";
    }

    public long getRedisMatchCount() {
        return redisMatchCount.get();
    }

    public long getFallbackMatchCount() {
        return fallbackMatchCount.get();
    }

    public void resetForTest() {
        if (circuitBreaker.getState() != CircuitBreaker.State.CLOSED) {
            circuitBreaker.transitionToClosedState();
        }
        redisMatchCount.set(0);
        fallbackMatchCount.set(0);
        redisIndexReady.set(false);
    }

    private List<Long> executeDbFallback(Long stockId, BigDecimal price) {
        fallbackMatchCount.incrementAndGet();
        return alertRuleMapper.findMatchingRuleIdsFallback(stockId, price);
    }

    private String pingWithReconnectFallback() {
        try {
            return stringRedisTemplate.execute((RedisCallback<String>) RedisConnectionCommands::ping);
        } catch (RuntimeException firstEx) {
            RedisConnectionFactory factory = stringRedisTemplate.getConnectionFactory();
            if (factory instanceof LettuceConnectionFactory lettuceFactory) {
                lettuceFactory.resetConnection();
                return stringRedisTemplate.execute((RedisCallback<String>) RedisConnectionCommands::ping);
            }
            throw firstEx;
        }
    }

    private boolean isTcpPortReachable(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
