package com.pricepulse.market.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricepulse.PricePulseProperties;
import com.pricepulse.common.PriceCents;
import com.pricepulse.market.mapper.KlineMapper;
import com.pricepulse.market.mapper.StockMapper;
import com.pricepulse.market.model.KlineBar;
import com.pricepulse.market.model.QuoteSnapshot;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.pipeline.QuoteLaneDispatcher;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class MarketQueryService {

    public static final String NULL_SENTINEL = "__NULL__";
    private static final Logger log = LoggerFactory.getLogger(MarketQueryService.class);
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class
    );

    private final PricePulseProperties properties;
    private final StockMapper stockMapper;
    private final KlineMapper klineMapper;
    private final QuoteLaneDispatcher quoteLaneDispatcher;
    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;
    private final AtomicLong dbQueryCount = new AtomicLong(0);

    public MarketQueryService(PricePulseProperties properties,
                              StockMapper stockMapper,
                              KlineMapper klineMapper,
                              QuoteLaneDispatcher quoteLaneDispatcher,
                              StringRedisTemplate stringRedisTemplate,
                              ObjectMapper objectMapper) {
        this.properties = properties;
        this.stockMapper = stockMapper;
        this.klineMapper = klineMapper;
        this.quoteLaneDispatcher = quoteLaneDispatcher;
        this.stringRedisTemplate = stringRedisTemplate;
        this.objectMapper = objectMapper;
    }

    public List<Stock> listStocks() {
        return listStocks(null);
    }

    public List<Stock> listStocks(String query) {
        List<Stock> all = stockMapper.findAllActive();
        if (query == null || query.isBlank()) {
            return all;
        }
        String needle = query.trim().toUpperCase(Locale.ROOT);
        return all.stream()
                .filter(stock -> stock.getSymbol().toUpperCase(Locale.ROOT).contains(needle)
                        || stock.getName().toUpperCase(Locale.ROOT).contains(needle))
                .toList();
    }

    public QuoteSnapshot getLatestQuote(String rawSymbol) {
        String symbol = normalizeSymbol(rawSymbol);
        if (!properties.getReadCache().isEnabled()) {
            Stock stock = findStockInDb(symbol);
            if (stock == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
            }
            return resolveQuoteSnapshot(stock);
        }

        String cacheKey = "cache:quote:" + symbol;
        try {
            Map<Object, Object> liveHash = stringRedisTemplate.opsForHash().entries("quote:latest:" + symbol);
            if (liveHash != null && !liveHash.isEmpty()) {
                return new QuoteSnapshot(
                        Long.parseLong(String.valueOf(liveHash.get("stockId"))),
                        String.valueOf(liveHash.get("symbol")),
                        new BigDecimal(String.valueOf(liveHash.get("price"))),
                        Long.parseLong(String.valueOf(liveHash.get("priceCents"))),
                        Long.parseLong(String.valueOf(liveHash.get("eventTs"))),
                        Long.parseLong(String.valueOf(liveHash.get("seq")))
                );
            }
            String cached = stringRedisTemplate.opsForValue().get(cacheKey);
            if (NULL_SENTINEL.equals(cached)) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
            }
            if (cached != null) {
                return objectMapper.readValue(cached, QuoteSnapshot.class);
            }
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception ex) {
            log.debug("Redis quote cache read failed for {}: {}", symbol, ex.getMessage());
        }

        Stock stock = findStockInDb(symbol);
        if (stock == null) {
            writeNullSentinelSafely(cacheKey);
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
        }
        QuoteSnapshot snapshot = resolveQuoteSnapshot(stock);
        if (snapshot != null) {
            writeJsonCacheSafely(cacheKey, snapshot, nextBaseTtlSeconds());
        }
        return snapshot;
    }

    public List<KlineBar> getKlines(String rawSymbol, int requestedLimit) {
        String symbol = normalizeSymbol(rawSymbol);
        int limit = Math.max(1, Math.min(500, requestedLimit));
        if (!properties.getReadCache().isEnabled()) {
            return queryKlinesFromDb(symbol, limit);
        }

        String cacheKey = klineCacheKey(symbol, limit);
        try {
            String cached = stringRedisTemplate.opsForValue().get(cacheKey);
            if (NULL_SENTINEL.equals(cached)) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
            }
            if (cached != null) {
                return deserializeKlines(cached);
            }

            String lockKey = "lock:kline:" + symbol;
            String lockToken = UUID.randomUUID().toString();
            for (int attempt = 0; attempt < 25; attempt++) {
                Boolean acquired = stringRedisTemplate.opsForValue()
                        .setIfAbsent(lockKey, lockToken, Duration.ofSeconds(5));
                if (Boolean.TRUE.equals(acquired)) {
                    try {
                        String doubleCheck = stringRedisTemplate.opsForValue().get(cacheKey);
                        if (NULL_SENTINEL.equals(doubleCheck)) {
                            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
                        }
                        if (doubleCheck != null) {
                            return deserializeKlines(doubleCheck);
                        }

                        Stock stock = findStockInDb(symbol);
                        if (stock == null) {
                            writeNullSentinelSafely(cacheKey);
                            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
                        }
                        List<KlineBar> bars = queryKlineBarsInDb(stock.getId(), limit);
                        writeJsonCacheSafely(cacheKey, bars, nextBaseTtlSeconds());
                        return bars;
                    } finally {
                        releaseLockSafely(lockKey, lockToken);
                    }
                }

                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                String retryCached = stringRedisTemplate.opsForValue().get(cacheKey);
                if (NULL_SENTINEL.equals(retryCached)) {
                    throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
                }
                if (retryCached != null) {
                    return deserializeKlines(retryCached);
                }
            }
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception ex) {
            log.debug("Redis kline cache path failed for {}, falling back to MySQL: {}", symbol, ex.getMessage());
        }

        return queryKlinesFromDb(symbol, limit);
    }

    public static String klineCacheKey(String symbol, int limit) {
        return "cache:kline:" + symbol.trim().toUpperCase(Locale.ROOT) + ":" + limit;
    }

    public long getDbQueryCount() {
        return dbQueryCount.get();
    }

    public void resetDbQueryCount() {
        dbQueryCount.set(0);
    }

    private List<KlineBar> queryKlinesFromDb(String symbol, int limit) {
        Stock stock = findStockInDb(symbol);
        if (stock == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown stock symbol: " + symbol);
        }
        return queryKlineBarsInDb(stock.getId(), limit);
    }

    private Stock findStockInDb(String symbol) {
        dbQueryCount.incrementAndGet();
        return stockMapper.findBySymbol(symbol);
    }

    private List<KlineBar> queryKlineBarsInDb(Long stockId, int limit) {
        dbQueryCount.incrementAndGet();
        List<KlineBar> recent = klineMapper.findRecentByStockId(stockId, limit);
        if (recent.isEmpty()) {
            return Collections.emptyList();
        }
        List<KlineBar> ascending = new ArrayList<>(recent);
        ascending.sort(Comparator.comparing(KlineBar::getMinuteTs));
        return ascending;
    }

    private QuoteSnapshot resolveQuoteSnapshot(Stock stock) {
        QuoteSnapshot inMemory = quoteLaneDispatcher.getLatestQuote(stock.getSymbol());
        if (inMemory != null) {
            return inMemory;
        }
        List<KlineBar> latestBars = queryKlineBarsInDb(stock.getId(), 1);
        if (latestBars.isEmpty()) {
            return null;
        }
        KlineBar bar = latestBars.get(0);
        return new QuoteSnapshot(
                stock.getId(),
                stock.getSymbol(),
                bar.getClosePrice(),
                PriceCents.toCents(bar.getClosePrice()),
                0L,
                0L
        );
    }

    private List<KlineBar> deserializeKlines(String json) throws Exception {
        return objectMapper.readValue(json, new TypeReference<>() {});
    }

    private void writeNullSentinelSafely(String cacheKey) {
        try {
            long ttl = nextNullTtlSeconds();
            stringRedisTemplate.opsForValue().set(cacheKey, NULL_SENTINEL, Duration.ofSeconds(ttl));
        } catch (RuntimeException ex) {
            log.debug("Failed writing null sentinel to {}: {}", cacheKey, ex.getMessage());
        }
    }

    private void writeJsonCacheSafely(String cacheKey, Object value, long ttlSeconds) {
        try {
            String json = objectMapper.writeValueAsString(value);
            stringRedisTemplate.opsForValue().set(cacheKey, json, Duration.ofSeconds(ttlSeconds));
        } catch (Exception ex) {
            log.debug("Failed writing cache value to {}: {}", cacheKey, ex.getMessage());
        }
    }

    private void releaseLockSafely(String lockKey, String lockToken) {
        try {
            stringRedisTemplate.execute(UNLOCK_SCRIPT, List.of(lockKey), lockToken);
        } catch (RuntimeException ex) {
            log.debug("Failed releasing lock {}: {}", lockKey, ex.getMessage());
        }
    }

    private long nextBaseTtlSeconds() {
        long base = properties.getReadCache().getBaseTtlSeconds();
        int maxJitter = properties.getReadCache().getJitterMaxSeconds();
        int jitter = maxJitter > 0 ? ThreadLocalRandom.current().nextInt(maxJitter + 1) : 0;
        return base + jitter;
    }

    private long nextNullTtlSeconds() {
        long base = properties.getReadCache().getNullTtlSeconds();
        int jitter = ThreadLocalRandom.current().nextInt(11);
        return base + jitter;
    }

    private String normalizeSymbol(String rawSymbol) {
        if (rawSymbol == null || rawSymbol.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Symbol must not be blank");
        }
        return rawSymbol.trim().toUpperCase(Locale.ROOT);
    }
}
