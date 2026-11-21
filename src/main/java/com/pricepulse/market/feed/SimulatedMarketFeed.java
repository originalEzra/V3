package com.pricepulse.market.feed;

import com.pricepulse.market.model.QuoteTick;
import com.pricepulse.market.model.Stock;
import com.pricepulse.market.service.StockCatalog;
import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

@Component
public class SimulatedMarketFeed implements MarketFeedSource {

    private static final BigDecimal MIN_PRICE = new BigDecimal("1.00");

    private final StockCatalog stockCatalog;
    private final Map<String, SymbolState> states = new ConcurrentHashMap<>();

    public SimulatedMarketFeed(StockCatalog stockCatalog) {
        this.stockCatalog = stockCatalog;
    }

    @PostConstruct
    public void init() {
        initFromCatalog();
    }

    public void initFromCatalog() {
        for (Stock stock : stockCatalog.getAllActive()) {
            String symbol = stock.getSymbol().toUpperCase(Locale.ROOT);
            states.computeIfAbsent(symbol, s -> {
                long baseCents = 10_000L + Math.floorMod(s.hashCode(), 40_000);
                return new SymbolState(BigDecimal.valueOf(baseCents, 2));
            });
        }
    }

    @Override
    public QuoteTick nextTick(String symbol) {
        String normalized = symbol.trim().toUpperCase(Locale.ROOT);
        SymbolState state = states.computeIfAbsent(normalized, s -> new SymbolState(new BigDecimal("150.00")));
        double deltaRatio = ThreadLocalRandom.current().nextDouble(-0.005, 0.005);
        return state.advanceByRatio(normalized, deltaRatio, System.currentTimeMillis());
    }

    @Override
    public List<QuoteTick> emitBatch() {
        if (states.isEmpty()) {
            initFromCatalog();
        }
        long now = System.currentTimeMillis();
        List<QuoteTick> ticks = new ArrayList<>(states.size());
        for (Map.Entry<String, SymbolState> entry : states.entrySet()) {
            double deltaRatio = ThreadLocalRandom.current().nextDouble(-0.005, 0.005);
            ticks.add(entry.getValue().advanceByRatio(entry.getKey(), deltaRatio, now));
        }
        return ticks;
    }

    @Override
    public List<QuoteTick> triggerCrashStorm(double dropRatio) {
        if (dropRatio <= 0.0 || dropRatio >= 1.0) {
            throw new IllegalArgumentException("dropRatio must be between 0 and 1");
        }
        if (states.isEmpty()) {
            initFromCatalog();
        }
        long now = System.currentTimeMillis();
        List<QuoteTick> ticks = new ArrayList<>(states.size());
        for (Map.Entry<String, SymbolState> entry : states.entrySet()) {
            ticks.add(entry.getValue().advanceByRatio(entry.getKey(), -dropRatio, now));
        }
        return ticks;
    }

    public void setBasePrice(String symbol, BigDecimal price) {
        String normalized = symbol.trim().toUpperCase(Locale.ROOT);
        states.computeIfAbsent(normalized, s -> new SymbolState(price)).resetPrice(price);
    }

    private static final class SymbolState {
        private final AtomicLong seq = new AtomicLong(0);
        private BigDecimal currentPrice;
        private long lastEventTs = Long.MIN_VALUE;

        private SymbolState(BigDecimal initialPrice) {
            this.currentPrice = initialPrice.setScale(2, RoundingMode.HALF_UP);
        }

        private synchronized QuoteTick advanceByRatio(String symbol, double deltaRatio, long eventTs) {
            BigDecimal multiplier = BigDecimal.valueOf(1.0 + deltaRatio);
            BigDecimal next = currentPrice.multiply(multiplier).setScale(2, RoundingMode.HALF_UP);
            if (next.compareTo(MIN_PRICE) < 0) {
                next = MIN_PRICE;
            }
            currentPrice = next;
            long effectiveTs = Math.max(eventTs, lastEventTs);
            lastEventTs = effectiveTs;
            long nextSeq = seq.incrementAndGet();
            return new QuoteTick(symbol, next, effectiveTs, nextSeq);
        }

        private synchronized void resetPrice(BigDecimal price) {
            this.currentPrice = price.setScale(2, RoundingMode.HALF_UP);
        }
    }
}
