package com.pricepulse.market.service;

import com.pricepulse.market.mapper.StockMapper;
import com.pricepulse.market.model.Stock;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public class StockCatalog {

    private final StockMapper stockMapper;
    private final Map<String, Stock> bySymbol = new ConcurrentHashMap<>();
    private final Map<Long, Stock> byId = new ConcurrentHashMap<>();

    public StockCatalog(StockMapper stockMapper) {
        this.stockMapper = stockMapper;
    }

    @PostConstruct
    public void reload() {
        List<Stock> active = stockMapper.findAllActive();
        bySymbol.clear();
        byId.clear();
        for (Stock stock : active) {
            String normalized = stock.getSymbol().toUpperCase(Locale.ROOT);
            bySymbol.put(normalized, stock);
            byId.put(stock.getId(), stock);
        }
    }

    public Stock getBySymbol(String symbol) {
        if (symbol == null) {
            return null;
        }
        return bySymbol.get(symbol.trim().toUpperCase(Locale.ROOT));
    }

    public Stock getById(Long id) {
        return id == null ? null : byId.get(id);
    }

    public List<Stock> getAllActive() {
        return Collections.unmodifiableList(new ArrayList<>(byId.values()));
    }

    public Collection<Long> getAllStockIds() {
        return Collections.unmodifiableSet(byId.keySet());
    }
}
