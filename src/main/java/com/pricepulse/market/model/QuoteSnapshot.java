package com.pricepulse.market.model;

import java.math.BigDecimal;

public record QuoteSnapshot(
        Long stockId,
        String symbol,
        BigDecimal price,
        long priceCents,
        long eventTs,
        long seq
) {
}
