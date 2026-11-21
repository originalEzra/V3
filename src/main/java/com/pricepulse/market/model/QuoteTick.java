package com.pricepulse.market.model;

import java.math.BigDecimal;

public record QuoteTick(
        String symbol,
        BigDecimal price,
        long eventTs,
        long seq
) {
}
