package com.pricepulse.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class PriceCents {

    private PriceCents() {
    }

    public static long toCents(BigDecimal price) {
        if (price == null) {
            throw new IllegalArgumentException("price must not be null");
        }
        return price.setScale(2, RoundingMode.HALF_UP)
                .movePointRight(2)
                .longValueExact();
    }

    public static BigDecimal fromCents(long cents) {
        return BigDecimal.valueOf(cents, 2);
    }
}
