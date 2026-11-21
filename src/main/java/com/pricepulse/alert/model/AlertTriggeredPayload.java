package com.pricepulse.alert.model;

import com.pricepulse.rule.model.Direction;
import java.math.BigDecimal;

public record AlertTriggeredPayload(
        Long triggerId,
        Long ruleId,
        Long userId,
        Long stockId,
        String symbol,
        Direction direction,
        BigDecimal thresholdPrice,
        BigDecimal triggerPrice,
        String triggeredAt,
        String content
) {
}
