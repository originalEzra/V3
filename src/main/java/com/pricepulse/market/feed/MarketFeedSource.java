package com.pricepulse.market.feed;

import com.pricepulse.market.model.QuoteTick;
import java.util.List;

public interface MarketFeedSource {

    QuoteTick nextTick(String symbol);

    List<QuoteTick> emitBatch();

    List<QuoteTick> triggerCrashStorm(double dropRatio);
}
