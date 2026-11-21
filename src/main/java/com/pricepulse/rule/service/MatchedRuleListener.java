package com.pricepulse.rule.service;

import com.pricepulse.market.model.QuoteSnapshot;
import java.util.List;

public interface MatchedRuleListener {

    void onMatchedRules(QuoteSnapshot snapshot, List<Long> matchedRuleIds);
}
