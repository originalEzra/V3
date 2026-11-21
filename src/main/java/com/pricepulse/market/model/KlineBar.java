package com.pricepulse.market.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class KlineBar {

    private Long stockId;
    private LocalDateTime minuteTs;
    private BigDecimal openPrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closePrice;

    public KlineBar() {
    }

    public KlineBar(Long stockId, LocalDateTime minuteTs, BigDecimal openPrice,
                    BigDecimal highPrice, BigDecimal lowPrice, BigDecimal closePrice) {
        this.stockId = stockId;
        this.minuteTs = minuteTs;
        this.openPrice = openPrice;
        this.highPrice = highPrice;
        this.lowPrice = lowPrice;
        this.closePrice = closePrice;
    }

    public Long getStockId() {
        return stockId;
    }

    public void setStockId(Long stockId) {
        this.stockId = stockId;
    }

    public LocalDateTime getMinuteTs() {
        return minuteTs;
    }

    public void setMinuteTs(LocalDateTime minuteTs) {
        this.minuteTs = minuteTs;
    }

    public BigDecimal getOpenPrice() {
        return openPrice;
    }

    public void setOpenPrice(BigDecimal openPrice) {
        this.openPrice = openPrice;
    }

    public BigDecimal getHighPrice() {
        return highPrice;
    }

    public void setHighPrice(BigDecimal highPrice) {
        this.highPrice = highPrice;
    }

    public BigDecimal getLowPrice() {
        return lowPrice;
    }

    public void setLowPrice(BigDecimal lowPrice) {
        this.lowPrice = lowPrice;
    }

    public BigDecimal getClosePrice() {
        return closePrice;
    }

    public void setClosePrice(BigDecimal closePrice) {
        this.closePrice = closePrice;
    }
}
