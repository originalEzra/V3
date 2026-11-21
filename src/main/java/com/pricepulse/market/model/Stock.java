package com.pricepulse.market.model;

public class Stock {

    private Long id;
    private String symbol;
    private String name;
    private String status;

    public Stock() {
    }

    public Stock(Long id, String symbol, String name, String status) {
        this.id = id;
        this.symbol = symbol;
        this.name = name;
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
