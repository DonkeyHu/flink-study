package com.donkey.flink.entity;

public class Order {

    public String orderId;
    public String currencyCode;
    public double amount;
    public long orderTimestamp;

    public Order() {
    }

    public Order(String orderId, String currencyCode, double amount, long orderTimestamp) {
        this.orderId = orderId;
        this.currencyCode = currencyCode;
        this.amount = amount;
        this.orderTimestamp = orderTimestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCurrencyCode() {
        return currencyCode;
    }

    public void setCurrencyCode(String currencyCode) {
        this.currencyCode = currencyCode;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getOrderTimestamp() {
        return orderTimestamp;
    }

    public void setOrderTimestamp(long orderTimestamp) {
        this.orderTimestamp = orderTimestamp;
    }
}
