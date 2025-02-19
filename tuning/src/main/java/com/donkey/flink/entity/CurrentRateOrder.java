package com.donkey.flink.entity;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class CurrentRateOrder implements SpecificRecord {
    public String orderId;
    public String currencyCode;
    public double amount;
    public long orderTimestamp;
    private double usRate;
    private long rateTimestamp;

    public double dollar;

    public CurrentRateOrder(String orderId, String currencyCode, double amount, long orderTimestamp, double usRate, long rateTimestamp, double dollar) {
        this.orderId = orderId;
        this.currencyCode = currencyCode;
        this.amount = amount;
        this.orderTimestamp = orderTimestamp;
        this.usRate = usRate;
        this.rateTimestamp = rateTimestamp;
        this.dollar = dollar;
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

    public double getUsRate() {
        return usRate;
    }

    public void setUsRate(double usRate) {
        this.usRate = usRate;
    }

    public long getRateTimestamp() {
        return rateTimestamp;
    }

    public void setRateTimestamp(long rateTimestamp) {
        this.rateTimestamp = rateTimestamp;
    }

    public double getDollar() {
        return dollar;
    }

    public void setDollar(double dollar) {
        this.dollar = dollar;
    }

    @Override
    public void put(int i, Object v) {

    }

    @Override
    public Object get(int i) {
        return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
