package com.donkey.flink.entity;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class CurrencyRate implements SpecificRecord {
    private String currencyCode;
    private double usRate;
    private long rateTimestamp;

    public String getCurrencyCode() {
        return currencyCode;
    }

    public void setCurrencyCode(String currencyCode) {
        this.currencyCode = currencyCode;
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
