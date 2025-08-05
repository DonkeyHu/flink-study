package com.donkey.flink.entity;

public class OrderPOJO {
    public String userId;
    public double amount;
    public long timestamp;

    public OrderPOJO() {
    }

    public OrderPOJO(String userId, double amount, long timestamp) {
        this.userId = userId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderPOJO{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
