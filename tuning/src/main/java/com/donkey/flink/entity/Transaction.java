package com.donkey.flink.entity;

import java.util.Objects;

public class Transaction {

    private long accountId;

    private long timestamp;

    private double amount;

    public Transaction() {
    }

    public Transaction(long accountId, long timestamp, double amount) {
        this.accountId = accountId;
        this.timestamp = timestamp;
        this.amount = amount;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Transaction that = (Transaction) obj;
        return accountId == that.accountId && timestamp == that.timestamp && Double.compare(amount, that.amount) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, timestamp, amount);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "accountId=" + accountId +
                ", timestamp=" + timestamp +
                ", amount=" + amount +
                '}';
    }
}
