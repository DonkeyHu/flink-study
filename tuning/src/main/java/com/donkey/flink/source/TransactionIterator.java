package com.donkey.flink.source;

import com.donkey.flink.entity.Transaction;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransactionIterator implements Iterator<Transaction>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Timestamp INIT_TIMESTAMP = Timestamp.valueOf("2023-01-01 00:00:00");

    private static final long SIX_MINUTES = 6 * 60 * 1000;

    private final boolean bounded;

    private long timestamp;

    private int index = 0;

    public TransactionIterator(boolean bounded) {
        this.bounded = bounded;
        this.timestamp = INIT_TIMESTAMP.getTime();
    }

    public static TransactionIterator bounded() {
        return new TransactionIterator(true);
    }

    public static TransactionIterator unbounded() {
        return new TransactionIterator(false);
    }

    @Override
    public boolean hasNext() {
        if (index < data.size()) {
            return true;
        } else if (!bounded) {
            index = 0;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Transaction next() {
        Transaction transaction = data.get(index++);
        transaction.setTimestamp(timestamp);
        timestamp += SIX_MINUTES;
        return transaction;
    }

    private static List<Transaction> data =
            Arrays.asList(
                    new Transaction(1, 0L, 188.23),
                    new Transaction(2, 0L, 374.79),
                    new Transaction(3, 0L, 112.15),
                    new Transaction(4, 0L, 478.75),
                    new Transaction(5, 0L, 208.85),
                    new Transaction(1, 0L, 379.64),
                    new Transaction(2, 0L, 351.44),
                    new Transaction(3, 0L, 320.75),
                    new Transaction(4, 0L, 259.42),
                    new Transaction(5, 0L, 273.44),
                    new Transaction(1, 0L, 267.25),
                    new Transaction(2, 0L, 397.15),
                    new Transaction(3, 0L, 0.219),
                    new Transaction(4, 0L, 231.94),
                    new Transaction(5, 0L, 384.73),
                    new Transaction(1, 0L, 419.62),
                    new Transaction(2, 0L, 412.91),
                    new Transaction(3, 0L, 0.77),
                    new Transaction(4, 0L, 22.10),
                    new Transaction(5, 0L, 377.54),
                    new Transaction(1, 0L, 375.44),
                    new Transaction(2, 0L, 230.18),
                    new Transaction(3, 0L, 0.80),
                    new Transaction(4, 0L, 350.89),
                    new Transaction(5, 0L, 127.55),
                    new Transaction(1, 0L, 483.91),
                    new Transaction(2, 0L, 228.22),
                    new Transaction(3, 0L, 871.15),
                    new Transaction(4, 0L, 64.19),
                    new Transaction(5, 0L, 79.43),
                    new Transaction(1, 0L, 56.12),
                    new Transaction(2, 0L, 256.48),
                    new Transaction(3, 0L, 148.16),
                    new Transaction(4, 0L, 199.95),
                    new Transaction(5, 0L, 252.37),
                    new Transaction(1, 0L, 274.73),
                    new Transaction(2, 0L, 473.54),
                    new Transaction(3, 0L, 119.92),
                    new Transaction(4, 0L, 323.59),
                    new Transaction(5, 0L, 353.16),
                    new Transaction(1, 0L, 211.90),
                    new Transaction(2, 0L, 280.93),
                    new Transaction(3, 0L, 347.89),
                    new Transaction(4, 0L, 459.86),
                    new Transaction(5, 0L, 82.31),
                    new Transaction(1, 0L, 373.26),
                    new Transaction(2, 0L, 479.83),
                    new Transaction(3, 0L, 454.25),
                    new Transaction(4, 0L, 83.64),
                    new Transaction(5, 0L, 292.44));
}
