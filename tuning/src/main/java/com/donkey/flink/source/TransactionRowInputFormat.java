package com.donkey.flink.source;

import com.donkey.flink.entity.Transaction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;

public class TransactionRowInputFormat extends GenericInputFormat<Row> implements NonParallelInput {

    private static final long serialVersionUID = 1L;

    private transient Iterator<Transaction> transactions;

    @Override
    public void open(GenericInputSplit split) throws IOException {
        transactions = TransactionIterator.bounded();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !transactions.hasNext();
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        Transaction next = transactions.next();
        reuse.setField(0, next.getAccountId());
        reuse.setField(1, new Timestamp(next.getTimestamp()));
        reuse.setField(2, next.getAmount());
        return reuse;
    }
}
