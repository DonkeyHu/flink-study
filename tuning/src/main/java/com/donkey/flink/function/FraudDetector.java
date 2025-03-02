package com.donkey.flink.function;

import com.donkey.flink.entity.Alert;
import com.donkey.flink.entity.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("smallAmountFlag", Types.BOOLEAN);
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Transaction value, Context ctx, Collector<Alert> out) throws Exception {
        Boolean smallAmountFlag = valueState.value();

        if (smallAmountFlag != null) {
            if (value.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(value.getAccountId());
                out.collect(alert);
            }
            valueState.clear();
        }

        if (value.getAmount() < SMALL_AMOUNT) {
            valueState.update(true);
        }

    }
}
