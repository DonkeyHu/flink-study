package com.donkey.flink.function;

import com.donkey.flink.entity.OrderPOJO;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OrderPreAggregator implements AggregateFunction<OrderPOJO, Double, Double> {

    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(OrderPOJO value, Double accumulator) {
        return accumulator + value.amount;
    }

    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}
