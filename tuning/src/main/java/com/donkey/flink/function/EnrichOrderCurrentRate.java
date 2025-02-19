package com.donkey.flink.function;

import com.donkey.flink.entity.CurrencyRate;
import com.donkey.flink.entity.CurrentRateOrder;
import com.donkey.flink.entity.Order;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class EnrichOrderCurrentRate extends ProcessJoinFunction<Order, CurrencyRate, CurrentRateOrder> {
    @Override
    public void processElement(Order left, CurrencyRate right, Context ctx, Collector<CurrentRateOrder> out) throws Exception {
        CurrentRateOrder res = new CurrentRateOrder(
                left.getOrderId(),
                left.getCurrencyCode(),
                left.getAmount(),
                left.getOrderTimestamp(),
                right.getUsRate(),
                right.getRateTimestamp(),
                left.getAmount() * right.getUsRate()
        );
        out.collect(res);
    }
}
