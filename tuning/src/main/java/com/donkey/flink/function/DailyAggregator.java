package com.donkey.flink.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class DailyAggregator extends KeyedProcessFunction<String, Tuple2<String, Double>, String> {

    // 状态，用于存储每个用户每天的累计金额
    private transient ValueState<Double> dailyAmountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("daily-amount", Double.class);
        dailyAmountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Tuple2<String, Double> value, Context ctx, Collector<String> out) throws Exception {
        String userId = value.f0;
        Double partialAmount = value.f1;

        Double currentAmount = dailyAmountState.value();
        double newDailyAmount = currentAmount == null ? 0.0 : currentAmount + partialAmount;
        dailyAmountState.update(newDailyAmount);

        if (currentAmount == null) {
            ZonedDateTime zonedDateTime = Instant.ofEpochMilli(ctx.timestamp()).atZone(ZoneId.systemDefault());
            long nextDayStartMillis = ctx.timestamp() + 5 * 60 * 1000;
            System.out.println("----register-----> " + nextDayStartMillis);
            ctx.timerService().registerEventTimeTimer(nextDayStartMillis);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        String userId = ctx.getCurrentKey();
        Double finalAmount = dailyAmountState.value();
        System.out.println("开始触发每十分钟结算总金额...");
        String result = String.format(
                "--- 每10分钟结算 [%s] ---\n" +
                        "用户: %s\n" +
                        "总金额: %.2f\n",
                Instant.ofEpochMilli(timestamp - 1),
                userId,
                finalAmount
        );
        out.collect(result);
        dailyAmountState.clear();
    }
}
