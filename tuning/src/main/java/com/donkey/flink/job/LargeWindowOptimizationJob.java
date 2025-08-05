package com.donkey.flink.job;


import com.donkey.flink.entity.OrderPOJO;
import com.donkey.flink.function.DailyAggregator;
import com.donkey.flink.function.OrderPreAggregator;
import com.donkey.flink.source.OrderSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink大窗口应该如何做优化
 */
public class LargeWindowOptimizationJob {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.disableOperatorChaining();

        DataStream<OrderPOJO> orderStream = env.addSource(new OrderSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderPOJO>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((order, ts) -> order.timestamp)
                );

        DataStream<Tuple2<String, Double>> preAggregatedStream = orderStream.keyBy(x -> x.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .aggregate(new OrderPreAggregator(), new WindowFunction<Double, Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Double> aggregates, Collector<Tuple2<String, Double>> out) throws Exception {
                        // 从 aggregates 中获取唯一的预聚合结果
                        Double partialSum = aggregates.iterator().next();
                        // 将 key (userId) 和预聚合结果包装成 Tuple2 并输出
                        out.collect(new Tuple2<>(key, partialSum));
                    }
                });

        preAggregatedStream.print("DEBUG-预聚合结果");

        SingleOutputStreamOperator<String> dailyResultStream = preAggregatedStream.keyBy(x -> x.f0)
                .process(new DailyAggregator());

        dailyResultStream.print("每日结算结果");

        env.execute("Large Window Optimization Demo");
    }
}
