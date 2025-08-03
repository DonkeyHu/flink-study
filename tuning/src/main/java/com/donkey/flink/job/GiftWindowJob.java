package com.donkey.flink.job;

import com.donkey.flink.entity.Gift;
import com.donkey.flink.source.GiftSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink滑动窗口计算的完整示例
 * 场景：实时统计直播间在过去10秒内，收到的不同类型礼物的总价值，每5秒钟更新一次。
 */
public class GiftWindowJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStream<Gift> giftStream = env.addSource(new GiftSource());
        SingleOutputStreamOperator<Gift> watermarks = giftStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Gift>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                        .withIdleness(Duration.ofSeconds(10))

        );

        SingleOutputStreamOperator<Integer> process = watermarks.keyBy(gift -> gift.name)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessWindowFunction<Gift, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Gift> elements, Collector<Integer> out) throws Exception {
                        int totalValue = 0;
                        int count = 0;

                        for (Gift element : elements) {
                            totalValue += element.value;
                            count++;
                        }

                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        // 格式化输出字符串
                        String result = String.format(
                                "--- 窗口 [%tT - %tT] ---\n" +
                                        "礼物: %s\n" +
                                        "数量: %d\n" +
                                        "总价值: %d\n",
                                start, end, s, count, totalValue
                        );
                        System.out.println(result);
                        out.collect(totalValue);
                    }
                });

        process.keyBy(x -> "global").sum(0).print();


//                .process(new ProcessWindowFunction<Gift, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<Gift> elements, Collector<String> out) throws Exception {
//                        int totalValue = 0;
//                        int count = 0;
//
//                        for (Gift element : elements) {
//                            totalValue += element.value;
//                            count++;
//                        }
//
//                        long start = context.window().getStart();
//                        long end = context.window().getEnd();
//
//                        // 格式化输出字符串
//                        String result = String.format(
//                                "--- 窗口 [%tT - %tT] ---\n" +
//                                        "礼物: %s\n" +
//                                        "数量: %d\n" +
//                                        "总价值: %d\n",
//                                start, end, s, count, totalValue
//                        );
//                        System.out.println(result);
//                        out.collect(result);
//                    }
//                });
//
//        resultStream.print();

        env.execute("Live Stream Gift Sliding Window Analysis");
    }
}
