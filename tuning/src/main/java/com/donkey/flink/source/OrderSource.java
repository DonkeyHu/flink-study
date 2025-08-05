package com.donkey.flink.source;

import com.donkey.flink.entity.OrderPOJO;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class OrderSource implements SourceFunction<OrderPOJO> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private final String[] userIds = {"Alice", "Bob", "Charlie", "David"};

    @Override
    public void run(SourceContext<OrderPOJO> ctx) throws Exception {
        long eventTime = System.currentTimeMillis();
        while (isRunning) {
            String userId = userIds[random.nextInt(userIds.length)];
            double amount = Math.round(random.nextDouble() * 100 * 100) / 100.0;
            eventTime += random.nextInt(2000) + 500;
            OrderPOJO order = new OrderPOJO(userId, amount, eventTime);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(order);
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
