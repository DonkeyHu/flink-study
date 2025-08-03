package com.donkey.flink.source;

import com.donkey.flink.entity.Gift;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GiftSource implements SourceFunction<Gift> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private final String[] giftNames = {"火箭", "飞机", "跑车", "鲜花"};
    private final int[] giftValues = {1000, 500, 200, 1};

    @Override
    public void run(SourceContext<Gift> ctx) throws Exception {
        while (isRunning) {
            int index = random.nextInt(giftNames.length);
            String name = giftNames[index];
            int value = giftValues[index];
            long currentTime = System.currentTimeMillis();
            Gift gift = new Gift(name, value, currentTime);
            ctx.collect(gift);
            TimeUnit.MILLISECONDS.sleep(random.nextInt(1500) + 100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
