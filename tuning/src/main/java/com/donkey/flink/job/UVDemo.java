package com.donkey.flink.job;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class UVDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setStateBackend(new HashMapStateBackend());

        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig ck = env.getCheckpointConfig();
        ck.setCheckpointStorage("C:\\Users\\donkey\\Documents\\Github\\donkey\\checkpoint\\UVDemo");
        ck.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
        ck.setCheckpointTimeout(TimeUnit.SECONDS.toMillis(10));
        ck.setMaxConcurrentCheckpoints(1);
        ck.setTolerableCheckpointFailureNumber(5);
        ck.enableUnalignedCheckpoints();
    }

}
