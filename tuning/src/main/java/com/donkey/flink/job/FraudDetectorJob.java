package com.donkey.flink.job;

import com.donkey.flink.entity.Alert;
import com.donkey.flink.entity.Transaction;
import com.donkey.flink.function.FraudDetector;
import com.donkey.flink.sink.AlertSink;
import com.donkey.flink.source.TransactionSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class FraudDetectorJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setStateBackend(new HashMapStateBackend());

        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig ck = env.getCheckpointConfig();
        ck.setCheckpointStorage("C:\\Users\\donkey\\Documents\\Github\\donkey\\checkpoint\\FraudDetector");
        ck.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
        ck.setCheckpointTimeout(TimeUnit.SECONDS.toMillis(10));
        ck.setMaxConcurrentCheckpoints(1);
        ck.setTolerableCheckpointFailureNumber(5);
        ck.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        ck.enableUnalignedCheckpoints();


        SingleOutputStreamOperator<Transaction> transactions = env.addSource(new TransactionSource()).name("transactions");

        SingleOutputStreamOperator<Alert> fraudDetector = transactions.keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        DataStreamSink<Alert> sink = fraudDetector.addSink(new AlertSink())
                .name("send-alerts");

        env.execute("Fraud-Detection");
    }

}
