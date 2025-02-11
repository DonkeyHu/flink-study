package com.donkey.flink.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class MockSourceFunction implements ParallelSourceFunction<String> {



    @Override
    public void run(SourceContext<String> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }


}
