package com.donkey.flink.sink;

import com.donkey.flink.entity.Alert;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertSink implements SinkFunction<Alert> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AlertSink.class);

    @Override
    public void invoke(Alert value, Context context) throws Exception {
        LOG.info(value.toString());
    }
}
