package com.donkey.flink.sink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LoggerOutputFormat implements OutputFormat<String> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(LoggerOutputFormat.class);

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void writeRecord(String record) throws IOException {
        LOG.info(record);
    }

    @Override
    public void close() throws IOException {

    }
}
