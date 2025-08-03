package com.donkey.flink.entity;

public class Gift {
    public String name;    // 礼物名称
    public int value;      // 礼物价值
    public long timestamp; // 事件发生的时间戳

    public Gift() {}

    public Gift(String name, int value, long timestamp) {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Gift{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
