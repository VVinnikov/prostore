package ru.ibs.dtm.common.status.kafka;

import lombok.Data;

@Data
public class StatusRequest {
    private String topic;
    private String consumerGroup;

    public StatusRequest(String topic, String consumerGroup) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    public StatusRequest() {}
}
