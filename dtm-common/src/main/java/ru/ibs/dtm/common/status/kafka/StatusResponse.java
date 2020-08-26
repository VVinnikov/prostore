package ru.ibs.dtm.common.status.kafka;

import lombok.Data;

@Data
public class StatusResponse {
    private String topic;
    private String consumerGroup;
    private long consumerOffset;
    private long producerOffset;
    private long lastCommitTime;
}
