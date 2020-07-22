package ru.ibs.dtm.common.plugin.status.kafka;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class KafkaPartitionInfo {
    private String consumerGroup;
    private String topic;
    private int partition;
    private Long start;
    private Long end;
    private Long offset;
    private Long lag;
    private LocalDateTime lastCommitTime;
}
