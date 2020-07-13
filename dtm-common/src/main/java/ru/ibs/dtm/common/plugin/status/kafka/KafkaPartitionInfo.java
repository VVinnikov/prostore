package ru.ibs.dtm.common.plugin.status.kafka;

import lombok.Data;
import org.apache.kafka.common.protocol.types.Field;

import java.time.LocalDateTime;
import java.util.Date;

@Data
public class KafkaPartitionInfo {
    private String consumerGroup;
    private String topic;
    private int partition;
    private Long start;
    private Long end;
    private Long offset;
    private Long lag;
    private Date lastCommitTime;
}
