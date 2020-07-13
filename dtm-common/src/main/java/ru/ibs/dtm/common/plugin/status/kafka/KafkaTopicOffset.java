package ru.ibs.dtm.common.plugin.status.kafka;

import lombok.Data;

@Data
public class KafkaTopicOffset {
    private Long start;
    private Long end;
}
