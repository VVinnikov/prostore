package ru.ibs.dtm.common.plugin.status.kafka;

import lombok.Data;

@Data
public class KafkaTopicCommitedOffset {
    private Long offset;
    private Long lastCommitTimestamp;
}
