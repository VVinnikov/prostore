package ru.ibs.dtm.common.plugin.status;

import lombok.Data;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;

@Data
public class StatusQueryResult {
    private KafkaPartitionInfo partitionInfo;
}
