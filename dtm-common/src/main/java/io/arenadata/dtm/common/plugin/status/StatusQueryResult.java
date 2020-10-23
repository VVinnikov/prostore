package io.arenadata.dtm.common.plugin.status;

import io.arenadata.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import lombok.Data;

@Data
public class StatusQueryResult {
    private KafkaPartitionInfo partitionInfo;
}
