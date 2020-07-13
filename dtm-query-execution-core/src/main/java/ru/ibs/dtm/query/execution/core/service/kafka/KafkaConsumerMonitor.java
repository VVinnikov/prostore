package ru.ibs.dtm.query.execution.core.service.kafka;


import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;

import java.util.List;

public interface KafkaConsumerMonitor {

    List<KafkaPartitionInfo> getGroupConsumerInfo();
}
