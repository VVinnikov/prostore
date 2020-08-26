package ru.ibs.dtm.kafka.core.service.kafka;


import io.vertx.core.Future;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaGroupTopic;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;

import java.util.List;
import java.util.Map;

public interface KafkaConsumerMonitor {
    Future<KafkaPartitionInfo> getAggregateGroupConsumerInfo(String consumerGroup, String topic);
}
