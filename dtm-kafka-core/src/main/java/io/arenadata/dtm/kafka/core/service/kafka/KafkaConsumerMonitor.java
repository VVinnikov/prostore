package io.arenadata.dtm.kafka.core.service.kafka;


import io.arenadata.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import io.vertx.core.Future;

public interface KafkaConsumerMonitor {
    Future<KafkaPartitionInfo> getAggregateGroupConsumerInfo(String consumerGroup, String topic);
}
