package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.vertx.core.Future;

import java.util.List;

/**
 * Kafka topic service management
 */
public interface KafkaTopicCreatorService {

    Future<Void> create(List<String> topics);

    Future<Void> delete(List<String> topics);

    Future<Void> createOrReplace(List<String> topics);
}
