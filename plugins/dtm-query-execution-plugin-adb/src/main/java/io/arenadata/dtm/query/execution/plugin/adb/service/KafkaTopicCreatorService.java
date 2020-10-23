package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Сервис управления топиками Kafka
 */
public interface KafkaTopicCreatorService {
  void create(List<String> topics, Handler<AsyncResult<Void>> completionHandler);
  void delete(List<String> topics, Handler<AsyncResult<Void>> completionHandler);
  void createOrReplace(List<String> topics, Handler<AsyncResult<Void>> completionHandler);
}
