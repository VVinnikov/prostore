package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;
import java.util.Set;

/**
 * Сервис управления топиками Kafka
 */
public interface KafkaTopicService {
  void create(List<String> topics, Handler<AsyncResult<Void>> handler);
  void delete(List<String> topics, Handler<AsyncResult<Void>> handler);
  void topics(Handler<AsyncResult<Set<String>>> handler);
  void createOrReplace(List<String> topics, Handler<AsyncResult<Void>> handler);
}
