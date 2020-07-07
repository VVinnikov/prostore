package ru.ibs.dtm.query.execution.core.factory.impl;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import ru.ibs.dtm.query.execution.core.factory.KafkaConsumerFactory;

import java.util.Map;

public class VertxKafkaConsumerFactory<T, S> implements KafkaConsumerFactory<T, S> {

  private final Vertx vertx;
  private final Map<String, String> defaultProps;

  public VertxKafkaConsumerFactory(Vertx vertx, Map<String, String> defaultProps) {
    this.vertx = vertx;
    this.defaultProps = defaultProps;
  }

  @Override
  public KafkaConsumer<T, S> create(Map<String, String> config) {
    defaultProps.forEach(config::putIfAbsent);
    return KafkaConsumer.create(vertx, defaultProps);
  }
}
