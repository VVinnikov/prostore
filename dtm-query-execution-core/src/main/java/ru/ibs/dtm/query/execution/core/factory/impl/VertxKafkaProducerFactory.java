package ru.ibs.dtm.query.execution.core.factory.impl;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import ru.ibs.dtm.query.execution.core.factory.KafkaProducerFactory;

import java.util.Map;

public class VertxKafkaProducerFactory<T, S> implements KafkaProducerFactory<T, S> {

  private final Vertx vertx;
  private final Map<String, String> defaultProps;

  public VertxKafkaProducerFactory(Vertx vertx, Map<String, String> defaultProps) {
    this.vertx = vertx;
    this.defaultProps = defaultProps;
  }

  @Override
  public KafkaProducer<T, S> create(Map<String, String> config) {
    defaultProps.forEach(config::putIfAbsent);
    return KafkaProducer.create(vertx, defaultProps);
  }
}
