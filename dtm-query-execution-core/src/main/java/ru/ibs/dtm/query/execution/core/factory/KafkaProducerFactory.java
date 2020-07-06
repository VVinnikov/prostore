package ru.ibs.dtm.query.execution.core.factory;

import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.Map;

public interface KafkaProducerFactory<T, S> {
  KafkaProducer<T, S> create(Map<String, String> config);
}
