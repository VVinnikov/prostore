package ru.ibs.dtm.query.execution.core.factory;

import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.Map;

public interface KafkaConsumerFactory<T, S> {
  KafkaConsumer<T, S> create(Map<String, String> config);
}