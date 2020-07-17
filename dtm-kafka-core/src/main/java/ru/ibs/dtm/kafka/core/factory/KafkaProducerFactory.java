package ru.ibs.dtm.kafka.core.factory;

import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.Map;

public interface KafkaProducerFactory<T, S> {
    KafkaProducer<T, S> create(Map<String, String> config);
}
