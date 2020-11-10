package io.arenadata.dtm.kafka.core.repository;

import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;

public interface ZookeeperKafkaProviderRepository {

    KafkaZookeeperConnectionProvider getOrCreate(String connectionString);
}
