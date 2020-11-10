package io.arenadata.dtm.query.execution.core.dao.zookeeper;

import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;

public interface ZookeeperKafkaProviderRepository {

    KafkaZookeeperConnectionProvider getOrCreate(String connectionString);
}
