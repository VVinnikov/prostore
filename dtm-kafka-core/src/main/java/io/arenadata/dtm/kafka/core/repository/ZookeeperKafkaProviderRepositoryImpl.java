package io.arenadata.dtm.kafka.core.repository;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProviderImpl;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component("mapZkKafkaProviderRepository")
public class ZookeeperKafkaProviderRepositoryImpl implements ZookeeperKafkaProviderRepository {

    private final Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap = new ConcurrentHashMap<>();
    private final Vertx vertx;

    @Autowired
    public ZookeeperKafkaProviderRepositoryImpl(@Qualifier("coreVertx") Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public KafkaZookeeperConnectionProvider getOrCreate(String connectionString) {
        final KafkaZookeeperConnectionProvider zkConnProvider = zkConnProviderMap.get(connectionString);
        final KafkaZookeeperProperties zookeeperProperties = new KafkaZookeeperProperties();
        zookeeperProperties.setConnectionString(connectionString);
        if (zkConnProvider == null) {
            zkConnProviderMap.put(zookeeperProperties.getConnectionString(),
                    new KafkaZookeeperConnectionProviderImpl(this.vertx, zookeeperProperties));
        }
        return zkConnProviderMap.get(zookeeperProperties.getConnectionString());
    }
}
