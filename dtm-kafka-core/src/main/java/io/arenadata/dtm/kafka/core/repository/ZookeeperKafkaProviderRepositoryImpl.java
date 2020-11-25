package io.arenadata.dtm.kafka.core.repository;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProviderImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@Component("mapZkKafkaProviderRepository")
public class ZookeeperKafkaProviderRepositoryImpl implements ZookeeperKafkaProviderRepository {
    private final KafkaZookeeperProperties defaultProperties;
    private final Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap = new ConcurrentHashMap<>();

    @Override
    public KafkaZookeeperConnectionProvider getOrCreate(String connectionString) {
        final KafkaZookeeperConnectionProvider zkConnProvider = zkConnProviderMap.get(connectionString);
        final KafkaZookeeperProperties zookeeperProperties = new KafkaZookeeperProperties();
        zookeeperProperties.setConnectionString(connectionString);
        zookeeperProperties.setChroot(defaultProperties.getChroot());
        if (zkConnProvider == null) {
            zkConnProviderMap.put(zookeeperProperties.getConnectionString(),
                new KafkaZookeeperConnectionProviderImpl(zookeeperProperties));
        }
        return zkConnProviderMap.get(zookeeperProperties.getConnectionString());
    }
}
