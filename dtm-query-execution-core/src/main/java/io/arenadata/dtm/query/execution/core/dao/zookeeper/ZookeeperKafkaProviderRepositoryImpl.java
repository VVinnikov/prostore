package io.arenadata.dtm.query.execution.core.dao.zookeeper;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProviderImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ZookeeperKafkaProviderRepositoryImpl implements ZookeeperKafkaProviderRepository {

    private final Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap;

    @Autowired
    public ZookeeperKafkaProviderRepositoryImpl(@Qualifier("coreKafkaZkConnProviderMap")
                                                        Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap) {
        this.zkConnProviderMap = zkConnProviderMap;
    }

    @Override
    public KafkaZookeeperConnectionProvider getOrCreate(String connectionString) {
        final KafkaZookeeperConnectionProvider zkConnProvider = zkConnProviderMap.get(connectionString);
        final KafkaZookeeperProperties zookeeperProperties = new KafkaZookeeperProperties();
        zookeeperProperties.setConnectionString(connectionString);
        if (zkConnProvider == null) {
            zkConnProviderMap.put(zookeeperProperties.getConnectionString(),
                    new KafkaZookeeperConnectionProviderImpl(zookeeperProperties));
        }
        return zkConnProviderMap.get(zookeeperProperties.getConnectionString());
    }
}
