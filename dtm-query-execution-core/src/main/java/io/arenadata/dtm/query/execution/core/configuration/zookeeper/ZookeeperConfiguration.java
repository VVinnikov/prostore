package io.arenadata.dtm.query.execution.core.configuration.zookeeper;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.configuration.properties.ServiceDbZookeeperProperties;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Configuration
public class ZookeeperConfiguration {

    @Bean("serviceDbZkConnectionProvider")
    public ZookeeperConnectionProvider serviceDbZkConnectionManager(ServiceDbZookeeperProperties properties,
                                                                    @Value("${core.env.name}") String envName) {
        return new ZookeeperConnectionProviderImpl(properties, envName);
    }

    @Bean("coreKafkaZkConnProviderMap")
    public Map<String, KafkaZookeeperConnectionProvider> kafkaZkConnectionProviderMap(KafkaZookeeperProperties zookeeperProperties) {
        final ConcurrentHashMap<String, KafkaZookeeperConnectionProvider> kafkaZkConnProviderMap = new ConcurrentHashMap<>();
        kafkaZkConnProviderMap.put(zookeeperProperties.getConnectionString(),
                new KafkaZookeeperConnectionProviderImpl(zookeeperProperties));
        return kafkaZkConnProviderMap;
    }

    @Bean
    public ZookeeperExecutor zookeeperExecutor(ZookeeperConnectionProvider connectionManager, Vertx vertx) {
        return new ZookeeperExecutorImpl(connectionManager, vertx);
    }


}
