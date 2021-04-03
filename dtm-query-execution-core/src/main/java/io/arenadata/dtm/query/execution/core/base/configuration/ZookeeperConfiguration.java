package io.arenadata.dtm.query.execution.core.base.configuration;

import io.arenadata.dtm.query.execution.core.base.configuration.properties.ServiceDbZookeeperProperties;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.impl.ZookeeperExecutorImpl;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ZookeeperConfiguration {

    @Bean("serviceDbZkConnectionProvider")
    public ZookeeperConnectionProvider serviceDbZkConnectionManager(ServiceDbZookeeperProperties properties,
                                                                    @Value("${core.env.name}") String envName) {
        return new ZookeeperConnectionProviderImpl(properties, envName);
    }

    @Bean
    public ZookeeperExecutor zookeeperExecutor(ZookeeperConnectionProvider connectionManager, Vertx vertx) {
        return new ZookeeperExecutorImpl(connectionManager, vertx);
    }


}
