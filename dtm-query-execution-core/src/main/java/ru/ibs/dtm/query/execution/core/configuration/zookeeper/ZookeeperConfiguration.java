package ru.ibs.dtm.query.execution.core.configuration.zookeeper;

import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.core.configuration.properties.ZookeeperProperties;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import ru.ibs.dtm.query.execution.core.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import ru.ibs.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;

@Slf4j
@Configuration
public class ZookeeperConfiguration {

    @Bean
    public ZookeeperConnectionProvider zkConnectionManager(ZookeeperProperties properties) {
        return new ZookeeperConnectionProviderImpl(properties);
    }

    @Bean
    public ZookeeperExecutor zookeeperExecutor(ZookeeperConnectionProvider connectionManager, Vertx vertx) {
        return new ZookeeperExecutorImpl(connectionManager, vertx);
    }

}
