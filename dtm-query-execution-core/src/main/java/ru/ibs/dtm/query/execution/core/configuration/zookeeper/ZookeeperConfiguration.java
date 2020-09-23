package ru.ibs.dtm.query.execution.core.configuration.zookeeper;

import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.core.configuration.properties.ZookeeperProperties;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZKConnectionProvider;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import ru.ibs.dtm.query.execution.core.service.zookeeper.impl.ZKConnectionProviderImpl;
import ru.ibs.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;

@Slf4j
@Configuration
public class ZookeeperConfiguration {

    @Bean
    public ZKConnectionProvider zkConnectionManager(ZookeeperProperties properties) {
        return new ZKConnectionProviderImpl(properties);
    }

    @Bean
    public ZookeeperExecutor zookeeperExecutor(ZKConnectionProvider connectionManager, Vertx vertx) {
        return new ZookeeperExecutorImpl(connectionManager, vertx);
    }

}
