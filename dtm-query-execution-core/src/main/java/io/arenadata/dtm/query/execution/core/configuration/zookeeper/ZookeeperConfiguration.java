package io.arenadata.dtm.query.execution.core.configuration.zookeeper;

import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ZookeeperConfiguration {


    @Bean
    public ZookeeperExecutor zookeeperExecutor(ZookeeperConnectionProvider connectionManager, Vertx vertx) {
        return new ZookeeperExecutorImpl(connectionManager, vertx);
    }


}
