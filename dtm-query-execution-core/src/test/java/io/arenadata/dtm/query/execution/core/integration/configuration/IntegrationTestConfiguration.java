package io.arenadata.dtm.query.execution.core.integration.configuration;

import io.arenadata.dtm.query.execution.core.configuration.properties.ServiceDbZookeeperProperties;
import io.arenadata.dtm.query.execution.core.integration.AbstractCoreDtmIntegrationTest;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.Objects;

@TestConfiguration
@Slf4j
public class IntegrationTestConfiguration {

    @Bean(destroyMethod = "close", name = "itTestVertx")
    public Vertx vertx() {
        System.setProperty("org.vertx.logger-delegate-factory-class-name",
                "org.vertx.java.core.logging.impl.SLF4JLogDelegateFactory");
        return Vertx.vertx();
    }

    @Bean("itTestZkProvider")
    public ZookeeperConnectionProvider zkConnectionProvider() {
        final ServiceDbZookeeperProperties zookeeperProperties = new ServiceDbZookeeperProperties();
        zookeeperProperties.setConnectionString(AbstractCoreDtmIntegrationTest.getZkDsConnectionString());
        zookeeperProperties.setChroot(
                Objects.requireNonNull(AbstractCoreDtmIntegrationTest.dtmProperties
                        .getProperty("core.datasource.zookeeper.chroot")).toString());
        return new ZookeeperConnectionProviderImpl(zookeeperProperties,
                Objects.requireNonNull(AbstractCoreDtmIntegrationTest.dtmProperties
                        .getProperty("core.env.name")).toString());
    }

    @Bean("itTestZkExecutor")
    public ZookeeperExecutor zkExecutor(@Qualifier("itTestZkProvider") ZookeeperConnectionProvider zookeeperConnectionProvider) {
        return new ZookeeperExecutorImpl(zookeeperConnectionProvider, vertx());
    }
}
