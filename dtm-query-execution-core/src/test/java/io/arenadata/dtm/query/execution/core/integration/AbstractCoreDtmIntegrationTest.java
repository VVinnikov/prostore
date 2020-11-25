package io.arenadata.dtm.query.execution.core.integration;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.execution.core.configuration.cache.CacheProperties;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.query.execution.core.configuration.properties.ServiceDbZookeeperProperties;
import io.arenadata.dtm.query.execution.core.integration.configuration.IntegrationTestConfiguration;
import io.arenadata.dtm.query.execution.core.integration.configuration.IntegrationTestProperties;
import io.arenadata.dtm.query.execution.core.integration.query.client.SqlClientFactoryImpl;
import io.arenadata.dtm.query.execution.core.integration.query.client.SqlClientProviderImpl;
import io.arenadata.dtm.query.execution.core.integration.query.executor.QueryExecutorImpl;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.GreenplumProperties;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolCartridgeProperties;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMpprProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ContextConfiguration(classes = IntegrationTestProperties.class)
@TestPropertySource(locations = "/application-it_test.yml")
@EnableConfigurationProperties(value = {
        EdmlProperties.class,
        ServiceDbZookeeperProperties.class,
        KafkaZookeeperProperties.class,
        EdmlProperties.class,
        ServiceDbZookeeperProperties.class,
        KafkaZookeeperProperties.class,
        KafkaProperties.class,
        CacheProperties.class,
        GreenplumProperties.class,
        MppwProperties.class,
        TarantoolDatabaseProperties.class,
        TarantoolCartridgeProperties.class,
        ClickhouseProperties.class,
        DdlProperties.class,
        AdqmMpprProperties.class,
        AdqmMppwProperties.class
})
@ActiveProfiles("it_test")
@SpringBootTest(classes = {IntegrationTestConfiguration.class,
        QueryExecutorImpl.class,
        SqlClientProviderImpl.class,
        SqlClientFactoryImpl.class})
public abstract class AbstractCoreDtmIntegrationTest {
    @Autowired
    @Qualifier("coreDtm")
    private GenericContainer<?> dtmCoreContainer;
    @Autowired
    private IntegrationTestProperties properties;

    public String getDtmCoreHostPort() {
        return dtmCoreContainer.getHost() + ":" + dtmCoreContainer.getMappedPort(properties.getDtmCorePort());
    }

    public String getDtmCoreMetricsHostPort() {
        return dtmCoreContainer.getHost() + ":" + dtmCoreContainer.getMappedPort(properties.getDtmMetricsPort());
    }

    public String getJdbcDtmConnectionString() {
        return "jdbc:adtm://" + getDtmCoreHostPort() + "/";
    }

}
