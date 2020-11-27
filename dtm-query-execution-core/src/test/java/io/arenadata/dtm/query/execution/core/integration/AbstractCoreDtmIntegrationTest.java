package io.arenadata.dtm.query.execution.core.integration;

import com.github.dockerjava.api.model.Bind;
import io.arenadata.dtm.query.execution.core.integration.configuration.IntegrationTestConfiguration;
import io.arenadata.dtm.query.execution.core.integration.factory.PropertyFactory;
import io.arenadata.dtm.query.execution.core.integration.query.client.SqlClientFactoryImpl;
import io.arenadata.dtm.query.execution.core.integration.query.client.SqlClientProviderImpl;
import io.arenadata.dtm.query.execution.core.integration.query.executor.QueryExecutorImpl;
import io.arenadata.dtm.query.execution.core.integration.util.DockerImagesUtil;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.service.zookeeper.impl.ZookeeperExecutorImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.PropertySource;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Objects;
import java.util.stream.Stream;

@Testcontainers
@ActiveProfiles("it_test")
@SpringBootTest(classes = {IntegrationTestConfiguration.class,
        ZookeeperExecutor.class,
        ZookeeperConnectionProvider.class,
        QueryExecutorImpl.class,
        SqlClientProviderImpl.class,
        SqlClientFactoryImpl.class})
@Slf4j
public abstract class AbstractCoreDtmIntegrationTest {

    private static final Network network = Network.SHARED;
    public static final GenericContainer<?> zkDsContainer;
    public static final GenericContainer<?> zkKafkaContainer;
    public static final KafkaContainer kafkaContainer;
    public static final GenericContainer<?> adqmContainer;
    public static final GenericContainer<?> dtmCoreContainer;
    public static final int ZK_PORT = 2181;
    public static final PropertySource<?> dtmProperties;

    static {
        dtmProperties = PropertyFactory.createPropertySource("application-it_test.yml");
        zkDsContainer = createZkDsContainer();
        zkKafkaContainer = createZkKafkaContainer();
        kafkaContainer = createKafkaContainer();
        adqmContainer = createAdqmContainer();
        dtmCoreContainer = createDtmCoreContainer();
        Stream.of(
                zkDsContainer,
                zkKafkaContainer,
                kafkaContainer,
                adqmContainer,
                dtmCoreContainer
        ).forEach(GenericContainer::start);
    }

    private static GenericContainer<?> createZkDsContainer() {
        return new GenericContainer<>(DockerImagesUtil.ZOOKEEPER)
                .withNetwork(network)
                .withExposedPorts(ZK_PORT)
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("core.datasource.zookeeper.connection-string")).toString())
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZK_PORT));
    }

    private static GenericContainer<?> createZkKafkaContainer() {
        return new GenericContainer<>(DockerImagesUtil.ZOOKEEPER)
                .withNetwork(network)
                .withExposedPorts(ZK_PORT)
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("core.kafka.cluster.zookeeper.connection-string")).toString())
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZK_PORT));
    }

    private static KafkaContainer createKafkaContainer() {
        return new KafkaContainer(DockerImageName.parse(DockerImagesUtil.KAFKA))
                .withNetwork(network)
                .withExternalZookeeper(Objects.requireNonNull(
                        dtmProperties.getProperty("core.kafka.cluster.zookeeper.connection-string")).toString());
    }

    private static GenericContainer<?> createAdqmContainer() {
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.ADQM))
                .withNetwork(network)
                .withExposedPorts(Integer.valueOf(Objects.requireNonNull(
                        dtmProperties.getProperty("adqm.datasource.hosts")).toString().split(":")[1]))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withBinds(Bind.parse("/home/viktor/arenadata/projects/dtm/dtm-query-execution-core/src/test/resources/config/adqm/config.xml:/etc/clickhouse-server/config.xml")))
                .withNetworkAliases(Objects.requireNonNull(
                        dtmProperties.getProperty("adqm.datasource.hosts")).toString().split(":")[0]);
    }

    private static GenericContainer<?> createDtmCoreContainer() {
        ToStringConsumer toStringConsumer = new ToStringConsumer();
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.DTM_CORE))
                .withLogConsumer(toStringConsumer)
                .withExposedPorts((Integer) dtmProperties.getProperty("core.http.port"),
                        (Integer) dtmProperties.getProperty("management.server.port"))
                .withNetworkAliases("test-it-dtm-core")
                .withNetwork(network)
                .withEnv("DTM_CORE_PORT", Objects.requireNonNull(dtmProperties.getProperty("core.http.port")).toString())
                .withEnv("DTM_METRICS_PORT", Objects.requireNonNull(dtmProperties.getProperty("management.server.port")).toString())
                .withEnv("DTM_NAME", Objects.requireNonNull(dtmProperties.getProperty("core.env.name")).toString())
                .withEnv("CORE_PLUGINS_ACTIVE", Objects.requireNonNull(dtmProperties.getProperty("core.plugins.active")).toString())
                .withEnv("EDML_DATASOURCE", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.sourceType")).toString())
                .withEnv("EDML_DEFAULT_CHUNK_SIZE", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.defaultChunkSize")).toString())
                .withEnv("EDML_DEFAULT_MESSAGE_LIMIT", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.defaultMessageLimit")).toString())
                .withEnv("EDML_STATUS_CHECK_PERIOD_MS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.pluginStatusCheckPeriodMs")).toString())
                .withEnv("EDML_FIRST_OFFSET_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.firstOffsetTimeoutMs")).toString())
                .withEnv("EDML_CHANGE_OFFSET_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.edml.changeOffsetTimeoutMs")).toString())
                .withEnv("ZOOKEEPER_DS_CONNECTION_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.zookeeper.connection-timeout-ms")).toString())
                .withEnv("ZOOKEEPER_DS_ADDRESS", Objects.requireNonNull(dtmProperties.getProperty("core.datasource.zookeeper.connection-string")).toString())
                .withEnv("ZOOKEEPER_KAFKA_ADDRESS", Objects.requireNonNull(dtmProperties.getProperty("core.kafka.cluster.zookeeper.connection-string")).toString())
                .withEnv("KAFKA_INPUT_STREAM_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("core.kafka.admin.inputStreamTimeoutMs")).toString())
                .withEnv("STATUS_MONITOR_URL", Objects.requireNonNull(dtmProperties.getProperty("core.kafka.statusMonitorUrl")).toString())
                .withEnv("ADB_DB_NAME", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.database")).toString())
                .withEnv("ADB_USERNAME", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.user")).toString())
                .withEnv("ADB_PASS", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.password")).toString())
                .withEnv("ADB_HOST", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.host")).toString())
                .withEnv("ADB_PORT", Objects.requireNonNull(dtmProperties.getProperty("adb.datasource.options.port")).toString())
                .withEnv("ADB_START_LOAD_URL", Objects.requireNonNull(dtmProperties.getProperty("adb.mppw.startLoadUrl")).toString())
                .withEnv("ADB_MPPW_POOL_SIZE", Objects.requireNonNull(dtmProperties.getProperty("adb.mppw.poolSize")).toString())
                .withEnv("ADB_LOAD_GROUP", Objects.requireNonNull(dtmProperties.getProperty("adb.mppw.consumerGroup")).toString())
                .withEnv("ADB_MPPW_STOP_TIMEOUT_MS", Objects.requireNonNull(dtmProperties.getProperty("adb.mppw.stopTimeoutMs")).toString())
                .withEnv("TARANTOOL_DB_HOST", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.host")).toString())
                .withEnv("TARANTOOL_DB_PORT", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.port")).toString())
                .withEnv("TARANTOOL_DB_USER", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.user")).toString())
                .withEnv("TARANTOOL_DB_PASS", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.password")).toString())
                .withEnv("TARANTOOL_DB_OPER_TIMEOUT", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.operationTimeout")).toString())
                .withEnv("TARANTOOL_DB_RETRY_COUNT", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.db.retryCount")).toString())
                .withEnv("TARANTOOL_CATRIDGE_URL", Objects.requireNonNull(dtmProperties.getProperty("adg.tarantool.cartridge.url")).toString())
                .withEnv("ADQM_DB_NAME", Objects.requireNonNull(dtmProperties.getProperty("adqm.datasource.database")).toString())
                .withEnv("ADQM_USERNAME", Objects.requireNonNull(dtmProperties.getProperty("adqm.datasource.user")).toString())
                .withEnv("ADQM_HOSTS", Objects.requireNonNull(dtmProperties.getProperty("adqm.datasource.hosts")).toString())
                .withEnv("ADQM_CLUSTER", Objects.requireNonNull(dtmProperties.getProperty("adqm.ddl.cluster")).toString())
                .withEnv("ADQM_TTL_SEC", Objects.requireNonNull(dtmProperties.getProperty("adqm.ddl.ttlSec")).toString())
                .withEnv("ADQM_ARCHIVE_DISK", Objects.requireNonNull(dtmProperties.getProperty("adqm.ddl.archiveDisk")).toString())
                .withEnv("ADQM_CONSUMER_GROUP", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.consumerGroup")).toString())
                .withEnv("ADQM_BROKERS", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.kafkaBrokers")).toString())
                .withEnv("ADQM_MPPR_CONNECTOR_HOST", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppr.host")).toString())
                .withEnv("ADQM_MPPR_CONNECTOR_PORT", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppr.port")).toString())
                .withEnv("ADQM_MPPR_CONNECTOR_URL", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppr.url")).toString())
                .withEnv("ADQM_MPPW_LOAD_TYPE", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.loadType")).toString())
                .withEnv("ADQM_REST_LOAD_URL", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.restLoadUrl")).toString())
                .withEnv("ADQM_REST_LOAD_GROUP", Objects.requireNonNull(dtmProperties.getProperty("adqm.mppw.restLoadConsumerGroup")).toString());
    }

    public static String getZkDsConnectionString() {
        return zkDsContainer.getHost() + ":" + zkDsContainer.getMappedPort(ZK_PORT);
    }

    public static String getZkKafkaConnectionString() {
        return zkKafkaContainer.getHost() + ":" + zkKafkaContainer.getMappedPort(ZK_PORT);
    }

    public static String getDtmCoreHostPort() {
        return dtmCoreContainer.getHost() + ":" + dtmCoreContainer.getMappedPort(
                Integer.parseInt(Objects.requireNonNull(dtmProperties.getProperty("core.http.port")).toString()));
    }

    public static String getDtmCoreMetricsHostPort() {
        return dtmCoreContainer.getHost() + ":" + dtmCoreContainer.getMappedPort(
                Integer.parseInt(Objects.requireNonNull(dtmProperties.getProperty("management.server.port")).toString()));
    }

    public static String getJdbcDtmConnectionString() {
        return "jdbc:adtm://" + getDtmCoreHostPort() + "/";
    }

    public static String getEntitiesPath(String datamartMnemonic) {
        return String.format("%s/%s/entity",
                Objects.requireNonNull(dtmProperties.getProperty("core.env.name")).toString(),
                datamartMnemonic);
    }

    public static String getEntityPath(String datamartMnemonic, String entityName) {
        return String.format("/%s/%s/entity/%s",
                Objects.requireNonNull(dtmProperties.getProperty("core.env.name")).toString(),
                datamartMnemonic,
                entityName);
    }



}
