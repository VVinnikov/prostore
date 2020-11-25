package io.arenadata.dtm.query.execution.core.integration.configuration;

import io.arenadata.dtm.query.execution.core.integration.util.DockerImagesUtil;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.*;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.stream.Stream;

@TestConfiguration
@Slf4j
public class IntegrationTestConfiguration {

    private final Network network = Network.SHARED;
    @Autowired
    private IntegrationTestProperties config;

    @PostConstruct
    public void startContainers() throws IOException, InterruptedException {
        Stream.of(zkDsContainer(),
                zkKafkaContainer(),
                kafkaContainer(),
                adqmContainer(),
                //adbContainer()
                dtmCoreContainer()
        )
                .forEach(GenericContainer::start);
        //adbContainer().execInContainer("su -gpadmin gpstart -a", "/adbStartResult.txt");
    }

    @PreDestroy
    public void stop() throws IOException, InterruptedException {
        dtmCoreContainer().stop();
        zkDsContainer().stop();
        zkKafkaContainer().stop();
        kafkaContainer().stop();
        adqmContainer().stop();
        //adbContainer().stop();
    }

    @Bean("zkDs")
    public GenericContainer<?> zkDsContainer() {
        return new GenericContainer<>(DockerImagesUtil.ZOOKEEPER)
                .withNetwork(network)
                .withExposedPorts(config.getZkPort())
                .withNetworkAliases(config.getZkDsProperties().getConnectionString())
                .withEnv("ZOOKEEPER_CLIENT_PORT", config.getZkPort().toString());
    }

    @Bean("zkKafka")
    public GenericContainer<?> zkKafkaContainer() {
        return new GenericContainer<>(DockerImagesUtil.ZOOKEEPER)
                .withNetwork(network)
                .withExposedPorts(config.getZkPort())
                .withNetworkAliases(config.getKafkaZkProperties().getConnectionString())
                .withEnv("ZOOKEEPER_CLIENT_PORT", config.getZkPort().toString());
    }

    @Bean("kafka")
    public KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse(DockerImagesUtil.KAFKA))
                .withNetwork(network)
                .withExternalZookeeper(config.getKafkaZkProperties().getConnectionString());
    }

    @Bean("adqm")
    public GenericContainer<?> adqmContainer() {
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.ADQM))
                //.withExposedPorts(8123)
                .withCommand("-v /config/adqm/config.xml:/etc/clickhouse-server/config.xml")
                .withNetwork(network)
                .withNetworkAliases(config.getAdqmDsProperties().getHosts());
    }

    @Bean("adb")
    public GenericContainer<?> adbContainer() throws IOException, InterruptedException {
        final GenericContainer<?> adbContainer = new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.ADB))
                .withNetwork(network)
                .withExposedPorts(config.getAdbDsProperties().getOptions().getPort())
                .withLogConsumer(new Slf4jLogConsumer(log))
                //.withCommand("--name gpdb-ds --hostname adb.it-test hdlee2u/gpdb-analytics /usr/sbin/sshd -D")
                .withNetworkAliases("localhost");
        //adbContainer.followOutput(new Slf4jLogConsumer(log).withPrefix(adbContainer.getContainerId()));
        return adbContainer;

    }

    @Bean("coreDtm")
    public GenericContainer<?> dtmCoreContainer() {
        ToStringConsumer toStringConsumer = new ToStringConsumer();
        return new GenericContainer<>(DockerImageName.parse(DockerImagesUtil.DTM_CORE))
                .withLogConsumer(toStringConsumer)
                .withExposedPorts(config.getDtmCorePort(), config.getDtmMetricsPort())
                .withNetworkAliases("test-it-dtm-core")
                .withNetwork(network)
                .withEnv("DTM_CORE_PORT", config.getDtmCorePort().toString())
                .withEnv("DTM_METRICS_PORT", config.getDtmMetricsPort().toString())
                .withEnv("DTM_NAME", config.getEnv())
                .withEnv("CORE_PLUGINS_ACTIVE", config.getActivePlugins())
                .withEnv("EDML_DATASOURCE", config.getEdmlProperties().getSourceType().name())
                .withEnv("EDML_DEFAULT_CHUNK_SIZE", config.getEdmlProperties().getDefaultChunkSize().toString())
                .withEnv("EDML_DEFAULT_MESSAGE_LIMIT", config.getEdmlProperties().getDefaultMessageLimit().toString())
                .withEnv("EDML_STATUS_CHECK_PERIOD_MS", config.getEdmlProperties().getPluginStatusCheckPeriodMs().toString())
                .withEnv("EDML_FIRST_OFFSET_TIMEOUT_MS", config.getEdmlProperties().getFirstOffsetTimeoutMs().toString())
                .withEnv("EDML_CHANGE_OFFSET_TIMEOUT_MS", config.getEdmlProperties().getChangeOffsetTimeoutMs().toString())
                .withEnv("ZOOKEEPER_DS_CONNECTION_TIMEOUT_MS", String.valueOf(config.getZkDsProperties().getConnectionTimeoutMs()))
                .withEnv("ZOOKEEPER_DS_ADDRESS", config.getZkDsProperties().getConnectionString())
                .withEnv("ZOOKEEPER_KAFKA_ADDRESS", config.getKafkaZkProperties().getConnectionString())
                .withEnv("KAFKA_INPUT_STREAM_TIMEOUT_MS", config.getKafkaProperties().getAdmin().getInputStreamTimeoutMs().toString())
                //.withEnv("KAFKA_MONITOR_POLLING_MS", "1000")
                .withEnv("STATUS_MONITOR_URL", config.getKafkaProperties().getStatusMonitorUrl())
                .withEnv("ADB_DB_NAME", config.getAdbDsProperties().getOptions().getDatabase())
                .withEnv("ADB_USERNAME", config.getAdbDsProperties().getOptions().getUser())
                .withEnv("ADB_PASS", config.getAdbDsProperties().getOptions().getPassword())
                .withEnv("ADB_HOST", config.getAdbDsProperties().getOptions().getHost())
                .withEnv("ADB_PORT", String.valueOf(config.getAdbDsProperties().getOptions().getPort()))
                .withEnv("ADB_START_LOAD_URL", config.getAdbMppwProperties().getStartLoadUrl())
                .withEnv("ADB_MPPW_POOL_SIZE", config.getAdbMppwProperties().getPoolSize().toString())
                .withEnv("ADB_LOAD_GROUP", config.getAdbMppwProperties().getConsumerGroup())
                .withEnv("ADB_MPPW_STOP_TIMEOUT_MS", config.getAdbMppwProperties().getStopTimeoutMs().toString())
                .withEnv("TARANTOOL_DB_HOST", config.getAdgDsProperties().getHost())
                .withEnv("TARANTOOL_DB_PORT", config.getAdgDsProperties().getPort().toString())
                .withEnv("TARANTOOL_DB_USER", config.getAdgDsProperties().getUser())
                .withEnv("TARANTOOL_DB_PASS", config.getAdgDsProperties().getPassword())
                .withEnv("TARANTOOL_DB_OPER_TIMEOUT", config.getAdgDsProperties().getOperationTimeout().toString())
                .withEnv("TARANTOOL_DB_RETRY_COUNT", config.getAdgDsProperties().getRetryCount().toString())
                .withEnv("TARANTOOL_CATRIDGE_URL", config.getAdgCrtgProperties().getUrl())
                .withEnv("ADQM_DB_NAME", config.getAdqmDsProperties().getDatabase())
                .withEnv("ADQM_USERNAME", config.getAdqmDsProperties().getUser())
                .withEnv("ADQM_HOSTS", config.getAdqmDsProperties().getHosts())
                .withEnv("ADQM_CLUSTER", config.getAdqmDdlProperties().getCluster())
                .withEnv("ADQM_TTL_SEC", config.getAdqmDdlProperties().getTtlSec().toString())
                .withEnv("ADQM_ARCHIVE_DISK", config.getAdqmDdlProperties().getArchiveDisk())
                .withEnv("ADQM_CONSUMER_GROUP", config.getAdqmMppwProperties().getConsumerGroup())
                .withEnv("ADQM_BROKERS", config.getAdqmMppwProperties().getKafkaBrokers())
                .withEnv("ADQM_MPPW_LOAD_TYPE", config.getAdqmMppwProperties().getLoadType().name())
                .withEnv("ADQM_REST_LOAD_URL", config.getAdqmMppwProperties().getRestLoadUrl())
                .withEnv("ADQM_REST_LOAD_GROUP", config.getAdqmMppwProperties().getRestLoadConsumerGroup());
    }

    @Bean(destroyMethod = "close", name = "itTestVertx")
    public Vertx vertx() {
        System.setProperty("org.vertx.logger-delegate-factory-class-name",
                "org.vertx.java.core.logging.impl.SLF4JLogDelegateFactory");
        return Vertx.vertx();
    }
}
