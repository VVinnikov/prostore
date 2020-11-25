package io.arenadata.dtm.query.execution.core.integration.configuration;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.execution.core.configuration.cache.CacheProperties;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.query.execution.core.configuration.properties.ServiceDbZookeeperProperties;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.GreenplumProperties;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolCartridgeProperties;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMpprProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Data
public class IntegrationTestProperties {
    @Value("${core.env.name}")
    private String env;
    @Value("${core.plugins.active}")
    private String activePlugins;
    @Value("${core.http.port}")
    private Integer dtmCorePort;
    @Value("${management.server.port}")
    private Integer dtmMetricsPort;
    private Integer zkPort = 2181;
    @Autowired
    private EdmlProperties edmlProperties;
    @Autowired
    private ServiceDbZookeeperProperties zkDsProperties;
    @Autowired
    private KafkaZookeeperProperties kafkaZkProperties;
    @Autowired
    private KafkaProperties kafkaProperties;
    @Autowired
    private CacheProperties cacheProperties;
    @Autowired
    private GreenplumProperties adbDsProperties;
    @Autowired
    private MppwProperties adbMppwProperties;
    @Autowired
    private TarantoolDatabaseProperties adgDsProperties;
    @Autowired
    private TarantoolCartridgeProperties adgCrtgProperties;
    @Autowired
    private ClickhouseProperties adqmDsProperties;
    @Autowired
    private DdlProperties adqmDdlProperties;
    @Autowired
    private AdqmMpprProperties adqmMpprProperties;
    @Autowired
    private AdqmMppwProperties adqmMppwProperties;
}
