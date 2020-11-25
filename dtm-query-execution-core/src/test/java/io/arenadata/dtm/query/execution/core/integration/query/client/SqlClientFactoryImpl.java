package io.arenadata.dtm.query.execution.core.integration.query.client;

import io.arenadata.dtm.query.execution.core.integration.configuration.IntegrationTestProperties;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testcontainers.containers.GenericContainer;

@Slf4j
@Component
public class SqlClientFactoryImpl implements SqlClientFactory {

    private final Vertx vertx;
    private final GenericContainer<?> dtmCoreContainer;
    private final IntegrationTestProperties testPropertiesl;

    @Autowired
    public SqlClientFactoryImpl(@Qualifier("itTestVertx") Vertx vertx,
                                @Qualifier("coreDtm") GenericContainer<?> dtmCoreContainer,
                                IntegrationTestProperties testPropertiesl) {
        this.vertx = vertx;
        this.dtmCoreContainer = dtmCoreContainer;
        this.testPropertiesl = testPropertiesl;
    }

    @Override
    public SQLClient create(String datamartMnemonic) {
        val jdbcUrl = String.format("jdbc:adtm://%s:%d/%s",
                dtmCoreContainer.getHost(),
                dtmCoreContainer.getMappedPort(testPropertiesl.getDtmCorePort()),
                datamartMnemonic);
        val jsonConfig = new JsonObject()
                .put("driver_class", "io.arenadata.dtm.jdbc.DtmDriver")
                .put("max_pool_size", 5)
                .put("password", "")
                .put("user", "")
                .put("url", jdbcUrl);
        return JDBCClient.create(vertx, jsonConfig);
    }
}
