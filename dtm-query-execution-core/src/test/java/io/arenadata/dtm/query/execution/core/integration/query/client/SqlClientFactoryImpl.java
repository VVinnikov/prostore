package io.arenadata.dtm.query.execution.core.integration.query.client;

import io.arenadata.dtm.query.execution.core.integration.AbstractCoreDtmIntegrationTest;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SqlClientFactoryImpl implements SqlClientFactory {

    private final Vertx vertx;

    @Autowired
    public SqlClientFactoryImpl(@Qualifier("itTestVertx") Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public SQLClient create(String datamartMnemonic) {
        val jdbcUrl = String.format("jdbc:adtm://%s/%s",
                AbstractCoreDtmIntegrationTest.getDtmCoreHostPortExternal(),
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
