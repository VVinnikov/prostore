package io.arenadata.dtm.query.execution.plugin.adb.query.configuration;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adb.base.configuration.properties.AdbProperties;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryConfiguration {

    @Bean("adbQueryExecutor")
    public AdbQueryExecutor greenplam(@Value("${core.env.name}") String database, // Todo transfer to EnvProperties
                                      AdbProperties adbProperties,
                                      @Qualifier("adbTypeToSqlTypeConverter") SqlTypeConverter typeConverter,
                                      @Qualifier("adbTypeFromSqlTypeConverter") SqlTypeConverter sqlTypeConverter) {
        PgPoolOptions poolOptions = new PgPoolOptions();
        poolOptions.setDatabase(database);
        poolOptions.setHost(adbProperties.getHost());
        poolOptions.setPort(adbProperties.getPort());
        poolOptions.setUser(adbProperties.getUser());
        poolOptions.setPassword(adbProperties.getPassword());
        poolOptions.setMaxSize(adbProperties.getMaxSize());
        PgPool pgPool = PgClient.pool(poolOptions);
        return new AdbQueryExecutor(pgPool, adbProperties.getFetchSize(), typeConverter, sqlTypeConverter);
    }
}
