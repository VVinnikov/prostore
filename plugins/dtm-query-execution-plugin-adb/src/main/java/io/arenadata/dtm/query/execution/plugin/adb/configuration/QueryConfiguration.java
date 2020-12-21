package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.GreenplumProperties;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
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
  public AdbQueryExecutor greenplam(@Value("${core.env.name}") String database,
                                    GreenplumProperties greenplumProperties,
                                    @Qualifier("adbTypeToSqlTypeConverter") SqlTypeConverter typeConverter) {
    PgPoolOptions poolOptions = new PgPoolOptions();
    poolOptions.setDatabase(database);
    poolOptions.setHost(greenplumProperties.getHost());
    poolOptions.setPort(greenplumProperties.getPort());
    poolOptions.setUser(greenplumProperties.getUser());
    poolOptions.setPassword(greenplumProperties.getPassword());
    poolOptions.setMaxSize(greenplumProperties.getMaxSize());
    PgPool pgPool = PgClient.pool(poolOptions);
    return new AdbQueryExecutor(pgPool, greenplumProperties.getFetchSize(), typeConverter);
  }
}
