package ru.ibs.dtm.query.execution.plugin.adb.configuration;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.DatabaseTypes;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.GreenplumProperties;
import ru.ibs.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;

import java.util.HashMap;
import java.util.Map;

import static ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.DatabaseTypes.GREENPLUM;

@Configuration
public class QueryConfiguration {

  @Bean("adbDatabaseExecutors")
  public Map<DatabaseTypes, DatabaseExecutor> databaseExecutors(AdbQueryExecutor adbQueryExecutor) {
    Map<DatabaseTypes, DatabaseExecutor> beanMap = new HashMap<>();
    beanMap.put(GREENPLUM, adbQueryExecutor);
    return beanMap;
  }


  @Bean("adbQueryExecutor")
  public AdbQueryExecutor greenplam(@Qualifier("adbVertx") Vertx vertx, GreenplumProperties greenplumProperties) {
    PgPool pgPool = PgClient.pool(greenplumProperties.getOptions());
    return new AdbQueryExecutor(pgPool, greenplumProperties.getFetchSize());
  }
}
