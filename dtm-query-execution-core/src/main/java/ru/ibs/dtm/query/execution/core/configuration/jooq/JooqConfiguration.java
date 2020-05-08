package ru.ibs.dtm.query.execution.core.configuration.jooq;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JooqConfiguration {

  public org.jooq.Configuration jooqConfiguration() {
    org.jooq.Configuration configuration = new DefaultConfiguration();
    configuration.set(SQLDialect.MARIADB);
    return configuration;
  }

  @Bean("coreQueryExecutor")
  public AsyncClassicGenericQueryExecutor queryExecutor(@Qualifier("coreAsyncSQLClient") AsyncSQLClient asyncSQLClient) {
    return new AsyncClassicGenericQueryExecutor(jooqConfiguration(), asyncSQLClient);
  }

  @Bean("coreAsyncSQLClient")
  public AsyncSQLClient asyncSQLClient(@Qualifier("coreVertx") Vertx vertx,
                                       MariaProperties properties) {
    JsonObject config = new JsonObject()
      .put("host", properties.options.getHost())
      .put("username", properties.options.getUser())
      .put("password", properties.options.getPassword())
      .put("database", properties.options.getDatabase());
    return MySQLClient.createNonShared(vertx, config);
  }
}
