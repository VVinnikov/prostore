package ru.ibs.dtm.query.execution.core.configuration.jooq;

import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("core.datasource.service")
@Component
public class MariaProperties {
  private final int DEFAULT_FETCH_SIZE = 1_000;
  int fetchSize = DEFAULT_FETCH_SIZE;

  MySQLConnectOptions options = new MySQLConnectOptions();
  PoolOptions poolOptions = new PoolOptions();

  public MySQLConnectOptions getOptions() {
    return options;
  }

  public void setOptions(MySQLConnectOptions options) {
    this.options = options;
  }

  public PoolOptions getPoolOptions() {
    return poolOptions;
  }

  public void setPoolOptions(PoolOptions poolOptions) {
    this.poolOptions = poolOptions;
  }
}


