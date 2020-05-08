package ru.ibs.dtm.query.execution.plugin.adb.configuration.properties;

import io.reactiverse.pgclient.PgPoolOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("datasource.adb")
@Component
public class GreenplumProperties {
  private final int DEFAULT_FETCH_SIZE = 1_000;

  int fetchSize = DEFAULT_FETCH_SIZE;
  PgPoolOptions options = new PgPoolOptions();

  public PgPoolOptions getOptions() {
    return options;
  }

  public void setOptions(PgPoolOptions options) {
    this.options = options;
  }

  public int getFetchSize() {
    return fetchSize;
  }
}
