package io.arenadata.dtm.query.execution.plugin.adb.base.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("adb.datasource")
@Component
public class AdbProperties {
  private static final int DEFAULT_FETCH_SIZE = 1_000;
  private static final int DEFAULT_PREPARED_CACHE_MAX_SIZE = 256;
  private static final int DEFAULT_PREPARED_CACHE_SQL_LIMIT = 2048;

  private String user;
  private String password;
  private String host;
  private int port;
  private int poolSize;
  private int executorsCount;
  private int fetchSize = DEFAULT_FETCH_SIZE;
  private int preparedStatementsCacheMaxSize = DEFAULT_PREPARED_CACHE_MAX_SIZE;
  private int preparedStatementsCacheSqlLimit = DEFAULT_PREPARED_CACHE_SQL_LIMIT;
  private boolean preparedStatementsCache = true;
}
