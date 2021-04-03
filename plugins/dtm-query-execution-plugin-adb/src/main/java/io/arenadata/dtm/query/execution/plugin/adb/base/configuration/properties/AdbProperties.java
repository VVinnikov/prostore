package io.arenadata.dtm.query.execution.plugin.adb.base.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("adb.datasource")
@Component
public class AdbProperties {
  private static final int DEFAULT_FETCH_SIZE = 1_000;

  private String user;
  private String password;
  private String host;
  private int port;
  private int maxSize;
  private int fetchSize = DEFAULT_FETCH_SIZE;
}
