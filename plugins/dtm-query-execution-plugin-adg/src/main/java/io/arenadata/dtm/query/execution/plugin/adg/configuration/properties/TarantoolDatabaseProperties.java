package io.arenadata.dtm.query.execution.plugin.adg.configuration.properties;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ToString
@ConfigurationProperties(prefix = "adg.tarantool.db")
public class TarantoolDatabaseProperties {
  String host = "localhost";
  Integer port = 3511;
  String user = "admin";
  String password = "123";
  Integer operationTimeout = 10000;
  Integer retryCount = 0;
  String engine = "MEMTX";
  Long initTimeoutMillis = 60000L;
}
