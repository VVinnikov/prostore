package ru.ibs.dtm.query.execution.plugin.adg.configuration;

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
}
