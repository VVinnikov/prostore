package io.arenadata.dtm.query.execution.plugin.adb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan(basePackages = "io.arenadata.dtm.query.execution.plugin.adb.configuration")
public class QueryExecutionAdbPluginApplication {

  public static void main(String[] args) {
    SpringApplication.run(QueryExecutionAdbPluginApplication.class, args);
  }
}
