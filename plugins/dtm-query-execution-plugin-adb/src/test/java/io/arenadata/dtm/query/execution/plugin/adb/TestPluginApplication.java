package io.arenadata.dtm.query.execution.plugin.adb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan("io.arenadata.dtm.query.execution.plugin.adb")
public class TestPluginApplication {
  public static void main(String[] args) {
    SpringApplication.run(TestPluginApplication.class, args);
  }
}
