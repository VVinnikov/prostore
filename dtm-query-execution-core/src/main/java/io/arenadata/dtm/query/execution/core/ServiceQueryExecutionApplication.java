package io.arenadata.dtm.query.execution.core;

import io.arenadata.dtm.query.execution.core.utils.BeanNameGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.admin.SpringApplicationAdminJmxAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(exclude = {SpringApplicationAdminJmxAutoConfiguration.class})
@ConfigurationPropertiesScan("io.arenadata.dtm")
@ComponentScan(basePackages = {"io.arenadata.dtm.query.execution", "io.arenadata.dtm.kafka.core"}, nameGenerator = BeanNameGenerator.class)
public class ServiceQueryExecutionApplication {

  public static void main(String[] args) {
    SpringApplication.run(ServiceQueryExecutionApplication.class, args);
  }
}
