package ru.ibs.dtm.query.execution.core;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.admin.SpringApplicationAdminJmxAutoConfiguration;
import org.springframework.boot.autoconfigure.jooq.JooqAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.ComponentScan;
import ru.ibs.dtm.query.execution.core.configuration.jooq.JooqConfiguration;
import ru.ibs.dtm.query.execution.core.utils.BeanNameGenerator;

@SpringBootApplication(exclude = {SpringApplicationAdminJmxAutoConfiguration.class, JooqAutoConfiguration.class})
@ConfigurationPropertiesScan("ru.ibs.dtm")
@ComponentScan(basePackages = "ru.ibs.dtm.query.execution", nameGenerator = BeanNameGenerator.class)
public class ServiceQueryExecutionApplication {

  public static void main(String[] args) {
    SpringApplication.run(ServiceQueryExecutionApplication.class, args);
  }
}
