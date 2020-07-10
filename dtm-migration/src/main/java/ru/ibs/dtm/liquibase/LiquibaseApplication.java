package ru.ibs.dtm.liquibase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import ru.ibs.dtm.liquibase.configuration.properties.DatasourceProperties;

@SpringBootApplication
@ConfigurationPropertiesScan("ru.ibs.dtm.liquibase.configuration")
public class LiquibaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(LiquibaseApplication.class, args);
    }
}
