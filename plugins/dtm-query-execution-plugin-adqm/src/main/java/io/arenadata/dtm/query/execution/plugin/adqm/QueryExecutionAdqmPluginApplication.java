package io.arenadata.dtm.query.execution.plugin.adqm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan(basePackages = "io.arenadata.dtm.query.execution.plugin.adqm.configuration")
public class QueryExecutionAdqmPluginApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueryExecutionAdqmPluginApplication.class, args);
    }
}
