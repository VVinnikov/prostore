package io.arenadata.dtm.liquibase.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@ConfigurationProperties("liquibase")
@Component
@Data
public class LiquibaseProperties {

    private String command;
    private Map<String, String> changeLog;
}
