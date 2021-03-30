package io.arenadata.dtm.liquibase.configuration.properties;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("")
@Component
@Data
@ToString
public class DatasourceProperties {
    private String database;
    private String host;
    private Integer port;
    private String password;
    private String username;
    private Integer maxPoolSize;
}


