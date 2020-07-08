package ru.ibs.dtm.liquibase.configuration.properties;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@ConfigurationProperties("datasource")
@Component
@Data
@ToString
public class DatasourceProperties {
    private String database;
    private String host;
    private Integer port;
    private String password;
    private String user;
    private Integer maxPoolSize;
}


