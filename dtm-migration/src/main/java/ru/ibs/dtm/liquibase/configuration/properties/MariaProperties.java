package ru.ibs.dtm.liquibase.configuration.properties;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("datasource.service.options")
@Component
@Data
@ToString
public class MariaProperties {
    private String database;
    private String host;
    private Integer port;
    private String password;
    private String user;
    private Integer maxPoolSize;
}


