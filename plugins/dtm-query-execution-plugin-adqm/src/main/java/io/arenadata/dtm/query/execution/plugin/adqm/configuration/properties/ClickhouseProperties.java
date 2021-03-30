package io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("adqm.datasource")
public class ClickhouseProperties {
    private String database;
    private String hosts;
    private String user;
    private String password;
    private int socketTimeout = 30_000;
    private int dataTransferTimeout = 10_000;
}
