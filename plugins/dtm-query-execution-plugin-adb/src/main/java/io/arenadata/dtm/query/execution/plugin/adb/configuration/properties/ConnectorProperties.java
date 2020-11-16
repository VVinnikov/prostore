package io.arenadata.dtm.query.execution.plugin.adb.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("adb.mppr")
@Data
public class ConnectorProperties {
    private String host;
    private Integer port;
    private String url;

}
