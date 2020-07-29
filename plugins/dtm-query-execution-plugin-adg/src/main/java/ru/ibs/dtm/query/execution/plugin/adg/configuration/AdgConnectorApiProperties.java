package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component("adgConnectorProperties")
@ConfigurationProperties(prefix = "adg.tarantool.connector")
public class AdgConnectorApiProperties {
    private String url;
    private String kafkaSubscriptionUrl;
    private String kafkaLoadDataUrl;
    private String transferDataToScdTableUrl;
}
