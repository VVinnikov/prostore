package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component("adgKafkaConnectorProperties")
@ConfigurationProperties(prefix = "kafka.adg.connector")
public class AdgConnectorApiProperties {
    private String address;
    private String subscriptionPath;
    private String loadDataPath;
    private String transferDataToScdTablePath;
}
