package io.arenadata.dtm.status.monitor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Data
public class AppProperties {
    private String brokersList;
    private int consumersCount = 8;
}
