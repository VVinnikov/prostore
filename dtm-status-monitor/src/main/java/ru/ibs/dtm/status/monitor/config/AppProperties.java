package ru.ibs.dtm.status.monitor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("monitor")
@Data
public class AppProperties {
    private String brokersList;
    private int consumersCount = 8;
}
