package io.arenadata.dtm.kafka.core.configuration.properties;

import lombok.Data;

@Data
public class KafkaStatusMonitorProperties {
    private String statusUrl;
    private String versionUrl;
}
