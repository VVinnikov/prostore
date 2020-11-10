package io.arenadata.dtm.kafka.core.configuration.kafka;

import lombok.Data;

@Data
public class BaseZookeeperProperties {
    private String connectionString;
    private int sessionTimeoutMs = 1000;
    private int connectionTimeoutMs = 15000;
}
