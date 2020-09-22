package ru.ibs.dtm.query.execution.core.configuration.properties;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("core.datasource.zookeeper")
public class ZookeeperProperties {
    private String connectionString;
    private String chroot = "/arenadata";
    private int sessionTimeoutMs = 1000;
}
