package io.arenadata.dtm.query.execution.core.base.configuration.properties;


import io.arenadata.dtm.kafka.core.configuration.kafka.BaseZookeeperProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;

@EqualsAndHashCode(callSuper = true)
@Data
@ConfigurationProperties("core.datasource.zookeeper")
public class ServiceDbZookeeperProperties extends BaseZookeeperProperties {
    private String chroot = "/arenadata";
}


