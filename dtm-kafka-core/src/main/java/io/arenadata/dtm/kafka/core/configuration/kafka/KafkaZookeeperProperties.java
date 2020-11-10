package io.arenadata.dtm.kafka.core.configuration.kafka;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@EqualsAndHashCode(callSuper = true)
@Data
@Component
@ConfigurationProperties("core.kafka.cluster.zookeeper")
public class KafkaZookeeperProperties extends BaseZookeeperProperties {
}
