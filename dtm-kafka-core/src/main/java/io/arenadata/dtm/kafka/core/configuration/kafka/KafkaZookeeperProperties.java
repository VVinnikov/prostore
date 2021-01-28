package io.arenadata.dtm.kafka.core.configuration.kafka;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;

@EqualsAndHashCode(callSuper = true)
@Data
@ConfigurationProperties("core.kafka.cluster.zookeeper")
public class KafkaZookeeperProperties extends BaseZookeeperProperties {
}
