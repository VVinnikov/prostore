package io.arenadata.dtm.kafka.core.configuration.properties;

import io.arenadata.dtm.common.configuration.kafka.*;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component("coreKafkaProperties")
@ConfigurationProperties(prefix = "core.kafka")
@Data
public class KafkaProperties {
    KafkaAdminProperty admin = new KafkaAdminProperty();

    private String statusMonitorUrl;

}
