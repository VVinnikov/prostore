package io.arenadata.dtm.kafka.core.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component("publishStatusEventProperties")
@Data
public class PublishStatusEventProperties {
    private String topic = "status.event";
}
