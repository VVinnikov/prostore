package ru.ibs.dtm.kafka.core.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component("statusEventProperties")
@ConfigurationProperties(prefix = "status.event.publish")
@Data
public class PublishStatusEventProperties {
    private String topic = "topicS";
    private boolean enabled = true;
}
