package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component("adgMppwKafkaProperties")
@ConfigurationProperties(prefix = "mppw.adg.kafka")
public class AdgMppwKafkaProperties {
    private long maxNumberOfMessagesPerPartition = 200;
}
