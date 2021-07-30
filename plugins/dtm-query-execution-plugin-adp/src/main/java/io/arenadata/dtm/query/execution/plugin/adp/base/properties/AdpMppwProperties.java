package io.arenadata.dtm.query.execution.plugin.adp.base.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("adp.mppw")
@Component
public class AdpMppwProperties {
    private String restStartLoadUrl;
    private String restStopLoadUrl;
    private String restVersionUrl;
    private String kafkaConsumerGroup;
}
