package io.arenadata.dtm.query.execution.plugin.adg.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("adg.mppw")
@Component
public class MppwProperties {
    private String consumerGroup;
}
