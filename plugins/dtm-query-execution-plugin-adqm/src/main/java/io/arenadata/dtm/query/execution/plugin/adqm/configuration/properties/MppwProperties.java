package io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties;

import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.LoadType;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("adqm.mppw")
@Getter
@Setter
public class MppwProperties {
    private String consumerGroup;
    private String kafkaBrokers;
    private LoadType loadType;
    private String restLoadUrl;
    private String restLoadConsumerGroup;
}