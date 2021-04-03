package io.arenadata.dtm.query.execution.plugin.adqm.mppw.configuration.properties;

import io.arenadata.dtm.query.execution.plugin.adqm.mppw.kafka.service.load.LoadType;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("adqm.mppw")
@Getter
@Setter
public class AdqmMppwProperties {
    private String consumerGroup;
    private String kafkaBrokers;
    private LoadType loadType;
    private String restStartLoadUrl;
    private String restStopLoadUrl;
    private String versionUrl;
    private String restLoadConsumerGroup;
}
