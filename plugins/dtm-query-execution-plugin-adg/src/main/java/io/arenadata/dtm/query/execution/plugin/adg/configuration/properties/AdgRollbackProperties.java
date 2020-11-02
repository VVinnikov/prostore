package io.arenadata.dtm.query.execution.plugin.adg.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component("adgRollbackProperties")
@ConfigurationProperties(prefix = "adg.rollback")
public class AdgRollbackProperties {
    private int eraseOperationBatchSize = 300;
}
