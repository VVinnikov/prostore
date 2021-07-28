package io.arenadata.dtm.query.execution.core.base.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("core.delta")
@Data
public class RollbackDeltaProperties {

    private long rollbackStatusCallsMs = 2000;

}
