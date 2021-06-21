package io.arenadata.dtm.query.execution.core.base.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("core.matviewsync")
@Data
public class MatViewSyncProperties {
    private long periodMs = 5000;
    private int retryCount = 10;
}
