package io.arenadata.dtm.query.execution.plugin.adg.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties(prefix = "adg.circuitbreaker")
@Component("adgCircuitBreakerProperties")
public class CircuitBreakerProperties {
    private int maxFailures;
    private long timeout;
    private boolean fallbackOnFailure;
    private long resetTimeout;
}
