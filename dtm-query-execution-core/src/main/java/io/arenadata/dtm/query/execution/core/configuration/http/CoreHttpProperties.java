package io.arenadata.dtm.query.execution.core.configuration.http;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Data
@ConfigurationProperties("core.http")
public class CoreHttpProperties {
    private boolean tcpNoDelay = true;
    private boolean tcpFastOpen = true;
    private boolean tcpQuickAck = true;
    private int port = 9090;
}
