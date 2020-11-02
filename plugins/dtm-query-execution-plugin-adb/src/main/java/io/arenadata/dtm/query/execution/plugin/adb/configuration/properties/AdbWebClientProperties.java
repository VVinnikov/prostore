package io.arenadata.dtm.query.execution.plugin.adb.configuration.properties;

import io.vertx.ext.web.client.WebClientOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("adb.web-client")
public class AdbWebClientProperties extends WebClientOptions {
    private static final int DEFAULT_MAX_POOL_SIZE = 100;

    public AdbWebClientProperties() {
        super();
        setMaxPoolSize(DEFAULT_MAX_POOL_SIZE);
    }
}
