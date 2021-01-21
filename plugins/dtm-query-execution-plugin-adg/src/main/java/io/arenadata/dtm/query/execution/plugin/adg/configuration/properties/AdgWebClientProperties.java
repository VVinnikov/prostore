package io.arenadata.dtm.query.execution.plugin.adg.configuration.properties;

import io.vertx.ext.web.client.WebClientOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;

public class AdgWebClientProperties extends WebClientOptions {
    private static final int DEFAULT_MAX_POOL_SIZE = 100;

    public AdgWebClientProperties() {
        super();
        setMaxPoolSize(DEFAULT_MAX_POOL_SIZE);
    }
}
