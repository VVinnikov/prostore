package io.arenadata.dtm.query.execution.plugin.adg.configuration;

import io.vertx.ext.web.client.WebClientOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("adg.web-client")
public class AdgWebClientProperties extends WebClientOptions {
    private static final int DEFAULT_MAX_POOL_SIZE = 100;

    public AdgWebClientProperties() {
        super();
        setMaxPoolSize(DEFAULT_MAX_POOL_SIZE);
    }
}
