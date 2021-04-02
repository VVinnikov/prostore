package io.arenadata.dtm.query.execution.plugin.adqm.base.configuration.properties;

import io.vertx.ext.web.client.WebClientOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("adqm.web-client")
public class AdqmWebClientProperties extends WebClientOptions {
    private static final int DEFAULT_MAX_POOL_SIZE = 100;

    public AdqmWebClientProperties() {
        super();
        setMaxPoolSize(DEFAULT_MAX_POOL_SIZE);
    }
}
