package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmWebClientProperties;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class VertxConfiguration {

    @Bean("adqmWebClient")
    public WebClient webClient(@Qualifier("coreVertx") Vertx vertx, AdqmWebClientProperties properties) {
        return WebClient.create(vertx, properties);
    }
}
