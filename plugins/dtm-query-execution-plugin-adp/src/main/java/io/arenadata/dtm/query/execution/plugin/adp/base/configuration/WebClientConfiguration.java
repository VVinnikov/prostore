package io.arenadata.dtm.query.execution.plugin.adp.base.configuration;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebClientConfiguration {

    @Bean("adpWebClient")
    public WebClient webClient(@Qualifier("coreVertx") Vertx vertx) {
        return WebClient.create(vertx);
    }
}
