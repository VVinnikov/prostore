package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.AdbWebClientProperties;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class VertxConfiguration {

    @Bean("adbWebClient")
    public WebClient webClient(@Qualifier("coreVertx") Vertx vertx, AdbWebClientProperties properties) {
        return WebClient.create(vertx, properties);
    }
}
