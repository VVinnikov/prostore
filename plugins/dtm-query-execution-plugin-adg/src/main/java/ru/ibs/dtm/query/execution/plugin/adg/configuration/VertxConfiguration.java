package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class VertxConfiguration {

    @Bean("adgWebClient")
    public WebClient webClient(@Qualifier("coreVertx") Vertx vertx) {
        return WebClient.create(vertx);
    }

    @Bean("adgCircuitBreaker")
    public CircuitBreaker circuitBreaker(@Qualifier("coreVertx") Vertx vertx,
                                         @Qualifier("adgCircuitBreakerProperties") CircuitBreakerProperties properties) {
        return CircuitBreaker.create("adgCircuitBreaker", vertx,
                new CircuitBreakerOptions()
                        .setMaxFailures(properties.getMaxFailures())
                        .setTimeout(properties.getTimeout())
                        .setFallbackOnFailure(properties.isFallbackOnFailure())
                        .setResetTimeout(properties.getResetTimeout()));
    }

}

