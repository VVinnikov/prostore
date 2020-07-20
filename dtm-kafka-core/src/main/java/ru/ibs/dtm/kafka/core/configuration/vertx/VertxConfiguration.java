package ru.ibs.dtm.kafka.core.configuration.vertx;

import io.vertx.core.Vertx;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class VertxConfiguration {

    @Bean("kafkaVertx")
    public Vertx vertx() {
        return Vertx.vertx();
    }
}
