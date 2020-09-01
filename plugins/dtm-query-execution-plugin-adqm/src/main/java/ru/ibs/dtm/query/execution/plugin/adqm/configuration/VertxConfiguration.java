package ru.ibs.dtm.query.execution.plugin.adqm.configuration;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.configuration.kafka.KafkaConfig;

import java.util.concurrent.CompletableFuture;

@Configuration
public class VertxConfiguration {

    @Bean("adqmWebClient")
    public WebClient webClient(@Qualifier("coreVertx") Vertx vertx) {
        return WebClient.create(vertx);
    }
}
