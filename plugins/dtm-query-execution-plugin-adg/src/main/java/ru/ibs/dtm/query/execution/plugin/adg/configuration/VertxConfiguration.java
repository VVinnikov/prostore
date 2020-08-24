package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.configuration.kafka.KafkaConfig;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;

@Slf4j
@Configuration
public class VertxConfiguration {

    @Bean("adgWebClient")
    public WebClient webClient(@Qualifier("adgVertx") Vertx vertx) {
        return WebClient.create(vertx);
    }

    @Bean("adgVertx")
    public Vertx clusteredVertx(@Qualifier("adgClusterManager") ZookeeperClusterManager clusterManager) {
        VertxOptions options = new VertxOptions().setClusterManager(clusterManager);
        CompletableFuture<Vertx> future = new CompletableFuture<>();
        Vertx.clusteredVertx(options, event -> {
            if (event.succeeded()) {
                future.complete(event.result());
            } else {
                log.error("adgVertx init error: ", event.cause());
                throw new RuntimeException(event.cause());
            }
        });
        return future.join();
    }

    @Bean("adgClusterManager")
    public ZookeeperClusterManager clusterManager(@Qualifier("coreKafkaProperties") KafkaConfig properties) {
        JsonObject config = new JsonObject();
        config.put("zookeeperHosts", properties.getKafkaClusterProperty().getZookeeperHosts());
        config.put("rootPath", properties.getKafkaClusterProperty().getRootPath());
        config.put("retry", new JsonObject()
                .put("initialSleepTime", 3000)
                .put("maxTimes", 3)
        );
        return new ZookeeperClusterManager(config);
    }

    @Bean("adgCircuitBreaker")
    public CircuitBreaker circuitBreaker(@Qualifier("adgVertx") Vertx vertx,
                                         @Qualifier("adgCircuitBreakerProperties") CircuitBreakerProperties properties) {
        return CircuitBreaker.create("adgCircuitBreaker", vertx,
                new CircuitBreakerOptions()
                        .setMaxFailures(properties.getMaxFailures())
                        .setTimeout(properties.getTimeout())
                        .setFallbackOnFailure(properties.isFallbackOnFailure())
                        .setResetTimeout(properties.getResetTimeout()));
    }

}
