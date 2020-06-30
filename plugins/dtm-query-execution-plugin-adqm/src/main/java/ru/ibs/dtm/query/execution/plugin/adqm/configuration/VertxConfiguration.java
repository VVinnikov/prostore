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
    public WebClient webClient(@Qualifier("adqmVertx") Vertx vertx) {
        return WebClient.create(vertx);
    }

    @Bean("adqmVertx")
    public Vertx clusteredVertx(@Qualifier("adqmClusterManager") ZookeeperClusterManager clusterManager) {
        VertxOptions options = new VertxOptions().setClusterManager(clusterManager);
        CompletableFuture<Vertx> future = new CompletableFuture<>();
        Vertx.clusteredVertx(options, event -> {
            if (event.succeeded()) {
                future.complete(event.result());
            } else {
                future.complete(null);
            }
        });
        return future.join();
    }

    @Bean("adqmClusterManager")
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
}
