package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CompletableFuture;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
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
        future.complete(null);
      }
    });
    return future.join();
  }

  @Bean("adgClusterManager")
  public ZookeeperClusterManager clusterManager(@Qualifier("adgKafkaProperties") KafkaProperties properties) {
    JsonObject config = new JsonObject();
    config.put("zookeeperHosts", properties.cluster.getZookeeperHosts());
    config.put("rootPath", properties.cluster.getRootPath());
    config.put("retry", new JsonObject()
      .put("initialSleepTime", 3000)
      .put("maxTimes", 3)
    );
    return new ZookeeperClusterManager(config);
  }

}
