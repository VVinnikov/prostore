package ru.ibs.dtm.query.execution.plugin.adb.configuration;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class VertxConfiguration {

  @Bean("adbWebClient")
  public WebClient webClient(@Qualifier("coreVertx") Vertx vertx) {
    return WebClient.create(vertx);
  }
}
