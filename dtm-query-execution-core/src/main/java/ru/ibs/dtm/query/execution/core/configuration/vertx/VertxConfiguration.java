package ru.ibs.dtm.query.execution.core.configuration.vertx;

import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class VertxConfiguration implements ApplicationListener<ApplicationReadyEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxConfiguration.class);

  @Bean("coreVertx")
  @ConditionalOnMissingBean(Vertx.class)
  public Vertx vertx() {
    return Vertx.vertx();
  }

  /**
   * Централизованно устанавливает все вертикали строго после подъема всех конфигураций. В отличие от вызова
   * initMethod гарантирует порядок деплоя вертикалей, поскольку @PostConstruct-фаза отрабатывает только в рамках
   * конфигурации.
   */
  @Override
  public void onApplicationEvent(ApplicationReadyEvent event) {
    // Получим ссылку на экземляр vertx или исключение при отсутствии бина
    Vertx vertx = event.getApplicationContext().getBean("coreVertx", Vertx.class);
    // Соберем все вертикали для деплоя
    Map<String, Verticle> verticles = event.getApplicationContext().getBeansOfType(Verticle.class);
    LOGGER.info("Verticals found: {}", verticles.size());
    verticles.forEach((key, value) -> {
      vertx.deployVerticle(value);
      LOGGER.debug("Vertical '{}' deployed successfully", key);
    });
  }
}
