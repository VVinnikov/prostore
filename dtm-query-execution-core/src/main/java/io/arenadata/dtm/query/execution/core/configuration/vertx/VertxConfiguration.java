package io.arenadata.dtm.query.execution.core.configuration.vertx;

import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Slf4j
@Configuration
public class VertxConfiguration implements ApplicationListener<ApplicationReadyEvent> {

    @Bean("coreVertx")
    @ConditionalOnMissingBean(Vertx.class)
    public Vertx vertx() {
        return Vertx.vertx();
    }

    /**
     * Centrally sets all verticals strictly after up all configurations. In contrast to the
     * initMethod call, it guarantees the order of the vertical deployment, since the @PostConstruct phase is executed only within the
     * configuration.
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        val informationSchemaService = event.getApplicationContext().getBean(InformationSchemaService.class);
        informationSchemaService.createInformationSchemaViews()
            .onSuccess(success -> deployVerticle(event))
            .onFailure(err -> {
                val exitCode = SpringApplication.exit(event.getApplicationContext(), () -> 1);
                System.exit(exitCode);
            });
    }

    private void deployVerticle(ApplicationReadyEvent event) {
        Vertx vertx = event.getApplicationContext().getBean("coreVertx", Vertx.class);
        Map<String, Verticle> verticles = event.getApplicationContext().getBeansOfType(Verticle.class);
        log.info("Verticals found: {}", verticles.size());
        verticles.forEach((key, value) -> vertx.deployVerticle(value, ar -> {
            if (ar.succeeded()) {
                log.debug("Vertical '{}' deployed successfully", key);
            } else {
                log.error("Vertical deploy error", ar.cause());
            }
        }));
    }
}
