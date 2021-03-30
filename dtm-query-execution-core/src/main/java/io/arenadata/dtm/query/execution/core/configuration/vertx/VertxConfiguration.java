package io.arenadata.dtm.query.execution.core.configuration.vertx;

import io.arenadata.dtm.query.execution.core.service.init.CoreInitializationService;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
        val coreInitializationService = event.getApplicationContext().getBean(CoreInitializationService.class);
        coreInitializationService.execute()
                .onSuccess(success -> log.debug("Dtm started successfully"))
                .onFailure(err -> {
                    log.error("Core startup error: ", err);
                    val exitCode = SpringApplication.exit(event.getApplicationContext(), () -> 1);
                    System.exit(exitCode);
                });
    }

}
