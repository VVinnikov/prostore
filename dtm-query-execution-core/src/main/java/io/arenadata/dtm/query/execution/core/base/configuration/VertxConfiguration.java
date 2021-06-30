package io.arenadata.dtm.query.execution.core.base.configuration;

import io.arenadata.dtm.query.execution.core.base.configuration.properties.VertxPoolProperties;
import io.arenadata.dtm.query.execution.core.init.service.CoreInitializationService;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
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
    public Vertx vertx(VertxPoolProperties properties) {
        VertxOptions options = new VertxOptions();
        options.setWorkerPoolSize(properties.getWorkerPool());
        options.setEventLoopPoolSize(properties.getEventLoopPool());
        options.setPreferNativeTransport(true);
        return Vertx.vertx(options);
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
