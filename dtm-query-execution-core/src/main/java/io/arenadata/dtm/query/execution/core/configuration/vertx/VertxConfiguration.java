package io.arenadata.dtm.query.execution.core.configuration.vertx;

import io.arenadata.dtm.query.execution.core.service.init.CoreInitializationService;
import io.arenadata.dtm.query.execution.core.service.metadata.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.rollback.RestoreStateService;
import io.arenadata.dtm.query.execution.core.verticle.starter.QueryWorkerStarter;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
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

import java.util.Collection;
import java.util.stream.Collectors;

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
        val coreInitializationService = event.getApplicationContext().getBean(CoreInitializationService.class);
        val restoreStateService = event.getApplicationContext().getBean(RestoreStateService.class);
        val vertx = event.getApplicationContext().getBean("coreVertx", Vertx.class);
        val verticles = event.getApplicationContext().getBeansOfType(Verticle.class);
        val queryWorkerStarter = event.getApplicationContext().getBean(QueryWorkerStarter.class);
        informationSchemaService.createInformationSchemaViews()
                .compose(v -> deployVerticle(vertx, verticles.values()))
                .compose(v -> coreInitializationService.execute())
                .compose(v -> {
                    restoreStateService.restoreState()
                            .onFailure(fail -> log.error("Error in restoring state", fail));
                    return queryWorkerStarter.start(vertx);
                })
                .onSuccess(success -> log.debug("Dtm started successfully"))
                .onFailure(err -> {
                    log.error("Core startup error: ", err);
                    val exitCode = SpringApplication.exit(event.getApplicationContext(), () -> 1);
                    System.exit(exitCode);
                });
    }

    private Future<Object> deployVerticle(Vertx vertx, Collection<Verticle> verticles) {
        log.info("Verticals found: {}", verticles.size());
        return CompositeFuture.join(verticles.stream()
                .map(verticle -> Future.future(p -> {
                    vertx.deployVerticle(verticle, ar -> {
                        if (ar.succeeded()) {
                            log.debug("Vertical '{}' deployed successfully", verticle.getClass().getName());
                            p.complete();
                        } else {
                            log.error("Vertical deploy error", ar.cause());
                            p.fail(ar.cause());
                        }
                    });
                }))
                .collect(Collectors.toList()))
                .mapEmpty();
    }

}
