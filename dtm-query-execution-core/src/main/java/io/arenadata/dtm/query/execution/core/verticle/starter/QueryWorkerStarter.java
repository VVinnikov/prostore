package io.arenadata.dtm.query.execution.core.verticle.starter;

import io.arenadata.dtm.query.execution.core.base.configuration.properties.CoreHttpProperties;
import io.arenadata.dtm.query.execution.core.controller.DatamartMetaController;
import io.arenadata.dtm.query.execution.core.controller.MetricsController;
import io.arenadata.dtm.query.execution.core.controller.QueryController;
import io.arenadata.dtm.query.execution.core.verticle.QueryVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class QueryWorkerStarter {
    private final DatamartMetaController datamartMetaController;
    private final MetricsController metricsController;
    private final CoreHttpProperties httpProperties;
    private final QueryController queryController;

    public Future<Void> start(Vertx vertx) {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        return Future.future(p -> vertx.deployVerticle(() -> new QueryVerticle(httpProperties, datamartMetaController, queryController, metricsController),
                new DeploymentOptions().setInstances(availableProcessors),
                ar -> {
                    if (ar.succeeded()) {
                        log.debug("Verticles '{}'({}) deployed successfully", QueryVerticle.class.getSimpleName(), availableProcessors);
                        log.info("The server is running on the port: {}", httpProperties.getPort());
                        p.complete();
                    } else {
                        log.error("Verticles deploy error", ar.cause());
                        p.fail(ar.cause());
                    }
                }));
    }

}
