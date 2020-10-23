package io.arenadata.dtm.query.execution.core.configuration.views;

import io.arenadata.dtm.query.execution.core.service.schema.SystemDatamartViewsProvider;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SystemViewsConfiguration {

    public SystemViewsConfiguration(@Qualifier("coreVertx") Vertx vertx, SystemDatamartViewsProvider systemDatamartViewsProvider) {
        fetchSystemViews(vertx, systemDatamartViewsProvider);
    }

    private void fetchSystemViews(Vertx vertx, SystemDatamartViewsProvider systemDatamartViewsProvider) {
        vertx.executeBlocking((Promise<Void> in) -> {
            systemDatamartViewsProvider.fetchSystemViews().onComplete(in);
        }, out -> {
            if (out.succeeded()) {
                log.debug("System views fetch completed");
            } else {
                log.error("System views fetch error", out.cause());
            }
        });
    }
}
