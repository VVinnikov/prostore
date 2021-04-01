package io.arenadata.dtm.query.execution.core.verticle;

import com.google.common.net.HttpHeaders;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.CoreHttpProperties;
import io.arenadata.dtm.query.execution.core.controller.DatamartMetaController;
import io.arenadata.dtm.query.execution.core.controller.MetricsController;
import io.arenadata.dtm.query.execution.core.controller.QueryController;
import io.arenadata.dtm.query.execution.core.base.dto.RequestParam;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.MimeTypeUtils;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@Slf4j
public class QueryVerticle extends AbstractVerticle {

    private final CoreHttpProperties httpProperties;
    private final DatamartMetaController datamartMetaController;
    private final QueryController queryController;
    private final MetricsController metricsController;

    public QueryVerticle(CoreHttpProperties httpProperties,
                         DatamartMetaController datamartMetaController,
                         QueryController queryController,
                         MetricsController metricsController) {
        this.httpProperties = httpProperties;
        this.datamartMetaController = datamartMetaController;
        this.queryController = queryController;
        this.metricsController = metricsController;
    }

    @Override
    public void start() {
        Router router = Router.router(vertx);
        router.mountSubRouter("/", apiRouter());
        vertx.createHttpServer(
                new HttpServerOptions()
                        .setTcpNoDelay(httpProperties.isTcpNoDelay())
                        .setTcpFastOpen(httpProperties.isTcpFastOpen())
                        .setTcpQuickAck(httpProperties.isTcpQuickAck())
        ).requestHandler(router)
                .listen(httpProperties.getPort());
    }

    private Router apiRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().consumes(APPLICATION_JSON_VALUE);
        router.route().produces(APPLICATION_JSON_VALUE);
        router.route().failureHandler(ctx -> {
            final JsonObject error = new JsonObject()
                    .put("exceptionMessage", ctx.failure().getMessage());
            ctx.response().setStatusCode(ctx.statusCode());
            ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE);
            ctx.response().end(error.encode());
        });
        router.get("/meta").handler(datamartMetaController::getDatamartMeta);
        router.get(String.format("/meta/:%s/entities", RequestParam.DATAMART_MNEMONIC))
                .handler(datamartMetaController::getDatamartEntityMeta);
        router.get(String.format("/meta/:%s/entity/:%s/attributes",
                RequestParam.DATAMART_MNEMONIC, RequestParam.ENTITY_MNEMONIC))
                .handler(datamartMetaController::getEntityAttributesMeta);
        router.post("/query/execute").handler(queryController::executeQuery);
        router.post("/query/prepare").handler(queryController::prepareQuery);
        router.put("/metrics/turn/on").handler(metricsController::turnOn);
        router.put("/metrics/turn/off").handler(metricsController::turnOff);
        return router;
    }
}
