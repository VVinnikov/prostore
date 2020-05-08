package ru.ibs.dtm.query.execution.core.verticle;

import com.google.common.net.HttpHeaders;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;
import ru.ibs.dtm.query.execution.core.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.core.controller.DatamartMetaController;
import ru.ibs.dtm.query.execution.core.controller.QueryController;
import ru.ibs.dtm.query.execution.core.controller.RequestParam;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@Component
public class QueryVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryVerticle.class);

  private final AppConfiguration configuration;
  private final DatamartMetaController datamartMetaController;
  private final QueryController queryController;

  @Autowired
  public QueryVerticle(AppConfiguration configuration,
                       DatamartMetaController datamartMetaController,
                       QueryController queryController) {
    this.configuration = configuration;
    this.datamartMetaController = datamartMetaController;
    this.queryController = queryController;
  }

  @Override
  public void start() {
    Router router = Router.router(vertx);
    router.mountSubRouter("/", apiRouter());
    HttpServer httpServer = vertx.createHttpServer().requestHandler(router)
      .listen(configuration.httpPort());
    LOGGER.info("Сервер запущен на порте: {}", httpServer.actualPort());
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
      ctx.response().putHeader(HttpHeaders.CONTENT_TYPE,  MimeTypeUtils.APPLICATION_JSON_VALUE);
      ctx.response().end(error.encode());
    });
    router.get("/meta").handler(datamartMetaController::getDatamartMeta);
    router.get(String.format("/meta/:%s/entities", RequestParam.DATAMART_MNEMONIC))
      .handler(datamartMetaController::getDatamartEntityMeta);
    router.get(String.format("/meta/:%s/entity/:%s/attributes",
      RequestParam.DATAMART_MNEMONIC, RequestParam.ENTITY_MNEMONIC))
      .handler(datamartMetaController::getEntityAttributesMeta);
    router.post("/query/execute").handler(queryController::executeQueryWithoutParams);
    return router;
  }
}
