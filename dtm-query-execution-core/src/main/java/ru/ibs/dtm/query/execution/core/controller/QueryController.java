package ru.ibs.dtm.query.execution.core.controller;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.service.QueryAnalyzer;

@Component
public class QueryController {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryController.class);

  private QueryAnalyzer queryAnalyzer;

  @Autowired
  public QueryController(QueryAnalyzer queryAnalyzer) {
    this.queryAnalyzer = queryAnalyzer;
  }

  public void executeQueryWithoutParams(RoutingContext context) {
    QueryRequest queryRequest = context.getBodyAsJson().mapTo(QueryRequest.class);
    LOGGER.info("Отправлен запрос на выполнение: [{}]", queryRequest);
    queryAnalyzer.analyzeAndExecute(queryRequest, queryResult -> {
      if (queryResult.succeeded()) {

        if (queryResult.result().getRequestId() == null) {
          queryResult.result().setRequestId(queryRequest.getRequestId());
        }

        String json = Json.encode(queryResult.result());
        LOGGER.info("Выполнен запрос {}", queryRequest.getSql());
        //LOGGER.trace("Результат выполнения запроса '{}' : {}", queryRequest, json);
        context.response()
          .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
          .setStatusCode(HttpResponseStatus.OK.code())
          .end(json);
      } else {
        LOGGER.error("Ошибка при выполнении запроса {}", queryRequest, queryResult.cause());
        context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), queryResult.cause());
      }
    });
  }
}
