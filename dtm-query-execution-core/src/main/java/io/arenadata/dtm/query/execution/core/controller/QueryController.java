package io.arenadata.dtm.query.execution.core.controller;

import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.query.execution.core.service.QueryAnalyzer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

@Slf4j
@Component
public class QueryController {
    private final QueryAnalyzer queryAnalyzer;

    @Autowired
    public QueryController(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public void executeQueryWithoutParams(RoutingContext context) {
        InputQueryRequest inputQueryRequest = context.getBodyAsJson().mapTo(InputQueryRequest.class);
        log.info("Execution request sent: [{}]", inputQueryRequest);
        queryAnalyzer.analyzeAndExecute(inputQueryRequest, queryResult -> {
            if (queryResult.succeeded()) {

                if (queryResult.result().getRequestId() == null) {
                    queryResult.result().setRequestId(inputQueryRequest.getRequestId());
                }

                String json = Json.encode(queryResult.result());
                log.info("Request completed: [{}]", inputQueryRequest.getSql());
                context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(json);
            } else {
                log.error("Error while executing request [{}]", inputQueryRequest, queryResult.cause());
                context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), queryResult.cause());
            }
        });
    }
}
