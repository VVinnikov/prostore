package ru.ibs.dtm.query.execution.core.controller;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.service.QueryAnalyzer;

@Slf4j
@Component
public class QueryController {
    private final QueryAnalyzer queryAnalyzer;

    @Autowired
    public QueryController(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public void executeQueryWithoutParams(RoutingContext context) {
        QueryRequest queryRequest = context.getBodyAsJson().mapTo(QueryRequest.class);
        log.info("Execution request sent: [{}]", queryRequest);
        queryAnalyzer.analyzeAndExecute(queryRequest, queryResult -> {
            if (queryResult.succeeded()) {

                if (queryResult.result().getRequestId() == null) {
                    queryResult.result().setRequestId(queryRequest.getRequestId());
                }

                String json = Json.encode(queryResult.result());
                log.info("Request completed: [{}]", queryRequest.getSql());
                context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(json);
            } else {
                log.error("Error while executing request [{}]", queryRequest, queryResult.cause());
                context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), queryResult.cause());
            }
        });
    }
}
