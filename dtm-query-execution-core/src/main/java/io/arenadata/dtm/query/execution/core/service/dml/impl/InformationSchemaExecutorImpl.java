package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

@Service
public class InformationSchemaExecutorImpl implements InformationSchemaExecutor {

    private final HSQLClient client;

    @Autowired
    public InformationSchemaExecutorImpl(HSQLClient client) {
        this.client = client;
    }

    @Override
    public void execute(QuerySourceRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        client.getQueryResult(request.getQueryRequest().getSql())
            .onSuccess(resultSet ->
            {
                val result = resultSet.getRows().stream()
                        .map(JsonObject::getMap)
                        .collect(Collectors.toList());
                asyncResultHandler.handle(Future.succeededFuture(
                    new QueryResult(request.getQueryRequest().getRequestId(), result, request.getMetadata())));
            })
            .onFailure(r -> asyncResultHandler.handle(Future.failedFuture(r.getCause())));
    }
}
