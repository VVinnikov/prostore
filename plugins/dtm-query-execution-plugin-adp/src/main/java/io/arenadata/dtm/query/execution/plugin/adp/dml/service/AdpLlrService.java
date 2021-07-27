package io.arenadata.dtm.query.execution.plugin.adp.dml.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpLlrService")
public class AdpLlrService implements LlrService<QueryResult> { //todo: extends QueryResultCacheableLlrService
    @Override
    public Future<QueryResult> execute(LlrRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }

    @Override
    public Future<Void> prepare(LlrRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
