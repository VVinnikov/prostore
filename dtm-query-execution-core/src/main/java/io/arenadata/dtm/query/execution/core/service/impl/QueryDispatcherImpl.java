package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.service.QueryDispatcher;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class QueryDispatcherImpl implements QueryDispatcher {

    private final Map<SqlProcessingType, DatamartExecutionService<RequestContext<? extends DatamartRequest>, QueryResult>> serviceMap = new HashMap<>();

    @Autowired
    public QueryDispatcherImpl(List<DatamartExecutionService<? extends RequestContext<? extends DatamartRequest>, QueryResult>> services) {
        for (DatamartExecutionService<? extends RequestContext<? extends DatamartRequest>, QueryResult> es : services) {
            serviceMap.put(es.getSqlProcessingType(), (DatamartExecutionService<RequestContext<? extends DatamartRequest>, QueryResult>) es);
        }
    }

    @Override
    public Future<QueryResult> dispatch(RequestContext<?> context) {
        try {
            return serviceMap.get(context.getProcessingType()).execute(context);
        } catch (Exception e) {
            return Future.failedFuture(new DtmException("An error occurred while dispatching the request", e));
        }
    }
}
