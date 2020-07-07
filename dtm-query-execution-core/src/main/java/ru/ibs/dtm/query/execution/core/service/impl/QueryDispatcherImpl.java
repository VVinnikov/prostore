package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.service.QueryDispatcher;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class QueryDispatcherImpl implements QueryDispatcher {

    private final Map<SqlProcessingType, DatamartExecutionService<RequestContext<? extends DatamartRequest>, AsyncResult<QueryResult>>> serviceMap = new HashMap<>();

    @Autowired
    public QueryDispatcherImpl(List<DatamartExecutionService<? extends RequestContext<? extends DatamartRequest>, AsyncResult<QueryResult>>> services) {
        for (DatamartExecutionService<? extends RequestContext<? extends DatamartRequest>, AsyncResult<QueryResult>> es : services) {
            serviceMap.put(es.getSqlProcessingType(), (DatamartExecutionService<RequestContext<? extends DatamartRequest>, AsyncResult<QueryResult>>) es);
        }
    }

    @Override
    public void dispatch(RequestContext<?> context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        try {
            serviceMap.get(context.getProcessingType())
                    .execute(context, asyncResultHandler);
        } catch (Exception e) {
            log.error("Произошла ошибка при диспетчеризации запроса", e);
            asyncResultHandler.handle(Future.failedFuture(e));
        }
    }
}
