package ru.ibs.dtm.query.execution.core.service.dml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import ru.ibs.dtm.query.execution.core.utils.MetaDataQueryPreparer;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;

@Service
public class InformationSchemaExecutorImpl implements InformationSchemaExecutor {

    private final ServiceDbFacade serviceDbFacade;

    @Autowired
    public InformationSchemaExecutorImpl(ServiceDbFacade serviceDbFacade) {
        this.serviceDbFacade = serviceDbFacade;
    }

    @Override
    public void execute(QuerySourceRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        serviceDbFacade.getDdlServiceDao().executeQuery(
                SqlPreparer.replaceQuote(MetaDataQueryPreparer.modify(request.getQueryRequest().getSql())),
                request.getMetadata(), ar -> {
                    if (ar.succeeded()) {
                        QueryResult result = new QueryResult(request.getQueryRequest().getRequestId(),
                                ar.result(), request.getMetadata());
                        asyncResultHandler.handle(Future.succeededFuture(result));
                    } else {
                        asyncResultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
