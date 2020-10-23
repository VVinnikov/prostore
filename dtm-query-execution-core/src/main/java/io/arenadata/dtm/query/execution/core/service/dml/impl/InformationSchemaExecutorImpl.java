package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class InformationSchemaExecutorImpl implements InformationSchemaExecutor {

    private final ServiceDbFacade serviceDbFacade;

    @Autowired
    public InformationSchemaExecutorImpl(ServiceDbFacade serviceDbFacade) {
        this.serviceDbFacade = serviceDbFacade;
    }

    @Override
    public void execute(QuerySourceRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        //FIXME implement receiving information schema
        asyncResultHandler.handle(Future.failedFuture("Need to implement receiving information schema"));
    }
}
