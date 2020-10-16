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
        //FIXME implement receiving information schema
        asyncResultHandler.handle(Future.failedFuture("Need to implement receiving information schema"));
    }
}
