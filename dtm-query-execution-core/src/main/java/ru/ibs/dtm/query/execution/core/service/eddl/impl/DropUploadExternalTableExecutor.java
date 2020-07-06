package ru.ibs.dtm.query.execution.core.service.eddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlAction;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;
import ru.ibs.dtm.query.execution.core.service.eddl.EddlExecutor;

@Component
public class DropUploadExternalTableExecutor implements EddlExecutor {

    private final ServiceDao serviceDao;

    @Autowired
    public DropUploadExternalTableExecutor(ServiceDao serviceDao) {
        this.serviceDao = serviceDao;
    }

    @Override
    public void execute(EddlQuery query, Handler<AsyncResult<Void>> asyncResultHandler) {
        DropUploadExternalTableQuery castQuery;
        try {
            castQuery = (DropUploadExternalTableQuery) query;
        } catch (Exception e) {
            asyncResultHandler.handle(Future.failedFuture(e));
            return;
        }
        executeInternal(castQuery, asyncResultHandler);
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.DROP_UPLOAD_EXTERNAL_TABLE;
    }

    private void executeInternal(DropUploadExternalTableQuery query, Handler<AsyncResult<Void>> asyncResultHandler) {
        serviceDao.dropUploadExternalTable(query, asyncResultHandler);
    }
}
