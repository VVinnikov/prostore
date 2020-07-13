package ru.ibs.dtm.query.execution.core.dao.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadQueryRecord;

public interface DownloadQueryDao {

    void insertDownloadQuery(DownloadQueryRecord downloadQueryRecord, Handler<AsyncResult<Void>> resultHandler);
}
