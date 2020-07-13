package ru.ibs.dtm.query.execution.core.dao.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadQueryRecord;

public interface UploadQueryDao {

    void inserUploadQuery(UploadQueryRecord uploadQueryRecord, Handler<AsyncResult<Void>> resultHandler);
}
