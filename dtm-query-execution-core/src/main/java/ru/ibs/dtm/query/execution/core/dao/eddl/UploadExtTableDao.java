package ru.ibs.dtm.query.execution.core.dao.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;

public interface UploadExtTableDao {

    void findUploadExternalTable(String schemaName, String tableName, Handler<AsyncResult<UploadExtTableRecord>> resultHandler);

}
