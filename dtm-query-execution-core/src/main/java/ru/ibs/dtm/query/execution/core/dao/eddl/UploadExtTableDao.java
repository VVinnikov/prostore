package ru.ibs.dtm.query.execution.core.dao.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;

public interface UploadExtTableDao {

    void findUploadExternalTable(String schemaName, String tableName, Handler<AsyncResult<UploadExtTableRecord>> resultHandler);

    void insertUploadExternalTable(CreateUploadExternalTableQuery query, Handler<AsyncResult<Void>> asyncResultHandler);

    void dropUploadExternalTable(DropUploadExternalTableQuery query, Handler<AsyncResult<Void>> asyncResultHandler);

    void dropUploadExternalTableById(Long uploadExtTableId, Handler<AsyncResult<Void>> resultHandler);
}
