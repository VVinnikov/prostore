package ru.ibs.dtm.query.execution.core.dao.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;

public interface DownloadExtTableDao {

    void insertDownloadExternalTable(CreateDownloadExternalTableQuery downloadExternalTableQuery, Handler<AsyncResult<Void>> resultHandler);

    void dropDownloadExternalTable(String datamart, String tableName, Handler<AsyncResult<Void>> resultHandler);

    void findDownloadExternalTable(String datamartMnemonic, String table, Handler<AsyncResult<DownloadExtTableRecord>> resultHandler);
}
