package ru.ibs.dtm.query.execution.core.dao.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExtTableRecord;

public interface DownloadExtTableDao {

    void findDownloadExternalTable(String datamartMnemonic, String table, Handler<AsyncResult<DownloadExtTableRecord>> resultHandler);

}
