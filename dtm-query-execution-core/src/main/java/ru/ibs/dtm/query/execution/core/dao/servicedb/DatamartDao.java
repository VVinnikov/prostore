package ru.ibs.dtm.query.execution.core.dao.servicedb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartInfo;

import java.util.List;

public interface DatamartDao {

    void insertDatamart(String name, Handler<AsyncResult<Void>> resultHandler);

    void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler);

    void findDatamart(String name, Handler<AsyncResult<Long>> resultHandler);

    void dropDatamart(Long id, Handler<AsyncResult<Void>> resultHandler);
}
