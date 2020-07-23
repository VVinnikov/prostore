package ru.ibs.dtm.query.execution.core.dao.servicedb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.dto.DatamartInfo;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;

import java.util.List;

public interface EntityDao {

    void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler);

    void insertEntity(Long datamartId, String name, Handler<AsyncResult<Void>> resultHandler);

    void findEntity(Long datamartId, String name, Handler<AsyncResult<Long>> resultHandler);

    void isEntityExists(Long datamartId, String name, Handler<AsyncResult<Boolean>> resultHandler);

    void dropEntity(Long datamartId, String name, Handler<AsyncResult<Integer>> resultHandler);

    void findEntitiesByDatamartAndTableNames(DatamartInfo datamartInfo, Handler<AsyncResult<List<DatamartEntity>>> resultHandler);
}
