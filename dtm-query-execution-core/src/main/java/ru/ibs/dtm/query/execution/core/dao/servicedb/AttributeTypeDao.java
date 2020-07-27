package ru.ibs.dtm.query.execution.core.dao.servicedb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface AttributeTypeDao {

    void findTypeIdByTypeMnemonic(String datamartMnemonic, Handler<AsyncResult<Integer>> resultHandler);
}
