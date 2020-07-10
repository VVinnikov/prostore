package ru.ibs.dtm.query.execution.core.dao.servicedb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.query.execution.core.dto.metadata.EntityAttribute;

import java.util.List;

public interface AttributeDao {

    void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler);

    void insertAttribute(Long entityId, ClassField field, Integer typeId, Handler<AsyncResult<Void>> resultHandler);

    void dropAttribute(Long entityId, Handler<AsyncResult<Void>> resultHandler);
}
