package ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;

import java.util.List;

public interface EntityDao<T> extends ZkDao<T> {
    void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler);

    Future<Void> createEntity(Entity entity);

    Future<Void> updateEntity(Entity entity);

    Future<Void> deleteEntity(String datamartMnemonic, String entityName);

    Future<Entity> getEntity(String datamartMnemonic, String entityName);

    Future<List<String>> getEntityNamesByDatamart(String datamartMnemonic);
}
