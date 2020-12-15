package io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartEntity;
import io.vertx.core.Future;

import java.util.List;

public interface EntityDao extends ZookeeperDao<Entity> {
    void getEntitiesMeta(String datamartMnemonic, AsyncHandler<List<DatamartEntity>> resultHandler);

    Future<Void> createEntity(Entity entity);

    Future<Void> updateEntity(Entity entity);

    Future<Boolean> existsEntity(String datamartMnemonic, String entityName);

    Future<Void> deleteEntity(String datamartMnemonic, String entityName);

    Future<Entity> getEntity(String datamartMnemonic, String entityName);

    Future<List<String>> getEntityNamesByDatamart(String datamartMnemonic);
}
