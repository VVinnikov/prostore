package io.arenadata.dtm.query.execution.core.eddl.service.download;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.eddl.dto.DropDownloadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlAction;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlQuery;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.eddl.service.EddlExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DropDownloadExternalTableExecutor implements EddlExecutor {

    private final EntityDao entityDao;

    @Autowired
    public DropDownloadExternalTableExecutor(ServiceDbFacade serviceDbFacade) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<QueryResult> execute(EddlQuery query) {
        return Future.future(promise -> {
            DropDownloadExternalTableQuery castQuery = (DropDownloadExternalTableQuery) query;
            val datamartName = castQuery.getSchemaName();
            val entityName = castQuery.getTableName();
            dropTable(datamartName, entityName)
                    .onSuccess(r -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    protected Future<Void> dropTable(String datamartName, String entityName) {
        return getEntity(datamartName, entityName)
                .compose(this::dropEntityIfExists);
    }

    private Future<Entity> getEntity(String datamartName, String entityName) {
        return Future.future(entityPromise -> {
            val tableWithSchema = datamartName + "." + entityName;
            entityDao.getEntity(datamartName, entityName)
                    .onSuccess(entity -> {
                        if (EntityType.DOWNLOAD_EXTERNAL_TABLE == entity.getEntityType()) {
                            entityPromise.complete(entity);
                        } else {
                            entityPromise.fail(new EntityNotExistsException(tableWithSchema));
                        }
                    })
                    .onFailure(entityPromise::fail);
        });
    }

    private Future<Void> dropEntityIfExists(Entity entity) {
        if (entity != null) {
            return entityDao.deleteEntity(entity.getSchema(), entity.getName());
        } else {
            return Future.succeededFuture();
        }
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.DROP_DOWNLOAD_EXTERNAL_TABLE;
    }
}