package ru.ibs.dtm.query.execution.core.service.eddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlAction;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;
import ru.ibs.dtm.query.execution.core.service.eddl.EddlExecutor;

@Slf4j
@Component
public class DropDownloadExternalTableExecutor implements EddlExecutor {

    private final EntityDao entityDao;

    @Autowired
    public DropDownloadExternalTableExecutor(ServiceDbFacade serviceDbFacade) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(EddlQuery query, Handler<AsyncResult<Void>> handler) {
        try {
            DropDownloadExternalTableQuery castQuery = (DropDownloadExternalTableQuery) query;
            val datamartName = castQuery.getSchemaName();
            val entityName = castQuery.getTableName();
            dropTable(datamartName, entityName)
                    .onSuccess(r -> handler.handle(Future.succeededFuture()))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error deleting table!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.DROP_DOWNLOAD_EXTERNAL_TABLE;
    }

    protected Future<Void> dropTable(String datamartName, String entityName) {
        return getEntity(datamartName, entityName)
                .compose(entity -> dropEntityIfExists(entity));
    }

    private Future<Entity> getEntity(String datamartName, String entityName) {
        return Future.future(entityPromise -> {
            entityDao.getEntity(datamartName, entityName)
                    .onSuccess(entity -> {
                        if (EntityType.DOWNLOAD_EXTERNAL_TABLE == entity.getEntityType()) {
                            entityPromise.complete(entity);
                        } else {
                            val errMsg = String.format("Table [%s] in datamart [%s] doesn't exist!", entityName, datamartName);
                            log.error(errMsg);
                            entityPromise.fail(errMsg);
                        }
                    })
                    .onFailure(error -> {
                        log.error("Table [{}] in datamart [{}] doesn't exist!", entityName, datamartName, error);
                        entityPromise.fail(error);
                    });
        });
    }

    private Future<Void> dropEntityIfExists(Entity entity) {
        if (entity != null) {
            return entityDao.deleteEntity(entity.getSchema(), entity.getName());
        } else {
            return Future.succeededFuture();
        }
    }
}
