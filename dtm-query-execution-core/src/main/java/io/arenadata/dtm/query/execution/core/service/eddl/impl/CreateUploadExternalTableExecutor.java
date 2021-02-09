package io.arenadata.dtm.query.execution.core.service.eddl.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.eddl.CreateUploadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlAction;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.arenadata.dtm.query.execution.core.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.exception.table.ExternalTableAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CreateUploadExternalTableExecutor implements EddlExecutor {

    private final DatamartDao datamartDao;
    private final EntityDao entityDao;

    @Autowired
    public CreateUploadExternalTableExecutor(ServiceDbFacade serviceDbFacade) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<QueryResult> execute(EddlQuery query) {
        return Future.future(promise -> {
            CreateUploadExternalTableQuery castQuery = (CreateUploadExternalTableQuery) query;
            val schema = castQuery.getSchemaName();
            val entity = castQuery.getEntity();
            entity.setExternalTableLocationType(ExternalTableLocationType.valueOf(castQuery.getLocationType().getName().toUpperCase()));
            entity.setExternalTableLocationPath(castQuery.getLocationPath());
            entity.setExternalTableFormat(castQuery.getFormat().getName());
            entity.setExternalTableSchema(castQuery.getTableSchema());
            entity.setExternalTableUploadMessageLimit(castQuery.getMessageLimit());
            datamartDao.existsDatamart(schema)
                    .compose(isExistsDatamart -> isExistsDatamart ?
                            entityDao.existsEntity(schema, entity.getName()) : Future.failedFuture(new DatamartNotExistsException(schema)))
                    .onSuccess(isExistsEntity -> createTableIfNotExists(entity, isExistsEntity)
                            .onSuccess(success -> {
                                promise.complete(QueryResult.emptyResult());
                            })
                            .onFailure(promise::fail))
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> createTableIfNotExists(Entity entity, Boolean isTableExists) {
        if (isTableExists) {
            return Future.failedFuture(new ExternalTableAlreadyExistsException(entity.getNameWithSchema()));
        } else {
            return createTable(entity);
        }
    }

    private Future<Void> createTable(Entity entity) {
        return entityDao.createEntity(entity)
                .onSuccess(ar2 -> log.debug("Table [{}] in datamart [{}] successfully created",
                        entity.getName(),
                        entity.getSchema()));
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.CREATE_UPLOAD_EXTERNAL_TABLE;
    }
}
