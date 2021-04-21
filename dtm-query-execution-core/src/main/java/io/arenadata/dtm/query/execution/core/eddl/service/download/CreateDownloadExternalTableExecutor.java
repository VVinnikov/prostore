package io.arenadata.dtm.query.execution.core.eddl.service.download;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.edml.configuration.EdmlProperties;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.eddl.dto.CreateDownloadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlAction;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlQuery;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.eddl.service.EddlExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CreateDownloadExternalTableExecutor implements EddlExecutor {

    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final EdmlProperties edmlProperties;

    @Autowired
    public CreateDownloadExternalTableExecutor(ServiceDbFacade serviceDbFacade, EdmlProperties edmlProperties) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.edmlProperties = edmlProperties;
    }

    @Override
    public Future<QueryResult> execute(EddlQuery query) {
        return Future.future(promise -> {
            CreateDownloadExternalTableQuery castQuery = (CreateDownloadExternalTableQuery) query;
            val schema = castQuery.getSchemaName();
            val entity = castQuery.getEntity();
            entity.setExternalTableLocationType(ExternalTableLocationType.valueOf(castQuery.getLocationType().getName().toUpperCase()));
            entity.setExternalTableLocationPath(castQuery.getLocationPath());
            entity.setExternalTableFormat(castQuery.getFormat());
            entity.setExternalTableSchema(castQuery.getTableSchema());
            entity.setExternalTableDownloadChunkSize(getChunkSize(castQuery));
            datamartDao.existsDatamart(schema)
                    .compose(isExistsDatamart -> isExistsDatamart ?
                            entityDao.createEntity(entity)
                                    .onSuccess(ar -> log.debug("Table [{}] in datamart [{}] successfully created",
                                            entity.getName(),
                                            entity.getSchema())) : Future.failedFuture(new DatamartNotExistsException(schema)))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    private Integer getChunkSize(CreateDownloadExternalTableQuery castQuery) {
        return castQuery.getChunkSize() == null ? edmlProperties.getDefaultChunkSize() : castQuery.getChunkSize();
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE;
    }
}