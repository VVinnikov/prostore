package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.eddl.DropDatabase;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.cache.impl.HotDeltaCacheService;
import io.arenadata.dtm.query.execution.core.service.cache.impl.OkDeltaCacheService;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaDdlExecutor extends QueryResultDdlExecutor {
    private final HotDeltaCacheService hotDeltaCacheService;
    private final OkDeltaCacheService okDeltaCacheService;
    private final EntityCacheService entityCacheService;
    private final DatamartDao datamartDao;

    @Autowired
    public DropSchemaDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                 HotDeltaCacheService hotDeltaCacheService,
                                 OkDeltaCacheService okDeltaCacheService,
                                 EntityCacheService entityCacheService,
                                 ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        this.hotDeltaCacheService = hotDeltaCacheService;
        this.okDeltaCacheService = okDeltaCacheService;
        this.entityCacheService = entityCacheService;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return dropSchema(context, sqlNodeName);
    }

    private Future<QueryResult> dropSchema(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            String schemaName = ((DropDatabase) context.getQuery()).getName().names.get(0);
            if (InformationSchemaView.SCHEMA_NAME.equalsIgnoreCase(schemaName)) {
                promise.fail(new DtmException("Removing system databases is impossible"));
            } else {
                clearCacheByDatamartName(schemaName);
                context.getRequest().getQueryRequest().setDatamartMnemonic(schemaName);
                context.setDatamartName(schemaName);
                datamartDao.existsDatamart(schemaName)
                        .compose(isExists -> isExists
                                ? dropDatamartInPlugins(context)
                                : getNotExistsDatamartFuture(schemaName))
                        .compose(r -> dropDatamart(context))
                        .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                        .onFailure(promise::fail);
            }
        });
    }

    private void clearCacheByDatamartName(String schemaName) {
        entityCacheService.removeIf(ek -> ek.getDatamartName().equals(schemaName));
        hotDeltaCacheService.remove(schemaName);
        okDeltaCacheService.remove(schemaName);
    }

    private Future<Void> getNotExistsDatamartFuture(String schemaName) {
        return Future.failedFuture(new DatamartNotExistsException(schemaName));
    }

    private Future<Void> dropDatamartInPlugins(DdlRequestContext context) {
        try {
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(DROP_SCHEMA);
            log.debug("Delete physical objects in plugins for datamart: [{}]", context.getDatamartName());
            return metadataExecutor.execute(context);
        } catch (Exception e) {
            return Future.failedFuture(new DtmException("Error generating drop datamart request", e));
        }
    }

    private Future<Void> dropDatamart(DdlRequestContext context) {
        log.debug("Delete schema [{}] in data sources", context.getDatamartName());
        return datamartDao.deleteDatamart(context.getDatamartName())
                .onSuccess(success -> log.debug("Deleted datamart [{}] from datamart registry",
                        context.getDatamartName()));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }
}
