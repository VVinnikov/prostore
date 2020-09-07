package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.extension.eddl.DropDatabase;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.DROP_SCHEMA;

@Slf4j
@Component
public class DropSchemaDdlExecutor extends DropTableDdlExecutor {

    @Autowired
    public DropSchemaDdlExecutor(
            MetadataExecutor<DdlRequestContext> metadataExecutor,
            MariaProperties mariaProperties,
            ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            String schemaName = ((DropDatabase) context.getQuery()).getName().names.get(0);
            context.getRequest().getQueryRequest().setDatamartMnemonic(schemaName);
            context.setDatamartName(schemaName);
            getDatamart(context)
                    .compose(datamartId -> dropSchema(context, datamartId))
                    .compose(r -> dropDatamart(context))
                    .compose(r -> dropEntities(context))
                    .compose(r -> dropViews(context))
                    .compose(r -> dropDeltas(context))
                    .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error deleting datamart!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Long> getDatamart(DdlRequestContext context) {
        return Future.future((Promise<Long> promise) ->
                serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(context.getDatamartName(), ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        log.error("Error finding datamart [{}]!", context.getDatamartName(), ar.cause());
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<Void> dropDatamart(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) -> {
            log.debug("Deleted schema [{}] in data sources", context.getDatamartName());
            serviceDbFacade.getServiceDbDao().getDatamartDao().dropDatamart(context.getDatamartId(), ar -> {
                if (ar.succeeded()) {
                    log.debug("Deleted datamart [{}] from datamart registry", context.getDatamartName());
                    promise.complete();
                } else {
                    log.error("Error deleting datamart [{}] from datamart registry!", context.getDatamartName(), ar.cause());
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private Future<Void> dropSchema(DdlRequestContext context, Long datamartId) {
        try {
            context.setDatamartId(datamartId);
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(DROP_SCHEMA);
            log.debug("Delete physical objects in plugins for datamart: [{}]", context.getDatamartName());
            return Future.future((Promise<Void> promise) -> metadataExecutor.execute(context, promise));
        } catch (Exception e) {
            log.error("Error in dropping schema [{}]", context.getDatamartName(), e);
            return Future.failedFuture(e);
        }
    }

    private Future<Void> dropEntities(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) ->
                serviceDbFacade.getServiceDbDao().getEntityDao().dropByDatamartId(context.getDatamartId(), ar -> {
                    if (ar.succeeded()) {
                        log.debug("Deleted entities in datamart [{}]", context.getDatamartName());
                        promise.complete();
                    } else {
                        log.error("Error deleting entities in datamart [{}]!", context.getDatamartName(), ar.cause());
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<Void> dropViews(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) ->
                serviceDbFacade.getServiceDbDao().getViewServiceDao().dropByDatamartId(context.getDatamartId(), ar -> {
                    if (ar.succeeded()) {
                        log.debug("Deleted views in datamart [{}]", context.getDatamartName());
                        promise.complete();
                    } else {
                        log.error("Error deleting views in datamart [{}]!", context.getDatamartName(), ar.cause());
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<Void> dropDeltas(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) ->
                serviceDbFacade.getDeltaServiceDao().dropByDatamart(context.getDatamartName(), ar -> {
                    if (ar.succeeded()) {
                        log.debug("Deltas for datamart [{}] deleted successfully", context.getDatamartName());
                        promise.complete(ar.result());
                    } else {
                        log.error("Error deleting delta by datamart [{}]!", context.getDatamartName(), ar.cause());
                        promise.fail(ar.cause());
                    }
                }));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }
}
