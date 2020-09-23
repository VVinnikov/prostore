package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType;

@Slf4j
@Component
public class DropTableDdlExecutor extends QueryResultDdlExecutor {

    @Autowired
    public DropTableDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                MariaProperties mariaProperties,
                                ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            String schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
            String tableName = getTableName(sqlNodeName);
            Entity entity = createClassTable(schema, tableName);
            context.getRequest().setClassTable(entity);
            context.setDatamartName(schema);
            context.setDdlType(DdlType.DROP_TABLE);
            dropTable(context, containsIfExistsCheck(context.getRequest().getQueryRequest().getSql()))
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        }
                    })
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error deleting table!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    @NotNull
    private Entity createClassTable(String schema, String tableName) {
        return new Entity(getTableNameWithSchema(schema, tableName), null);
    }

    protected Future<Void> dropTable(DdlRequestContext context, boolean ifExists) {
        return getDatamart(context)
                .compose(datamartId -> getEntity(context, ifExists, datamartId))
                .compose(entityId -> dropEntityIfExists(context, entityId));
    }

    private boolean containsIfExistsCheck(String sql) {
        return sql.toLowerCase().contains("if exists");
    }

    private Future<Long> getDatamart(DdlRequestContext context) {
        return Future.future((Promise<Long> promise) ->
                serviceDbFacade.getServiceDbDao().getDatamartDao()
                        .findDatamart(context.getDatamartName(), promise));
    }

    private Future<Long> getEntity(DdlRequestContext context, boolean ifExists, Long datamartId) {
        return Future.future((Promise<Long> entityPromise) -> {
            context.setDatamartId(datamartId);
            serviceDbFacade.getServiceDbDao().getEntityDao().findEntity(context.getDatamartId(),
                    context.getRequest().getClassTable().getName(), ar -> {
                        if (ar.succeeded()) {
                            entityPromise.complete(ar.result());
                        } else {
                            if (ifExists) {
                                entityPromise.complete(null);
                            } else {
                                log.error("Table [{}] in datamart [{}] doesn't exist!",
                                        context.getRequest().getClassTable().getName(),
                                        context.getDatamartName(), ar.cause());
                                entityPromise.fail(ar.cause());
                            }
                        }
                    });
        });
    }

    private Future<Void> dropEntityIfExists(DdlRequestContext context, Long entityId) {
        if (entityId != null) {
            return Future.future((Promise<Void> metaPromise) -> metadataExecutor.execute(context, ar -> {
                if (ar.succeeded()) {
                    metaPromise.complete();
                } else {
                    log.error("Error deleting table [{}], datamart [{}] in datasources!",
                            context.getRequest().getClassTable().getName(),
                            context.getDatamartName(), ar.cause());
                    metaPromise.fail(ar.cause());
                }
            })).compose(r -> dropEntityWithAttributes(context, entityId));
        } else {
            return Future.future(Promise::complete);
        }
    }

    private Future<Void> dropEntityWithAttributes(DdlRequestContext context, Long entityId) {
        return Future.future((Promise<Void> attrPromise) ->
                serviceDbFacade.getServiceDbDao().getAttributeDao().dropAttribute(entityId, ar -> {
                    if (ar.succeeded()) {
                        attrPromise.complete();
                    } else {
                        log.error("Error deleting attributes for table [{}] in datamart [{}]!",
                                context.getRequest().getClassTable().getName(),
                                context.getDatamartName(), ar.cause());
                        attrPromise.fail(ar.cause());
                    }
                }))
                .compose(result -> dropEntity(context));
    }

    private Future<Void> dropEntity(DdlRequestContext context) {
        return Future.future((Promise<Void> promise) -> {
            serviceDbFacade.getServiceDbDao().getEntityDao().dropEntity(context.getDatamartId(),
                    context.getRequest().getClassTable().getName(), ar -> {
                        if (ar.succeeded()) {
                            log.debug("Table [{}] in datamart [{}] deleted successfully",
                                    context.getRequest().getClassTable().getName(),
                                    context.getDatamartName());
                            promise.complete();
                        } else {
                            log.error("Error deleting table entity [{}] for datamart [{}]!",
                                    context.getRequest().getClassTable().getName(),
                                    context.getDatamartName(), ar.cause());
                            promise.fail(ar.cause());
                        }
                    });
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }
}
