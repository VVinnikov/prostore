package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.exception.EntityNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType;

@Slf4j
@Component
public class DropTableDdlExecutor extends QueryResultDdlExecutor {

    private final EntityDao entityDao;

    @Autowired
    public DropTableDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            String schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
            String tableName = getTableName(sqlNodeName);
            Entity entity = createClassTable(schema, tableName);
            context.getRequest().setEntity(entity);
            context.setDatamartName(schema);
            context.setDdlType(DdlType.DROP_TABLE);
            dropTable(context, containsIfExistsCheck(context.getRequest().getQueryRequest().getSql()))
                .onSuccess(r -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
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
        return getEntity(context, ifExists)
            .compose(entity -> dropEntityIfExists(context, entity));
    }

    private boolean containsIfExistsCheck(String sql) {
        return sql.toLowerCase().contains("if exists");
    }

    private Future<Entity> getEntity(DdlRequestContext context, boolean ifExists) {
        return Future.future(entityPromise -> {
            val entityName = context.getRequest().getEntity().getName();
            val datamartName = context.getDatamartName();
            entityDao.getEntity(datamartName, entityName)
                .onSuccess(entity -> {
                    if (EntityType.TABLE == entity.getEntityType()) {
                        entityPromise.complete(entity);
                    } else {
                        val errMsg = String.format("Table [%s] in datamart [%s] doesn't exist!", entityName, datamartName);
                        log.error(errMsg);
                        entityPromise.fail(errMsg);
                    }
                })
                .onFailure(error -> {
                    if (error instanceof EntityNotExistsException && ifExists) {
                        entityPromise.complete(null);
                    } else {
                        log.error("Table [{}] in datamart [{}] doesn't exist!",
                            entityName,
                            datamartName, error);
                        entityPromise.fail(error);
                    }
                });
        });
    }

    private Future<Void> dropEntityIfExists(DdlRequestContext context, Entity entity) {
        if (entity != null) {
            return dropEntityFromPlugins(context)
                .compose(r -> entityDao.deleteEntity(context.getDatamartName(), entity.getName()));
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> dropEntityFromPlugins(DdlRequestContext context) {
        return Future.future((Promise<Void> metaPromise) -> metadataExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                metaPromise.complete();
            } else {
                log.error("Error deleting table [{}], datamart [{}] in datasources!",
                    context.getRequest().getEntity().getName(),
                    context.getDatamartName(), ar.cause());
                metaPromise.fail(ar.cause());
            }
        }));
    }


    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }
}
