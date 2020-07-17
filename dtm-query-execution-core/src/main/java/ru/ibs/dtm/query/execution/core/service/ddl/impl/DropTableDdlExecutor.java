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
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

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
            String table = getTableName(sqlNodeName);
            context.getRequest().getQueryRequest().setDatamartMnemonic(schema);
            context.setTableName(table);
            dropTable(context, containsIfExistsCheck(context.getRequest().getQueryRequest().getSql()))
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        } else {
                            log.error("Error deleting datamart table [{}], datamart [{}]",
                                    context.getClassTable().getName(), context.getDatamartName());
                            handler.handle(Future.failedFuture(ar.cause()));//TODO проверить
                        }
                    })
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error in deleting table!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private boolean containsIfExistsCheck(String sql) {
        //FIXME проверять на уровне SqlNode
        return sql.toLowerCase().contains("if exists");
    }

    protected Future<Void> dropTable(DdlRequestContext context, boolean ifExists) {
        return getDatamart(context)
                .compose(datamartId -> getEntity(context, ifExists, datamartId))
                .compose(entityId -> dropEntityIfExists(context, entityId));
    }

    private Future<Long> getDatamart(DdlRequestContext context) {
        return Future.future((Promise<Long> promise) ->
                serviceDbFacade.getServiceDbDao().getDatamartDao()
                        .findDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic(), promise));
    }

    private Future<Long> getEntity(DdlRequestContext context, boolean ifExists, Long datamartId) {
        return Future.future((Promise<Long> entityPromise) -> {
            context.setDatamartId(datamartId);
            serviceDbFacade.getServiceDbDao().getEntityDao().findEntity(datamartId, context.getTableName(), ar -> {
                if (ar.succeeded()) {
                    entityPromise.complete(ar.result());
                } else {
                    //TODO проверить, будет ли ошибка при ненахождении таблицы
                    if (ifExists) {
                        entityPromise.complete(null);
                    } else {
                        log.error("Logical table [{}] doesn't exist!", context.getTableName());
                        entityPromise.fail(ar.cause());
                    }
                }
            });
        });
    }

    private Future<Void> dropEntityIfExists(DdlRequestContext context, Long entityId) {
        Promise promise = Promise.promise();
        if (entityId != null) {
            //FIXME убедиться что context содержит нужные параметры
            return Future.future((Promise<Void> metaPromise) -> metadataExecutor.execute(context, metaPromise))
                    .onComplete(result -> serviceDbFacade.getServiceDbDao().getEntityDao().dropEntity(context.getDatamartId(),
                            context.getTableName())
                            .onComplete(ar -> {
                                if (ar.succeeded()) {
                                    log.debug("Deleted logical table [{}]", context.getTableName());
                                    promise.complete();
                                } else {
                                    promise.fail(ar.cause());
                                }
                            })
                            .onFailure(promise::fail))
                    //FIXME проверить на тестах, что нет лишней обработки исключений
                    .onFailure(promise::fail);
        } else {
            return Future.future((Promise<Void> pr) -> promise.complete());
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }
}
