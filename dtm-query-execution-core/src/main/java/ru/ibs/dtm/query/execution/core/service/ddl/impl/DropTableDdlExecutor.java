package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.service.DatabaseSynchronizeService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

@Slf4j
@Component
public class DropTableDdlExecutor extends QueryResultDdlExecutor {
    protected final DatabaseSynchronizeService databaseSynchronizeService;

    @Autowired
    public DropTableDdlExecutor(MetadataFactory<DdlRequestContext> metadataFactory,
                                DatabaseSynchronizeService databaseSynchronizeService,
                                MariaProperties mariaProperties,
                                ServiceDbFacade serviceDbFacade) {
        super(metadataFactory, mariaProperties, serviceDbFacade);
        this.databaseSynchronizeService = databaseSynchronizeService;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        String schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        String table = getTableName(sqlNodeName);
        context.getRequest().getQueryRequest().setDatamartMnemonic(schema);
        dropTable(context, table, containsIfExistsCheck(context.getRequest().getQueryRequest().getSql()), handler);
    }

    private boolean containsIfExistsCheck(String sql) {
        return sql.toLowerCase().contains("if exists");
    }

    protected void dropTable(DdlRequestContext context, String tableName, boolean ifExists, Handler<AsyncResult<QueryResult>> handler) {
        serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic(), datamartResult -> {
            if (datamartResult.succeeded()) {
                serviceDbFacade.getServiceDbDao().getEntityDao().findEntity(datamartResult.result(), tableName, entityResult -> {
                    if (entityResult.succeeded()) {
                        databaseSynchronizeService.removeTable(context, datamartResult.result(), tableName, removeResult -> {
                            if (removeResult.succeeded()) {
                                handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                            } else {
                                handler.handle(Future.failedFuture(removeResult.cause()));
                            }
                        });
                    } else {
                        if (ifExists) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                            return;
                        }
                        final String msg = "Логической таблицы " + tableName + " не существует (не найдена сущность)";
                        log.error(msg);
                        handler.handle(Future.failedFuture(msg));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(datamartResult.cause()));
            }
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }
}
