package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.service.DatabaseSynchronizeService;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

@Slf4j
@Component
public class CreateTableDdlExecutor extends QueryResultDdlExecutor {
    private final DatabaseSynchronizeService databaseSynchronizeService;

    @Autowired
    public CreateTableDdlExecutor(MetadataFactory<DdlRequestContext> metadataFactory,
                                  DatabaseSynchronizeService databaseSynchronizeService,
                                  MariaProperties mariaProperties,
                                  ServiceDbFacade serviceDbFacade) {
        super(metadataFactory, mariaProperties, serviceDbFacade);
        this.databaseSynchronizeService = databaseSynchronizeService;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        String schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        QueryRequest request = context.getRequest().getQueryRequest();
        request.setDatamartMnemonic(schema);

        String tableWithSchema = SqlPreparer.getTableWithSchema(mariaProperties.getOptions().getDatabase(), sqlNodeName);
        String sql = getSql(context, sqlNodeName);
        serviceDbFacade.getDdlServiceDao().executeUpdate(sql, ar2 -> {
            if (ar2.succeeded()) {
                databaseSynchronizeService.putForRefresh(
                        context,
                        tableWithSchema,
                        true, ar3 -> {
                            if (ar3.succeeded()) {
                                handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                            } else {
                                log.error("Ошибка синхронизации {}", tableWithSchema, ar3.cause());
                                handler.handle(Future.failedFuture(ar3.cause()));
                            }
                        });
            } else {
                log.error("Ошибка исполнения запроса {}", sql, ar2.cause());
                handler.handle(Future.failedFuture(ar2.cause()));
            }
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }
}
