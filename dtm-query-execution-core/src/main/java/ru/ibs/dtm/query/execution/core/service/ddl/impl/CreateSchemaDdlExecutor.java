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
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.CREATE_SCHEMA;

@Slf4j
@Component
public class CreateSchemaDdlExecutor extends QueryResultDdlExecutor {
    @Autowired
    public CreateSchemaDdlExecutor(MetadataFactory<DdlRequestContext> metadataFactory,
                                   MariaProperties mariaProperties,
                                   ServiceDbFacade serviceDbFacade) {
        super(metadataFactory, mariaProperties, serviceDbFacade);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
        context.setDdlType(CREATE_SCHEMA);
        metadataFactory.apply(context, result -> {
            if (result.succeeded()) {
                createDatamart(sqlNodeName, handler);
            } else {
                handler.handle(Future.failedFuture(result.cause()));
            }
        });
    }

    private void createDatamart(String datamartName, Handler<AsyncResult<QueryResult>> handler) {
        serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(datamartName, datamartResult -> {
            if (datamartResult.succeeded()) {
                log.error("База данных {} уже существует", datamartName);
                handler.handle(Future.failedFuture(String.format("База данных [%s] уже существует", datamartName)));
            } else {
                serviceDbFacade.getServiceDbDao().getDatamartDao().insertDatamart(datamartName, insertResult -> {
                    if (insertResult.succeeded()) {
                        log.debug("Создана новая витрина {}", datamartName);
                        handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                    } else {
                        log.error("Ошибка при создании витрины {}", datamartName, insertResult.cause());
                        handler.handle(Future.failedFuture(insertResult.cause()));
                    }
                });
            }
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }
}
