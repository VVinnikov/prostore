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
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.CREATE_SCHEMA;

@Slf4j
@Component
public class CreateSchemaDdlExecutor extends QueryResultDdlExecutor {

    @Autowired
    public CreateSchemaDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                   MariaProperties mariaProperties,
                                   ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(CREATE_SCHEMA);
            metadataExecutor.execute(context, ar -> {
                if (ar.succeeded()) {
                    createDatamart(context, cr -> {
                        if (cr.succeeded()) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        } else {
                            log.error("Error creating datamart [{}]", context.getDatamartName(), cr.cause());
                            handler.handle(Future.failedFuture(cr.cause()));
                        }
                    });
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } catch (Exception e) {
            log.error("Error in creating datamart!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private void createDatamart(DdlRequestContext context, Handler<AsyncResult<Void>> resultHandler) {
        serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(context.getDatamartName(), datamartResult -> {
            if (datamartResult.succeeded()) {
                resultHandler.handle(Future.failedFuture(new RuntimeException(
                        String.format("Datamart [%s] is already exists", context.getDatamartName()))));
            } else {
                serviceDbFacade.getServiceDbDao().getDatamartDao().insertDatamart(context.getDatamartName(), insertResult -> {
                    if (insertResult.succeeded()) {
                        log.debug("Datamart [{}] successfully created", context.getDatamartName());
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(insertResult.cause()));
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
