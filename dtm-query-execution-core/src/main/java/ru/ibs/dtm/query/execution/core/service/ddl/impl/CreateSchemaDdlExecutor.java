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
import ru.ibs.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
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
            String schemaName = ((SqlCreateDatabase) context.getQuery()).getName().names.get(0);
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(CREATE_SCHEMA);
            context.setDatamartName(schemaName);
            createDatamartIfNotExists(context, handler);
        } catch (Exception e) {
            log.error("Error creating datamart!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private void createDatamartIfNotExists(DdlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        serviceDbFacade.getServiceDbDao().getDatamartDao().isDatamartExists(context.getDatamartName(), isExists -> {
            if (isExists.succeeded()) {
                if (isExists.result()) {
                    final RuntimeException existsException = new RuntimeException(
                            String.format("Datamart [%s] is already exists!", context.getDatamartName()));
                    log.error("Error creating datamart [{}]!", context.getDatamartName(), existsException);
                    resultHandler.handle(Future.failedFuture(existsException));
                } else {
                    createDatamart(context, resultHandler);
                }
            } else {
                log.error("Error receive isExists status for datamart [{}]!", context.getDatamartName(), isExists.cause());
                resultHandler.handle(Future.failedFuture(isExists.cause()));
            }
        });
    }

    private void createDatamart(DdlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        Future.future((Promise<Void> promise) -> metadataExecutor.execute(context, promise))
                .onComplete(result -> {
                    if (result.succeeded()) {
                        serviceDbFacade.getServiceDbDao().getDatamartDao()
                                .insertDatamart(context.getDatamartName(), insertResult -> {
                                    if (insertResult.succeeded()) {
                                        log.debug("Datamart [{}] successfully created", context.getDatamartName());
                                        resultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                                    } else {
                                        log.error("Error inserting datamart [{}]!", context.getDatamartName(),
                                                insertResult.cause());
                                        resultHandler.handle(Future.failedFuture(insertResult.cause()));
                                    }
                                });
                    }
                })
                .onFailure(fail -> {
                    log.error("Error creating schema [{}] in data sources!", context.getDatamartName(), fail);
                    resultHandler.handle(Future.failedFuture(fail));
                });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }
}
