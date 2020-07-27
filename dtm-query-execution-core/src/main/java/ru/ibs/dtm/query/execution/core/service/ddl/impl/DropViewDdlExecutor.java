package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

@Slf4j
@Component
public class DropViewDdlExecutor extends QueryResultDdlExecutor {
    @Autowired
    public DropViewDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                               MariaProperties mariaProperties,
                               ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            val schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
            val sql = getSql(context, sqlNodeName);
            val viewName = SqlPreparer.getViewName(sql);
            findDatamart(schema)
                    .compose(datamartId -> dropView(viewName, datamartId))
                    .onComplete(handler);
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Long> findDatamart(String datamartName) {
        return Future.future(p -> serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(datamartName, ar -> {
            if (ar.succeeded()) {
                p.complete(ar.result());
            } else {
                p.fail(ar.cause());
            }
        }));
    }

    private Future<QueryResult> dropView(String viewName, Long datamartId) {
        return Future.future(p -> serviceDbFacade.getServiceDbDao().getViewServiceDao().existsView(viewName, datamartId, existsHandler -> {
            if (existsHandler.succeeded()) {
                if (existsHandler.result()) {
                    serviceDbFacade.getServiceDbDao().getViewServiceDao().dropView(viewName, datamartId, updateHandler -> {
                        if (updateHandler.succeeded()) {
                            p.complete(QueryResult.emptyResult());
                        } else {
                            p.fail(updateHandler.cause());
                        }
                    });
                } else {
                    val failureMsg = String.format("View is not exists [%s] by datamartId [%s]"
                            , viewName
                            , datamartId);
                    log.error(failureMsg);
                    p.fail(failureMsg);
                }
            } else {
                p.fail(existsHandler.cause());
            }
        }));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_VIEW;
    }
}
