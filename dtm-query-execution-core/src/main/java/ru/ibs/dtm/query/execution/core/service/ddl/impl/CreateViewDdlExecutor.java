package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

@Slf4j
@Component
public class CreateViewDdlExecutor extends QueryResultDdlExecutor {

    @Autowired
    public CreateViewDdlExecutor(MetadataFactory<DdlRequestContext> metadataFactory,
                                 MariaProperties mariaProperties,
                                 ServiceDao serviceDao) {
        super(metadataFactory, mariaProperties, serviceDao);
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        findDatamart(getCreateViewContext(context, sqlNodeName))
                .compose(this::checkEntity)
                .compose(this::createOrReplaceView)
                .onComplete(handler);

    }

    @NotNull
    private CreateViewContext getCreateViewContext(DdlRequestContext context, String sqlNodeName) {
        val schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        val sql = getSql(context, sqlNodeName);
        val viewName = SqlPreparer.getViewName(sql);
        val viewQuery = SqlPreparer.getViewQuery(sql);
        return new CreateViewContext(schema, sql, viewName, viewQuery);
    }

    private Future<CreateViewContext> findDatamart(CreateViewContext ctx) {
        return Future.future(p -> serviceDao.findDatamart(ctx.getDatamartName(), ar -> {
            if (ar.succeeded()) {
                ctx.setDatamartId(ar.result());
                p.complete(ctx);
            } else {
                p.fail(ar.cause());
            }
        }));
    }

    private Future<CreateViewContext> checkEntity(CreateViewContext ctx) {
        return Future.future(p -> {
            val datamartId = ctx.getDatamartId();
            val viewName = ctx.getViewName();
            serviceDao.existsEntity(datamartId, viewName, ar -> {
                if (ar.succeeded()) {
                    if (ar.result()) {
                        val msg = String.format(
                                "Table exists by datamart [%d] and name [%s]"
                                , datamartId
                                , viewName);
                        p.fail(msg);
                    } else {
                        p.complete(ctx);
                    }
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }

    private Future<QueryResult> createOrReplaceView(CreateViewContext ctx) {
        return Future.future(p -> serviceDao.existsView(ctx.getViewName(), ctx.getDatamartId(), ar -> {
            if (ar.succeeded()) {
                if (ar.result()) {
                    if (SqlPreparer.isCreateOrReplace(ctx.getSql())) {
                        update(ctx, p);
                    } else {
                        val failureMsg = String.format(
                                "View is exists [%s] by datamart [%s]. Use  CREATE OR REPLACE"
                                , ctx.getViewName()
                                , ctx.getDatamartName());
                        log.error(failureMsg);
                        p.fail(failureMsg);
                    }
                } else {
                    if (SqlPreparer.isAlter(ctx.getSql())) {
                        val failureMsg = String.format(
                                "View is not exists [%s] by datamart [%s]"
                                , ctx.getViewName()
                                , ctx.getDatamartName());
                        log.error(failureMsg);
                        p.fail(failureMsg);
                    } else {
                        insert(ctx, p);
                    }
                }
            } else {
                p.fail(ar.cause());
            }
        }));
    }

    private void insert(CreateViewContext ctx,
                        Handler<AsyncResult<QueryResult>> handler) {
        serviceDao.insertView(ctx.getViewName(), ctx.getDatamartId(), ctx.getViewQuery(), updateHandler -> {
            if (updateHandler.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(updateHandler.cause()));
            }
        });
    }

    private void update(CreateViewContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        serviceDao.updateView(ctx.getViewName(), ctx.getDatamartId(), ctx.getViewQuery(), updateHandler -> {
            if (updateHandler.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(updateHandler.cause()));
            }
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_VIEW;
    }

    @Data
    @RequiredArgsConstructor
    private final static class CreateViewContext {
        private final String datamartName;
        private final String sql;
        private final String viewName;
        private final String viewQuery;
        private Long datamartId;
    }
}
