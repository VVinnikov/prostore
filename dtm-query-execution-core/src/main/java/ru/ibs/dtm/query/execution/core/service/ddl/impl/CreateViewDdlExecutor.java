package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.calcite.core.node.SqlTreeNode;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.List;

@Slf4j
@Component
public class CreateViewDdlExecutor extends QueryResultDdlExecutor {
    public static final String VIEW_QUERY_PATH = "CREATE_VIEW.SELECT";
    private final SqlDialect sqlDialect;

    @Autowired
    public CreateViewDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                 MariaProperties mariaProperties,
                                 ServiceDbFacade serviceDbFacade,
                                 @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
        this.sqlDialect = sqlDialect;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            List<SqlTreeNode> bySnapshot = new SqlSelectTree(context.getQuery())
                .findNodesByPath(SqlSelectTree.SELECT_AS_SNAPSHOT);
            if (bySnapshot.isEmpty()) {
                val ctx = getCreateViewContext(context, sqlNodeName);
                findDatamart(ctx)
                    .compose(this::checkEntity)
                    .compose(this::createOrReplaceView)
                    .onComplete(handler);
            } else {
                handler.handle(Future.failedFuture("It is not possible to specify a snapshot when creating a view"));
            }
        } catch (Exception e) {
            log.error("CreateViewContext creating error", e);
            handler.handle(Future.failedFuture(e));
        }

    }

    @NotNull
    private CreateViewContext getCreateViewContext(DdlRequestContext context, String sqlNodeName) {
        val schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        val sql = getSql(context, sqlNodeName);
        val tree = new SqlSelectTree(context.getQuery());
        val viewName = SqlPreparer.getViewName(tree);
        val viewQuery = getViewQuery(tree);
        return new CreateViewContext(schema, sql, viewName, viewQuery);
    }

    private String getViewQuery(SqlSelectTree tree) {
        val queryByView = tree.findNodesByPath(VIEW_QUERY_PATH);
        if (queryByView.isEmpty()) {
            throw new IllegalArgumentException("Unable to get view query");
        } else {
            return queryByView.get(0).getNode().toSqlString(sqlDialect).toString();
        }
    }

    private Future<CreateViewContext> findDatamart(CreateViewContext ctx) {
        return Future.future(p -> serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(ctx.getDatamartName(), ar -> {
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
            serviceDbFacade.getServiceDbDao().getEntityDao().isEntityExists(datamartId, viewName, ar -> {
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
        return Future.future(p -> serviceDbFacade.getServiceDbDao().getViewServiceDao().existsView(ctx.getViewName(), ctx.getDatamartId(), ar -> {
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
        serviceDbFacade.getServiceDbDao().getViewServiceDao().insertView(ctx.getViewName(), ctx.getDatamartId(), ctx.getViewQuery(), updateHandler -> {
            if (updateHandler.succeeded()) {
                handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
            } else {
                handler.handle(Future.failedFuture(updateHandler.cause()));
            }
        });
    }

    private void update(CreateViewContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        serviceDbFacade.getServiceDbDao().getViewServiceDao().updateView(ctx.getViewName(), ctx.getDatamartId(), ctx.getViewQuery(), updateHandler -> {
            if (updateHandler.succeeded()) {
                handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
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
