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
public class AlterViewDdlExecutor extends QueryResultDdlExecutor {

    public static final String ALTER_VIEW_QUERY_PATH = "ALTER_VIEW.SELECT";
    private final SqlDialect sqlDialect;

    @Autowired
    public AlterViewDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                MariaProperties mariaProperties, ServiceDbFacade serviceDbFacade,
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
                val ctx = getAlterViewContext(context, sqlNodeName);
                findDatamart(ctx)
                        .compose(this::checkEntity)
                        .compose(this::alterView)
                        .onComplete(handler);
            } else {
                handler.handle(Future.failedFuture("FOR SYSTEM_TIME syntax forbidden in a view"));
            }
        } catch (Exception e) {
            log.error("CreateViewContext creating error", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    @NotNull
    private AlterViewDdlExecutor.AlterViewContext getAlterViewContext(DdlRequestContext context, String sqlNodeName) {
        val schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
        val sql = getSql(context, sqlNodeName);
        val tree = new SqlSelectTree(context.getQuery());
        val viewName = SqlPreparer.getViewName(tree);
        val viewQuery = getViewQuery(tree);
        return new AlterViewDdlExecutor.AlterViewContext(schema, sql, viewName, viewQuery);
    }

    private Future<AlterViewDdlExecutor.AlterViewContext> findDatamart(AlterViewDdlExecutor.AlterViewContext ctx) {
        return Future.future(p -> serviceDbFacade.getServiceDbDao().getDatamartDao()
                .findDatamart(ctx.getDatamartName(), ar -> {
                    if (ar.succeeded()) {
                        ctx.setDatamartId(ar.result());
                        p.complete(ctx);
                    } else {
                        p.fail(ar.cause());
                    }
                }));
    }

    private Future<AlterViewDdlExecutor.AlterViewContext> checkEntity(AlterViewDdlExecutor.AlterViewContext ctx) {
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

    private Future<QueryResult> alterView(AlterViewDdlExecutor.AlterViewContext ctx) {
        return Future.future(p -> serviceDbFacade.getServiceDbDao().getViewServiceDao()
                .existsView(ctx.getViewName(), ctx.getDatamartId(), ar -> {
                    if (ar.succeeded()) {
                        if (ar.result()) {
                            update(ctx, p);
                        } else {
                            val failureMsg = String.format(
                                    "View is not exists [%s] by datamart [%s]"
                                    , ctx.getViewName()
                                    , ctx.getDatamartName());
                            log.error(failureMsg);
                            p.fail(failureMsg);
                        }
                    } else {
                        p.fail(ar.cause());
                    }
                }));
    }

    private void update(AlterViewDdlExecutor.AlterViewContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        serviceDbFacade.getServiceDbDao().getViewServiceDao().updateView(ctx.getViewName(), ctx.getDatamartId(),
                ctx.getViewQuery(), updateHandler -> {
                    if (updateHandler.succeeded()) {
                        handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                    } else {
                        handler.handle(Future.failedFuture(updateHandler.cause()));
                    }
                });
    }

    private String getViewQuery(SqlSelectTree tree) {
        val queryByView = tree.findNodesByPath(ALTER_VIEW_QUERY_PATH);
        if (queryByView.isEmpty()) {
            throw new IllegalArgumentException("Unable to get alter view query");
        } else {
            return queryByView.get(0).getNode().toSqlString(sqlDialect).toString();
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.ALTER_VIEW;
    }

    @Data
    @RequiredArgsConstructor
    private final static class AlterViewContext {
        private final String datamartName;
        private final String sql;
        private final String viewName;
        private final String viewQuery;
        private Long datamartId;
    }
}
