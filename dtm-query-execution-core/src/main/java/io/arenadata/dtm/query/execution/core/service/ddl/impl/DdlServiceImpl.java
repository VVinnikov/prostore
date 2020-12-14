package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.ddl.truncate.SqlBaseTruncate;
import io.arenadata.dtm.query.execution.core.exception.DtmException;
import io.arenadata.dtm.query.execution.core.utils.ParseQueryUtils;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlPostExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service("coreDdlService")
public class DdlServiceImpl implements DdlService<QueryResult> {

    private final Map<SqlKind, DdlExecutor<QueryResult>> executorMap;
    private final Map<PostSqlActionType, DdlPostExecutor> postExecutorMap;
    private final ParseQueryUtils parseQueryUtils;

    @Autowired
    public DdlServiceImpl(ParseQueryUtils parseQueryUtils) {
        this.parseQueryUtils = parseQueryUtils;
        this.executorMap = new HashMap<>();
        this.postExecutorMap = new HashMap<>();
    }

    @Override
    public void execute(DdlRequestContext context,
                        Handler<AsyncResult<QueryResult>> handler) {
        try {
            SqlCall sqlCall = getSqlCall(context.getQuery());
            DdlExecutor<QueryResult> executor = executorMap.get(sqlCall.getKind());
            if (executor != null) {
                executor.execute(context, parseQueryUtils.getDatamartName(sqlCall.getOperandList()), ddlAr -> {
                    if (ddlAr.succeeded()) {
                        handler.handle(Future.succeededFuture(ddlAr.result()));
                        context.getPostActions().addAll(executor.getPostActions());

                    } else {
                        handler.handle(Future.failedFuture(ddlAr.cause()));
                    }
                    executePostActions(context);
                });
            } else {
                handler.handle(Future.failedFuture(
                        new DtmException(String.format("Not supported DDL query type [%s]",
                                context.getQuery()))));
            }
        } catch (Exception e) {
            handler.handle(Future.failedFuture(new DtmException(String.format("Not supported request type [%s]",
                    context.getQuery()), e)));
        }
    }

    private void executePostActions(DdlRequestContext context) {
        CompositeFuture.join(context.getPostActions().stream()
                .distinct()
                .map(postType -> Optional.ofNullable(postExecutorMap.get(postType))
                        .map(postExecutor -> postExecutor.execute(context))
                        .orElse(Future.failedFuture(new DtmException(String.format("Not supported DDL post executor type [%s]",
                                postType)))))
                .collect(Collectors.toList()));
    }

    private SqlCall getSqlCall(SqlNode sqlNode) {
        if (sqlNode instanceof SqlAlter || sqlNode instanceof SqlDdl || sqlNode instanceof SqlBaseTruncate) {
            return (SqlCall) sqlNode;
        } else {
            throw new DtmException("Not supported request type");
        }
    }

    @Override
    public void addExecutor(DdlExecutor<QueryResult> executor) {
        executorMap.put(executor.getSqlKind(), executor);
    }

    @Override
    public void addPostExecutor(DdlPostExecutor executor) {
        postExecutorMap.put(executor.getPostActionType(), executor);
    }
}
