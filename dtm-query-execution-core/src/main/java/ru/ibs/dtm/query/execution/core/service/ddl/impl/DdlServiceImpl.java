package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.common.status.ddl.DatamartSchemaChangedEvent;
import ru.ibs.dtm.query.calcite.core.extension.ddl.SqlUseSchema;
import ru.ibs.dtm.query.execution.core.service.delta.StatusEventPublisher;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service("coreDdlService")
public class DdlServiceImpl implements DdlService<QueryResult>, StatusEventPublisher {

    private final CoreCalciteDefinitionService coreCalciteDefinitionService;
    private final Map<SqlKind, DdlExecutor<QueryResult>> executorMap;
    private final Vertx vertx;

    @Autowired
    public DdlServiceImpl(CoreCalciteDefinitionService coreCalciteDefinitionService,
                          @Qualifier("coreVertx") Vertx vertx) {
        this.coreCalciteDefinitionService = coreCalciteDefinitionService;
        this.vertx = vertx;
        this.executorMap = new HashMap<>();
    }

    @Override
    public void execute(DdlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        vertx.executeBlocking(it -> {
            try {
                SqlNode node = coreCalciteDefinitionService.processingQuery(context.getRequest().getQueryRequest().getSql());
                it.complete(node);
            } catch (Exception e) {
                log.error("Request parsing error", e);
                it.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                execute(context, asyncResultHandler, ar);
            } else {
                log.debug("Execution error", ar.cause());
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void execute(DdlRequestContext context,
                         Handler<AsyncResult<QueryResult>> handler,
                         AsyncResult<Object> ar) {
        try {
            final SqlCall sqlCall = getSqlCall(ar);
            if (executorMap.containsKey(sqlCall.getKind())) {
                executorMap.get(sqlCall.getKind())
                    .execute(context, getSqlNodeName(sqlCall.getOperandList()), ddlAr -> {
                        if (ddlAr.succeeded()) {
                            handler.handle(Future.succeededFuture(ddlAr.result()));
                            publishStatus(
                                StatusEventCode.DATAMART_SCHEMA_CHANGED,
                                context.getDatamartName(),
                                DatamartSchemaChangedEvent.builder()
                                    .datamart(context.getDatamartName())
                                    .changeDateTime(LocalDateTime.now(ZoneOffset.UTC))
                                    .build()
                            );
                        } else {
                            handler.handle(Future.failedFuture(ddlAr.cause()));
                        }
                    });
            } else {
                log.error("Not supported DDL query type");
                handler.handle(Future.failedFuture(String.format("Not supported DDL query type [%s]", context)));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            handler.handle(Future.failedFuture(String.format("Not supported request type [%s]", context)));
        }
    }

    private SqlCall getSqlCall(AsyncResult<Object> ar) {
        if (ar.result() instanceof SqlAlter) {
            return (SqlCall) ar.result();
        } else if (ar.result() instanceof SqlDdl) {
            return (SqlCall) ar.result();
        } else if (ar.result() instanceof SqlUseSchema) {
            return (SqlCall) ar.result();
        } else {
            throw new RuntimeException("Not supported request type");
        }
    }

    private String getSqlNodeName(List<SqlNode> operandList) {
        return operandList.stream().filter(t -> t instanceof SqlIdentifier).findFirst().get().toString();
    }

    @Override
    public void addExecutor(DdlExecutor<QueryResult> executor) {
        executorMap.put(executor.getSqlKind(), executor);
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
