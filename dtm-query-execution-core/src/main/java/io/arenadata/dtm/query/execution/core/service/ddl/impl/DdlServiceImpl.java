package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.ddl.DatamartSchemaChangedEvent;
import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.delta.StatusEventPublisher;
import io.arenadata.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.utils.ParseQueryUtils;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("coreDdlService")
public class DdlServiceImpl implements DdlService<QueryResult>, StatusEventPublisher {

    private final CoreCalciteDefinitionService coreCalciteDefinitionService;
    private final Map<SqlKind, DdlExecutor<QueryResult>> executorMap;
    private final InformationSchemaService informationSchemaService;
    private final Vertx vertx;
    private final ParseQueryUtils parseQueryUtils;

    @Autowired
    public DdlServiceImpl(CoreCalciteDefinitionService coreCalciteDefinitionService,
                          InformationSchemaService informationSchemaService,
                          @Qualifier("coreVertx") Vertx vertx, ParseQueryUtils parseQueryUtils) {
        this.coreCalciteDefinitionService = coreCalciteDefinitionService;
        this.informationSchemaService = informationSchemaService;
        this.vertx = vertx;
        this.parseQueryUtils = parseQueryUtils;
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
                    .execute(context, parseQueryUtils.getDatamartName(sqlCall.getOperandList()), ddlAr -> {
                        if (ddlAr.succeeded()) {
                            handler.handle(Future.succeededFuture(ddlAr.result()));
                            informationSchemaService.update(sqlCall);
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
        } else {
            throw new RuntimeException("Not supported request type");
        }
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
